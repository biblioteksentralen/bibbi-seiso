from __future__ import annotations

import json
import logging
from datetime import datetime, date
from typing import Optional, List, Dict, Union, Tuple, Sequence

from seiso.common.xml import XmlNode

from seiso.common.interfaces import NorafPersonRecord, NorafRecord, NorafCorporationRecord, IdentifierMap

logger = logging.getLogger(__name__)

TYPE_PERSON = 'PERSON'
TYPE_CORPORATION = 'CORPORATION'


class FieldNotFound(IndexError):
    pass


class SubfieldNotFound(IndexError):
    pass


class NorafJsonMarcField:
    """
    Wrapper class for a MARC field using the ad hoc JSON serialization from Noraf.
    """

    def __init__(self, data: Dict):
        if data['ind1'] == '':
            data['ind1'] = ' '
        if data['ind2'] == '':
            data['ind2'] = ' '
        self.data = data

    def __str__(self):
        subfields = ' '.join(
            '$%s %s' % (sf['subcode'], sf['value']) for sf in self.data['subfields']
        )
        return '%s%s%s %s' % (self.tag, self.ind1, self.ind2, subfields)

    @classmethod
    def construct(cls, tag, ind1=' ', ind2=' ', subfields=None):
        return cls({
            'tag': tag,
            'ind1': ind1,
            'ind2': ind2,
            'subfields': subfields or [],
        })

    @property
    def tag(self) -> str:
        return self.data['tag']

    @property
    def ind1(self) -> str:
        return self.data['ind1']

    @property
    def ind2(self) -> str:
        return self.data['ind2']

    def values(self, code: str) -> List[str]:
       return [sf['value'] for sf in self.data['subfields'] if sf['subcode'] == code]

    def value(self, code: str) -> str:
        values = self.values(code)
        if len(values) == 0:
            raise SubfieldNotFound()
        return values[0]

    def has(self, code: str) -> bool:
        return len(self.values(code)) != 0

    def value_or_none(self, code: str) -> Optional[str]:
        try:
            return self.value(code)
        except SubfieldNotFound:
            return None

    def set(self, code: str, value: str):
        n = 0
        for sf in self.data['subfields']:
            if sf['subcode'] < code:
                n += 1
            if sf['subcode'] == code:
                sf['value'] = value
                return

        self.data['subfields'].insert(n, {
            'subcode': code,
            'value': value,
        })

    def as_dict(self) -> dict:
        return self.data


record_types: Dict = {
    TYPE_PERSON: {
        'cls': NorafPersonRecord,
        'tag': '00',
    },
    TYPE_CORPORATION: {
        'cls': NorafCorporationRecord,
        'tag': '10',
    }
}


class NorafJsonRecord:
    """
    Wrapper class for the Noraf authority JSON format returned from {BASE_URI}/authorities/v2/{id}
    """
    def __init__(self, data: str):
        self.data: Dict = json.loads(data)
        self.fields = [NorafJsonMarcField(field) for field in self.data['marcdata']]
        self.dirty = False
        self.record_type = self.data['authorityType']
        if self.record_type not in record_types:
            raise Exception('Not implemented yet: %s' % self.record_type)

    def get_record_type_info(self):
        return record_types[self.record_type]

    def get_1xx_tag(self):
        return self.first('1' + self.get_record_type_info()['tag'])

    @property
    def id(self) -> str:
        return str(self.data['systemControlNumber'])

    @property
    def status(self) -> str:
        return self.data['status']

    @property
    def origin(self) -> str:
        return self.data['origin']

    @property
    def deleted(self) -> bool:
        return self.data['deleted']

    @property
    def created(self) -> date:
        return datetime.strptime(self.data['createdDate'][:10], '%Y-%m-%d').date()

    @property
    def modified(self) -> date:
        return datetime.strptime(self.data['lastUpdateDate'][:10], '%Y-%m-%d').date()

    @property
    def replaced_by(self) -> Optional[str]:
        if 'replacedBy' not in self.data:
            return None
        if self.data['replacedBy'] == '0':
            return None
        return str(self.data['replacedBy'])

    @property
    def name(self) -> str:
        return self.get_1xx_tag().value('a')

    @property
    def alt_names(self) -> List[str]:
        return [field.value('a') for field in self.all('400') if field.has('a')]

    @property
    def dates(self) -> Optional[str]:
        return self.get_1xx_tag().value_or_none('d')

    @property
    def gender(self) -> Optional[str]:
        try:
            return self.first('375').value_or_none('a')
        except FieldNotFound:
            return None

    @property
    def country_codes(self) -> List[str]:
        try:
            return self.first('043').values('c')
        except IndexError:
            return []

    @property
    def nationality(self) -> Optional[str]:
        ls = [
            field.value_or_none('a')
            for field in self.all('386')
            if field.value_or_none('2') in ['bs-nasj', 'bibbi']
        ]
        if len (ls) != 0:
            return ls[0]
        return None

    def identifiers(self, vocabulary: str) -> Tuple[str]:
        """Get a tuple of identifiers for records from another vocabulary, that this record maps to.

        Note that the tuple may include duplicates."""
        return tuple(self.data['identifiersMap'].get(vocabulary, []))

    def set_identifiers(self, vocabulary: str, values: Sequence[str]) -> bool:
        """Update the list of mappings to records in another vocabulary.

        Returns True and marks the record as dirty if it was changed."""
        values = list(values)
        if len(values) == 0:
            if vocabulary in self.data['identifiersMap']:
                logger.info('%s Remove identifiers %s = %s', self.id, vocabulary,
                            str(self.data['identifiersMap'][vocabulary]))
                del self.data['identifiersMap'][vocabulary]
                self.dirty = True
                return True
        if self.data['identifiersMap'].get(vocabulary, []) != values:
            self.data['identifiersMap'][vocabulary] = values
            self.dirty = True
            logger.info('%s Set identifiers %s = %s', self.id, vocabulary, str(values))
            return True
        return False

    def remove_identifier(self, vocabulary: str, value: str) -> bool:
        """Remove a mapping to a record in another vocabulary.

        Returns True and marks the record as dirty if it was changed. If the mapping didn't exist in the first place,
        no action is taken, and the method just returns False."""
        value = str(value)
        values = list(self.identifiers(vocabulary))
        if value not in values:
            return False
        values.remove(value)
        return self.set_identifiers(vocabulary, values)

    def add_identifier(self, vocabulary: str, value: str) -> bool:
        """Add a mapping to a record in another vocabulary.

        Returns True and marks the record as dirty if it was changed. If the mapping didn't exist in the first place,
        no action is taken, and the method just returns False."""
        value = str(value)
        values = list(self.identifiers(vocabulary))
        if value not in values:
            return False
        values.append(value)
        return self.set_identifiers(vocabulary, values)

    def all(self, tag: str) -> List[NorafJsonMarcField]:
        """Get all MARC fields with a given tag as a list"""
        return [field for field in self.fields if field.tag == tag]

    def has(self, tag: str) -> bool:
        """Check if the record contain at least one MARC field with a given tag"""
        return len(self.all(tag)) != 0

    def first(self, tag: str) -> NorafJsonMarcField:
        """Get the first MARC field with a given tag

        Raises FieldNotFound if not found.
        """
        if not self.has(tag):
            raise FieldNotFound()
        return self.all(tag)[0]

    def add(self, new_field: NorafJsonMarcField):
        """Add a MARC field from the record"""
        pos = 0
        for field in self.fields:
            if int(field.tag) > int(new_field.tag):
                break
            pos += 1
        self.fields.insert(pos, new_field)
        self.dirty = True
        logger.info('%s +++ %s', self.id, new_field)

    def remove(self, field: NorafJsonMarcField):
        """Remove a MARC field from the record"""
        self.fields.remove(field)
        self.dirty = True
        logger.info('%s --- %s', self.id, field)

    def __str__(self):
        out = self.name
        if self.dates is not None:
            out += ' (%s)' % self.dates
        return out

    def simple_record(self) -> Union[NorafRecord, NorafPersonRecord]:
        main_tag = self.get_1xx_tag()

        kwargs: Dict = {
            'id': self.id,
            'created': self.created,
            'modified': self.modified,
            'name': self.name,
            # autid is not included in the XML representation, so remove it to remain compatible
            'other_ids': {k: v for k, v in self.data['identifiersMap'].items() if k not in ['autid', 'scn']},
            'alt_names': self.alt_names,
        }

        if self.record_type == TYPE_PERSON:
            kwargs['dates'] = main_tag.value_or_none('d')
            kwargs['country_codes'] = self.country_codes
            kwargs['nationality'] = self.nationality
            kwargs['gender'] = self.gender

        return self.get_record_type_info()['cls'](**kwargs)

    def as_dict(self) -> Dict:
        self.data['marcdata'] = [field.as_dict() for field in self.fields]
        return self.data

    def as_json(self) -> str:
        return json.dumps(self.as_dict(), ensure_ascii=False, indent=4)


class NorafXmlRecord:

    @staticmethod
    def _parse_ids(rec: XmlNode) -> IdentifierMap:
        ids: IdentifierMap = {}
        for datafield in rec.all(':datafield[@tag="024"][./:subfield[@code="2"] and ./:subfield[@code="a"]]', xpath=True):
            voc = datafield.text(':subfield[@code="2"]')
            if voc == 'hdl':
                voc = 'handle'
            if voc == 'NO-TrBIB':
                continue
            value = datafield.text(':subfield[@code="a"]')
            ids[voc] = ids.get(voc, []) + [value]
        return ids

    @classmethod
    def parse(cls, rec: XmlNode) -> Optional[NorafRecord]:
        if main_tag := rec.first(':datafield[@tag="100"]'):
            return NorafPersonRecord(
                id=rec.text(':controlfield[@tag="001"]'),
                created=datetime.strptime(rec.text(':controlfield[@tag="008"]')[:6], '%y%m%d').date(),
                modified=datetime.strptime(rec.text(':controlfield[@tag="005"]')[:8], '%Y%m%d').date(),
                name=main_tag.text(':subfield[@code="a"]'),
                dates=main_tag.text_or_none(':subfield[@code="d"]'),
                other_ids=cls._parse_ids(rec),
                country_codes=rec.all_text(':datafield[@tag="043"]/:subfield[@code="c"]'),
                gender=rec.text_or_none(
                    ':datafield[@tag="375"]/:subfield[@code="a"]',
                    xpath=True
                ),
                nationality=rec.text_or_none(
                    ':datafield[@tag="386"][./:subfield[@code="2"] = "bibbi" or ./:subfield[@code="2"] = "bs-nasj"]/:subfield[@code="a"]',
                    xpath=True
                ),
                alt_names=rec.all_text(
                    ':datafield[@tag="400"]/:subfield[@code="a"]',
                    xpath=True
                ),
            )
        return None
