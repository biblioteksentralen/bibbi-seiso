from __future__ import annotations

import json
import logging
from typing import Optional, List, Dict, Union

from soji.common.interfaces import BarePersonRecord, BareRecord

logger = logging.getLogger(__name__)

TYPE_PERSON = 'PERSON'


class FieldNotFound(IndexError):
    pass


class SubfieldNotFound(IndexError):
    pass


class BibsysJsonMarcField:
    """
    Wrapper class for a MARC field using the ad hoc JSON serialization from BARE.
    """

    def __init__(self, data: Dict):
        self.data = data

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

    def values(self, code: str) -> List[str]:
       return [sf['value'] for sf in self.data['subfields'] if sf['subcode'] == code]

    def value(self, code: str) -> str:
        values = self.values(code)
        if len(values) == 0:
            raise SubfieldNotFound()
        return values[0]

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


class BibsysJsonRecord:
    """
    Wrapper class for the BARE authority JSON format returned from {BASE_URI}/authorities/v2/{id}
    """

    def __init__(self, data: str):
        self.data: Dict = json.loads(data)
        self.fields = [BibsysJsonMarcField(field) for field in self.data['marcdata']]

    @property
    def record_type(self) -> str:
        return self.data['authorityType']

    @property
    def id(self) -> str:
        return self.data['systemControlNumber']

    @property
    def status(self) -> str:
        return self.data['status']

    @property
    def origin(self) -> str:
        return self.data['origin']

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
            if field.value_or_none('2') == 'bs-nasj'
        ]
        if len (ls) != 0:
            return ls[0]
        return None

    def identifiers(self, vocabulary: str) -> List[str]:
        return self.data['identifiersMap'].get(vocabulary, [])

    def set_identifiers(self, vocabulary: str, values: List[str]):
        self.data['identifiersMap'][vocabulary] = values

    def all(self, tag: str) -> List[BibsysJsonMarcField]:
        return [field for field in self.fields if field.tag == tag]

    def has(self, tag: str) -> bool:
        return len(self.all(tag)) != 0

    def first(self, tag: str) -> BibsysJsonMarcField:
        if not self.has(tag):
            raise FieldNotFound()
        return self.all(tag)[0]

    def add(self, new_field: BibsysJsonMarcField):
        pos = 0
        for field in self.fields:
            if int(field.tag) > int(new_field.tag):
                break
            pos += 1
        self.fields.insert(pos, new_field)

    def simple_record(self) -> Union[BareRecord, BarePersonRecord]:
        record_types: Dict = {
            TYPE_PERSON: {
                'cls': BarePersonRecord,
                'tag': '00',
            }
        }
        if self.record_type not in record_types:
            raise Exception('Not implemented yet')

        record_type: Dict = record_types[self.record_type]
        main_tag = self.first('1' + record_type['tag'])

        kwargs: Dict = {
            'id': self.id,
            'name': main_tag.value('a'),
            'bibbi_ids': self.identifiers('bibbi'),
            'alt_names': [field.value('a') for field in self.all('400')]
        }

        if self.record_type == TYPE_PERSON:
            kwargs['dates'] = main_tag.value_or_none('d')
            kwargs['country_codes'] = self.country_codes
            kwargs['nationality'] = self.nationality

        return record_type['cls'](**kwargs)

    def as_dict(self) -> Dict:
        self.data['marcdata'] = [field.as_dict() for field in self.fields]
        return self.data

    def as_json(self) -> str:
        return json.dumps(self.as_dict(), ensure_ascii=False, indent=4)
