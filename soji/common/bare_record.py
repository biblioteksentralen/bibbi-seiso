from __future__ import annotations

import json
import logging
from typing import Optional, List, Dict

from soji.common.interfaces import BarePersonRecord, BareRecord

logger = logging.getLogger(__name__)

TYPE_PERSON = 'PERSON'


class BibsysJsonMarcField:
    """
    Wrapper class for a MARC field using the ad hoc JSON serialization from BARE.
    """

    def __init__(self, data: Dict):
        self.data = data

    @property
    def tag(self) -> str:
        return self.data['tag']

    def values(self, code: str) -> List[str]:
       return [sf['value'] for sf in self.data['subfields'] if sf['subcode'] == code]

    def value(self, code: str) -> str:
        return self.values(code)[0]

    def value_or_none(self, code: str) -> Optional[str]:
        try:
            return self.value(code)
        except IndexError:
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

    def identifiers(self, vocabulary: str) -> List[str]:
        return self.data['identifiersMap'].get(vocabulary, [])

    def set_identifiers(self, vocabulary: str, values: List[str]):
        self.data['identifiersMap'][vocabulary] = values

    def all(self, tag: str) -> List[BibsysJsonMarcField]:
        return [field for field in self.fields if field.tag == tag]

    def first(self, tag: str) -> BibsysJsonMarcField:
        return self.all(tag)[0]

    def simple_record(self) -> BareRecord:
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

        return record_type['cls'](**kwargs)

    def serialize(self) -> Dict:
        return self.data

