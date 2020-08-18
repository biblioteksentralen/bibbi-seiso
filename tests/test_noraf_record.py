import json
from copy import deepcopy
from datetime import date

import pytest

from seiso.common.noraf_record import NorafJsonMarcField, FieldNotFound, SubfieldNotFound
from seiso.common.interfaces import NorafPersonRecord
from seiso.services.noraf import NorafJsonRecord

example1 = {
    "authorityType": "PERSON",
    "status": "kat2",
    "origin": "hha253######47BIBSYS_UBTO",
    "deleted": False,
    "systemControlNumber": "1560455410566",
    "lastUpdateDate": "2019-08-12 11:26:03.539",
    "createdDate": "2019-08-12 11:26:03.539",
    "marcdata": [
        {
            "tag": "100",
            "ind1": "1",
            "ind2": " ",
            "subfields": [
                {
                    "subcode": "a",
                    "value": "Karlsson, Terése"
                }
            ]
        }
    ],
    "identifiersMap": {
        "handle": [
            "http://hdl.handle.net/11250/2607868"
        ],
        "scn": [
            "1560455410566"
        ]
    }
}

example2 = {
    "authorityType": "PERSON",
    "createdDate": "2014-03-13 00:00:00.000",
    "deleted": False,
    "identifiersMap": {
        "autid": [
            "x14011193"
        ],
        "bibbi": [
            "315434"
        ],
        "handle": [
            "http://hdl.handle.net/11250/2066683"
        ],
        "isni": [
            "0000000395090901"
        ],
        "scn": [
            "14011193"
        ],
        "viaf": [
            "http://viaf.org/viaf/289698481"
        ]
    },
    "lastUpdateDate": "2020-02-19 16:29:06.046",
    "marcdata": [
        {
            "ind1": "1",
            "ind2": " ",
            "subfields": [
                {
                    "subcode": "a",
                    "value": "Galbraith, Robert"
                },
                {
                    "subcode": "d",
                    "value": "1965-"
                }
            ],
            "tag": "100"
        },
        {
            "ind1": "",
            "ind2": "",
            "subfields": [
                {
                    "subcode": "a",
                    "value": "eng."
                },
                {
                    "subcode": "m",
                    "value": "Nasjonalitet/regional gruppe"
                },
                {
                    "subcode": "2",
                    "value": "bs-nasj"
                }
            ],
            "tag": "386"
        },
        {
            "ind1": "1",
            "ind2": " ",
            "subfields": [
                {
                    "subcode": "a",
                    "value": "Rowling, J.K."
                },
                {
                    "subcode": "d",
                    "value": "1965-"
                },
                {
                    "subcode": "0",
                    "value": "(NO-TrBIB)97027609"
                }
            ],
            "tag": "500"
        },
        {
            "ind1": " ",
            "ind2": " ",
            "subfields": [
                {
                    "subcode": "a",
                    "value": "Psevdonym for J.K. Rowling"
                }
            ],
            "tag": "678"
        }
    ],
    "origin": "sruu",
    "replacedBy": "0",
    "status": "kat3",
    "systemControlNumber": "14011193"
}


def test_parse_example1():
    rec = NorafJsonRecord(json.dumps(example1))
    assert rec.id == '1560455410566'
    assert rec.origin == 'hha253######47BIBSYS_UBTO'
    assert rec.status == 'kat2'
    assert rec.record_type == 'PERSON'
    assert rec.nationality is None

    assert rec.has('100') is True
    assert rec.first('100').value('a') == 'Karlsson, Terése'
    assert rec.identifiers('handle') == ('http://hdl.handle.net/11250/2607868',)
    assert rec.simple_record() == NorafPersonRecord(
        id='1560455410566',
        created=date(2019, 8, 12),
        modified=date(2019, 8, 12),
        name='Karlsson, Terése',
        other_ids={
            'handle': ['http://hdl.handle.net/11250/2607868'],
        }
    )

    with pytest.raises(SubfieldNotFound):
        rec.first('100').value('q')

    assert rec.has('043') is False
    with pytest.raises(FieldNotFound):
        rec.first('043').value('a')


def test_parse_example2():
    rec = NorafJsonRecord(json.dumps(example2))
    assert rec.id == '14011193'
    assert rec.origin == 'sruu'
    assert rec.status == 'kat3'
    assert rec.record_type == 'PERSON'
    assert rec.nationality == 'eng.'
    assert rec.identifiers('bibbi') == ('315434',)
    assert rec.simple_record() == NorafPersonRecord(
        id='14011193',
        created=date(2014, 3, 13),
        modified=date(2020, 2, 19),
        name='Galbraith, Robert',
        dates='1965-',
        nationality='eng.',
        other_ids={
            'bibbi': ['315434'],
            'handle': ['http://hdl.handle.net/11250/2066683'],
            'isni': ['0000000395090901'],
            'viaf': ['http://viaf.org/viaf/289698481'],
        }
    )


def test_update_field():
    rec = NorafJsonRecord(json.dumps(example1))
    rec.first('100').set('d', '1985-')
    assert rec.first('100').value('d') == '1985-'

    expected = deepcopy(example1)
    expected['marcdata'][0]['subfields'].append({
        "subcode": "d",
        "value": "1985-"
    })
    assert rec.as_dict() == expected


def test_add_field():
    rec = NorafJsonRecord(json.dumps(example1))
    new_field = NorafJsonMarcField.construct('043')
    new_field.set('c', 'no')
    rec.add(new_field)

    expected = deepcopy(example1)
    expected['marcdata'].insert(0, {
      "tag": "043",
      "ind1": " ",
      "ind2": " ",
      "subfields": [
        {
          "subcode": "c",
          "value": "no"
        }
      ]
    })
    assert rec.as_dict() == expected
