import json
from copy import deepcopy

from soji.common.bare import BibsysJsonRecord

template = {
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


def test_update_data():
    rec = BibsysJsonRecord(json.dumps(template))
    rec.first('100').set('d', '1985-')
    assert rec.first('100').value('d') == '1985-'

    expected = deepcopy(template)
    expected['marcdata'][0]['subfields'].append(                {
        "subcode": "d",
        "value": "1985-"
    })
    assert rec.serialize() == expected

