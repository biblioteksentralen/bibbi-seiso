from datetime import date

import pytest

from seiso.common.interfaces import NorafPersonRecord
from seiso.services.noraf import Noraf, NorafRecordNotFound

test_data = [
    # Entry with birth date
    (
        'bib.namePersonal="Hveberg, Klara"',
        NorafPersonRecord(
            id='99064681',
            created=date(2012, 10, 11),
            modified=date(2020, 2, 19),
            name='Hveberg, Klara',
            dates='1974-',
            country_codes=['no'],
            nationality='n.',
            alt_names=[],
            other_ids={
                'handle': ['http://hdl.handle.net/11250/1468313'],
                'isni': ['0000000384001264'],
                'viaf': ['http://viaf.org/viaf/272719331'],
                'bibbi':['407922']
            },
            gender='f'
        )
    ),
    # Entry without birth date
    (
        'bib.namePersonal="Karlsson, Terése"',
        NorafPersonRecord(
            id='1560455410566',
            created=date(2019, 8, 12),
            modified=date(2020, 7, 15),
            name='Karlsson, Terése',
            dates=None,
            alt_names=[],
            other_ids={
                'handle': ['http://hdl.handle.net/11250/2607868'],
                'bibbi': ['1102657'],
            },
            nationality='sv.',
            country_codes=['se'],
            gender=None
        )
    ),
    # Entry with alt names
    (
        'bib.namePersonal="Ewo, Jon"',
        NorafPersonRecord(
            id='90096006',
            created=date(2013, 2, 15),
            modified=date(2020, 2, 19),
            name='Ewo, Jon',
            dates='1957-',
            country_codes=['no'],
            nationality='n.',
            alt_names=['Wedding, Frank Miguel', 'Halvorsen, Jon Tore'],
            other_ids={
                'handle': ['http://hdl.handle.net/11250/1611874'],
                'isni': ['0000000078316647'],
                'viaf': ['http://viaf.org/viaf/32050185'],
                'bibbi': ['10802']
            },
            gender='m'
        )
    ),
]


@pytest.mark.integration
@pytest.mark.parametrize('query, expected_record', test_data)
def test_noraf_search(query, expected_record):
    noraf = Noraf()
    records = list(noraf.sru_search(query))

    assert len(records) >= 1
    assert records[0] == expected_record


@pytest.mark.integration
@pytest.mark.parametrize('query, expected_record', test_data)
def test_noraf_get(query, expected_record):
    noraf = Noraf()
    assert noraf.get(expected_record.id).simple_record() == expected_record


@pytest.mark.integration
def test_noraf_get_not_found():
    noraf = Noraf()
    with pytest.raises(NorafRecordNotFound) as exc:
        noraf.get('expected_fail')

    assert 'expected_fail' in str(exc)
