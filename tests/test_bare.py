import pytest

from soji.common.interfaces import BarePersonRecord
from soji.common.bare import Bare, BareRecordNotFound

test_data = [
    # Entry with birth date
    (
        'bib.namePersonal="Hveberg, Klara"',
        BarePersonRecord(
            name='Hveberg, Klara',
            id='99064681',
            dates='1974-',
            alt_names=[],
            bibbi_ids=['407922']
        )
    ),
    # Entry without birth date
    (
        'bib.namePersonal="Karlsson, Terése"',
        BarePersonRecord(
            name='Karlsson, Terése',
            id='1560455410566',
            dates=None,
            alt_names=[],
            bibbi_ids=[]
        )
    ),
    # Entry with alt names
    (
        'bib.namePersonal="Ewo, Jon"',
        BarePersonRecord(
            name='Ewo, Jon',
            id='90096006',
            dates='1957-',
            alt_names=['Wedding, Frank Miguel', 'Halvorsen, Jon Tore'],
            bibbi_ids=['10802']
        )
    ),
]


@pytest.mark.webtest
@pytest.mark.parametrize('query, expected_record', test_data)
def test_bare_search(query, expected_record):
    bare = Bare()
    records = list(bare.sru_search(query))

    assert len(records) >= 1
    assert records[0] == expected_record


@pytest.mark.webtest
@pytest.mark.parametrize('query, expected_record', test_data)
def test_bare_get(query, expected_record):
    bare = Bare()
    assert bare.get(expected_record.id).simple_record() == expected_record


@pytest.mark.webtest
def test_bare_get_not_found():
    bare = Bare()
    with pytest.raises(BareRecordNotFound) as exc:
        bare.get('expected_fail')

    assert 'expected_fail' in str(exc)
