import pytest
from soji.common.interfaces import BarePerson, BarePersonRecord
from soji.common.bare import Bare


@pytest.mark.webtest
def test_bare_search():
    test_name = 'Hveberg, Klara'

    bare = Bare()
    records = list(bare.search('bib.namePersonal="%s"' % test_name))

    assert len(records) == 1

    assert records[0] == BarePersonRecord(
        name='Hveberg, Klara',
        id='99064681',
        dates='1974-',
        alt_names=[],
        bibbi_ids=['407922']
    )
