import pytest
from soji.common.interfaces import BarePerson
from soji.common.viaf import get_viaf_candidates


@pytest.mark.webtest
@pytest.mark.parametrize('test_input, expected', [
    # Entry with birth date
    (
        'local.personalNames="Wedding, Frank Miguel"',
        BarePerson(
            name='Ewo, Jon',
            id='90096006',
            dates='1957-',
            alt_names=['Halvorsen, Jon Tore', 'Wedding, Frank Miguel']
        )
    ),
    # Entry without birth date
    (
        'local.personalNames="Olsson, Anna-Karin Maria"',
        BarePerson(
            name='Olsson, Anna-Karin Maria',
            id='8033921',
            dates=None,
            alt_names=[]
        )
    )
])
def test_get_viaf_candidates(test_input, expected):
    candidates = list(
        get_viaf_candidates(test_input)
    )

    bare_persons = [
        cand.person
        for cand in candidates
        if isinstance(cand.person, BarePerson)
    ]

    assert bare_persons[0] == expected
