import pytest
from seiso.common.interfaces import NorafPerson
from seiso.common.viaf import get_viaf_candidates


@pytest.mark.integration
@pytest.mark.parametrize('test_input, expected', [
    # Entry with birth date
    (
        'local.personalNames="Wedding, Frank Miguel"',
        NorafPerson(
            id='90096006',
            created=None,
            modified=None,
            name='Ewo, Jon',
            dates='1957-',
            alt_names=['Halvorsen, Jon Tore', 'Wedding, Frank Miguel']
        )
    ),
    # Entry without birth date
    (
        'local.personalNames="Olsson, Anna-Karin Maria"',
        NorafPerson(
            id='8033921',
            created=None,
            modified=None,
            name='Olsson, Anna-Karin Maria',
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
        if isinstance(cand.person, NorafPerson)
    ]

    assert bare_persons[0] == expected
