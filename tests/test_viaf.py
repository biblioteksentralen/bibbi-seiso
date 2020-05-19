import pytest
from soji.common.interfaces import BarePerson
from soji.common.viaf import get_viaf_candidates


@pytest.mark.webtest
def test_get_viaf_candidates():
    candidates = list(
        get_viaf_candidates('local.personalNames="{creator}"'.format(creator="Ewo, Jon "))
    )

    bare_persons = [
        cand.person
        for cand in candidates
        if isinstance(cand.person, BarePerson)
    ]

    bare_names = set([
        person.name for person in bare_persons
    ])

    bare_matches = [
        person for person in bare_persons
        if person.name == 'Ewo, Jon'
    ]

    assert len(candidates) >= 83
    assert bare_names == {'Ewo, Jon', 'Selmas, Holger'}
    assert bare_matches[0] == BarePerson(
        name='Ewo, Jon',
        id='90096006',
        dates='1957-',
        alt_names=['Halvorsen, Jon Tore', 'Wedding, Frank Miguel']
    )
