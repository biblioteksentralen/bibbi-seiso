from datetime import date
from typing import Optional

import pytest
from dotenv import load_dotenv

from seiso.common.interfaces import BibbiPerson
from seiso.services.promus import Promus


@pytest.fixture(scope="module")
def promus():
    print('setup promus')
    load_dotenv()
    return Promus()


person_examples = [
    # Entry with birth date
    (
        '407922',
        BibbiPerson(
            id='407922',
            created=date(2015, 6, 1),
            modified=date(2015, 6, 1),
            name='Hveberg, Klara',
            noraf_id='99064681',
            dates='1974-',
            nationality='n.',
            newest_approved=date(2019, 2, 20),
            country_codes=['no'],
            gender='f',
        )
    ),
    # Entry with multiple nationalities
    (
        '75716',
        BibbiPerson(
            id='75716',
            created=date(2005, 11, 3),
            modified=date(2005, 11, 3),
            name='Curie, Marie Sklodowska',
            alt_names=['Sklodowska Curie, Marie'],
            noraf_id='90528061',
            dates='1867-1934',
            nationality='pol.-fr.',
            newest_approved=date(2009, 8, 18),
            country_codes=['pl', 'fr'],
            gender='f',
        )
    ),
    # Entry with alt names
    # @TODO
]


@pytest.mark.integration
@pytest.mark.parametrize('bibbi_id, expected_record', person_examples)
def test_promus_get_person(promus, bibbi_id, expected_record):
    """Check that we can get a person authority by ID"""
    record = promus.authorities.person.get(bibbi_id)
    record.items = []   # Skip testing the exact structure, but we still test indirectly that there are
                        # item set through 'newest_approved'.
    assert record == expected_record


@pytest.mark.integration
@pytest.mark.parametrize('bibbi_id, expected_record', person_examples)
def test_promus_get_authority(promus, bibbi_id, expected_record):
    """Check that we can get an authority by ID without knowing what type it is."""
    record = promus.authorities.get(bibbi_id)
    record.items = []   # Skip testing the exact structure, but we still test indirectly that there are
                        # item set through 'newest_approved'.

    assert record == expected_record
