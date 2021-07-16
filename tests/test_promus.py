from dataclasses import fields
from datetime import date, datetime
from typing import Optional

import pytest
from dotenv import load_dotenv

from seiso.services.promus import Promus
from seiso.services.promus.authorities import BibbiPersonRecord


@pytest.fixture(scope="module")
def promus():
    print('setup promus')
    load_dotenv()
    return Promus()


person_examples = [
    # Entry with birth date
    (
        '407922',
        {
            'Bibsent_ID': 407922,
            'Created': datetime(2015, 6, 1, 13, 11, 0, 177000),
            'PersonName': 'Hveberg, Klara',
            'NB_ID': 99064681,
            'PersonYear': '1974-',
            'PersonNation': 'n.',
            'Gender': 'f',
            'Approved': True,
            # newest_approved=date(2019, 2, 20),
            # country_codes=['no'],
        }
    ),
    # Entry with multiple nationalities
    (
        '75716',
        {
            'Bibsent_ID': 75716,
            'Created': datetime(2005, 11, 3,  0, 39, 7, 167000),
            'PersonName': 'Curie, Marie Sklodowska',
            'NB_ID': 90528061,
            'PersonYear': '1867-1934',
            'PersonNation': 'pol.-fr.',
            'Gender': 'f',
            'Approved': True,
            # newest_approved=date(2009, 8, 18),
            # country_codes=['pl', 'fr'],
        }  # .set_references( alt_names=['Sklodowska Curie, Marie'],            ....)
    ),
    # Entry with alt names
    # @TODO
]


@pytest.mark.integration
@pytest.mark.parametrize('bibbi_id, expected_record', person_examples)
def test_promus_get_person_by_bibbi_id(promus, bibbi_id, expected_record):
    """Check that we can get a person authority by ID"""
    record = promus.authorities.person.first(Bibsent_ID=bibbi_id)

    assert isinstance(record, BibbiPersonRecord)
    assert isinstance(record.LastChanged, datetime)
    assert record.collection == promus.authorities.person
    for key, val in expected_record.items():
        assert getattr(record, key) == val, '%s differs' % key
        # Note: The full record may contain additional fields


@pytest.mark.integration
@pytest.mark.parametrize('bibbi_id, expected_record', person_examples)
def test_promus_get_authority_by_bibbi_id(promus, bibbi_id, expected_record):
    """Check that we can get an authority by ID without knowing what type it is."""
    record = promus.authorities.first(Bibsent_ID=bibbi_id)
    assert isinstance(record, BibbiPersonRecord)
