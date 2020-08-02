from datetime import date
from typing import Optional

import pytest
from dotenv import load_dotenv

from seiso.common.interfaces import BibbiPerson
from seiso.services.promus import Promus

test_data = [
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

conn: Optional[Promus] = None


def setup_module(module):
    global conn
    load_dotenv()
    conn = Promus()


@pytest.mark.integration
@pytest.mark.parametrize('bibbi_id, expected_record', test_data)
def test_promus_get_person(bibbi_id, expected_record):
    record = conn.persons.get(bibbi_id)
    record.items=[]

    assert record == expected_record
