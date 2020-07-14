from datetime import datetime
from typing import Optional

import pytest
from dotenv import load_dotenv

from soji.common.interfaces import BarePersonRecord, BibbiPerson
from soji.common.promus import Promus

test_data = [
    # Entry with birth date
    (
        '407922',
        BibbiPerson(
            id='407922',
            name='Hveberg, Klara',
            bare_id='99064681',
            dates='1974-',
            nationality='n.',
            newest_approved=datetime(2019,2,20,8,20,9, 650000),
            country_codes=['NO'],
        )
    ),
    # Entry with multiple nationalities
    (
        '75716',
        BibbiPerson(
            id='75716',
            name='Curie, Marie Sklodowska',
            bare_id='90528061',
            dates='1867-1934',
            nationality='pol.-fr.',
            newest_approved=datetime(2009, 8, 18, 9, 14, 6, 443333),
            country_codes=['PL', 'FR'],
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
    load_dotenv()
    record = conn.persons.get(bibbi_id)
    record.items=[]

    assert record == expected_record
