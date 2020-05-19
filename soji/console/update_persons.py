import argparse
import logging
from dataclasses import dataclass

from dotenv import load_dotenv
from openpyxl import load_workbook
from openpyxl.cell import Cell
from openpyxl.worksheet.worksheet import Worksheet

from soji.common.interfaces import BibbiPerson
from soji.common.promus import Promus
from soji.common.logging import logger


@dataclass
class BibbiBareMatch():
    bibbi_id: str
    bare_id: str


def update_person(bibbi_person: BibbiPerson, match: BibbiBareMatch):
    logger.info('PLAN: Bibbi person %s: Add Bare ID %s', bibbi_person.id, match.bare_id)


def main():
    load_dotenv()
    logger.info('Starting')

    promus = Promus()

    parser = argparse.ArgumentParser(description='Update persons using data from Excel sheet')
    parser.add_argument(
        'infile',
        nargs='?',
        default=r'/mnt/c/Users/dmheg/Bibliotekenes IT-Senter/Bibliotekfaglig avdeling - General/' +
                r'Promusdugnad/bare_forslag_fra_isbn_tittel_match.xlsx',
        type=argparse.FileType('rb')
    )
    parser.add_argument('--debug', action='store_true')
    args = parser.parse_args()

    if args.debug:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)

    wb = load_workbook(args.infile)

    sheet: Worksheet = wb['Resultater']

    logger.info('Rows: %d, columns. %d' % (sheet.max_row, sheet.max_column))
    ok = []
    for row in sheet.iter_rows(3):

        if row[0].value is None:
            continue

        status = str(row[0].value)

        if status.lower() == 'ok':
            ok.append(BibbiBareMatch(
                bibbi_id=str(row[1].value),
                bare_id=str(row[6].value)
            ))
        else:
            print(status)

    logger.info('OK: %d', len(ok))

    for match in ok:
        bibbi_person = promus.persons.get(match.bibbi_id)
        if bibbi_person is None:
            logger.warning('Person no longer exists: %s', match.bibbi_id)
        elif bibbi_person.bare_id is not None:
            logger.debug('Person already matched to %s. Our suggestion: %s', bibbi_person.bare_id, match.bare_id)
        else:
            update_person(bibbi_person, match)
