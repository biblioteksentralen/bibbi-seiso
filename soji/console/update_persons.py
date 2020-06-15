import argparse
import logging
from dataclasses import dataclass

from dotenv import load_dotenv
from openpyxl import load_workbook
from openpyxl.cell import Cell
from openpyxl.worksheet.worksheet import Worksheet
from soji.common.bare import Bare

from soji.common.interfaces import BibbiPerson
from soji.common.promus import Promus
from soji.common.logging import logger


@dataclass
class BibbiBareMatch():
    bibbi_id: str
    bare_id: str


promus = Promus()
bare = Bare()


def update_person(bibbi_person: BibbiPerson, match: BibbiBareMatch):
    # logger.info('PLAN: Bibbi person %s: Add Bare ID %s', bibbi_person.id, match.bare_id)
    bare_rec = bare.get(match.bare_id)
    if bibbi_person.dates is None and bare_rec.dates is not None:
        print("GNU")
    logger.info('"%s" <=> "%s"' % (bibbi_person, bare_rec))

    # Update BARE

    bare_rec.set_identifiers('bibbi_id', [bibbi_person.id])
    if bibbi_person.dates is not None and bare_rec.dates is None:
        bare_rec.first('100').set('d', bibbi_person.dates)
    bare.update_record(bare_rec)

    # Update Bibbi
    promus.persons.update(bibbi_person, bare_id=bare_rec.id)



def main():
    load_dotenv()
    logger.info('Starting')

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
    marked_ok = []
    for row in sheet.iter_rows(3):

        if row[0].value is None:
            continue

        status = str(row[0].value)

        if status.lower() == 'ok':
            marked_ok.append(BibbiBareMatch(
                bibbi_id=str(row[1].value),
                bare_id=str(row[6].value)
            ))
        else:
            print(status)

    logger.info('Marked OK: %d', len(marked_ok))

    for match in marked_ok:
        bibbi_person = promus.persons.get(match.bibbi_id)
        if bibbi_person is None:
            logger.warning('Person no longer exists: %s', match.bibbi_id)
        elif bibbi_person.bare_id is not None:
            logger.warning('Person already matched to %s. Our suggestion: %s', bibbi_person.bare_id, match.bare_id)
        else:
            update_person(bibbi_person, match)
