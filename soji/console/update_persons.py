import argparse
from dataclasses import dataclass

from dotenv import load_dotenv
from openpyxl import load_workbook
from openpyxl.cell import Cell
from openpyxl.worksheet.worksheet import Worksheet

from soji.common.promus import Promus


@dataclass
class Match():
    bibbi_id: str
    bare_id: str


def main():
    load_dotenv()

    promus = Promus()

    parser = argparse.ArgumentParser(description='Update persons using data from Excel sheet')
    parser.add_argument(
        'infile',
        nargs='?',
        default=r'/mnt/c/Users/dmheg/Bibliotekenes IT-Senter/Bibliotekfaglig avdeling - General/' +
                r'Promusdugnad/bare_forslag_fra_isbn_tittel_match.xlsx',
        type=argparse.FileType('rb')
    )
    args = parser.parse_args()

    wb = load_workbook(args.infile)

    sheet: Worksheet = wb['Resultater']

    print('Rows: %d, columns. %d' % (sheet.max_row, sheet.max_column))
    ok = []
    for row in sheet.iter_rows(3):

        if row[0].value is None:
            continue

        status = str(row[0].value)

        if status.lower() == 'ok':
            ok.append(Match(
                bibbi_id=str(row[1].value),
                bare_id=str(row[6].value)
            ))
        else:
            print(status)

    print("OK: %d" % len(ok))

    for match in ok:
        print(match)
        promus.persons.get(match.bibbi_id)
