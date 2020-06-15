import os
import pyodbc  # type: ignore
from dataclasses import dataclass, field
from typing import List, Dict, Optional

from soji.common.interfaces import BibbiPerson, BibbiVare


class MsSql:

    def __init__(self, **db_settings):
        if os.name == 'posix':
            conn_string = ';'.join([
                'DRIVER={FreeTDS}',
                'Server=%(server)s',
                'Database=%(database)s',
                'UID=%(user)s',
                'PWD=%(password)s',
                'TDS_Version=8.0',
                'Port=1433',
            ]) % db_settings
        else:
            conn_string = ';'.join([
                'Driver={SQL Server}',
                'Server=%(server)s',
                'Database=%(database)s',
                'Trusted_Connection=yes',
            ]) % db_settings
        self.conn: pyodbc.Connection = pyodbc.connect(conn_string)

    def cursor(self) -> pyodbc.Cursor:
        return self.conn.cursor()


BibbiPersons = Dict[str, BibbiPerson]


@dataclass
class QueryFilter:
    stmt: str
    param: Optional[str] = None


class Promus:

    def __init__(self):
        self.db = MsSql(
            server=os.getenv('DB_SERVER'),
            database=os.getenv('DB_DB'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD')
        )

        self.persons = Persons(self)

    def cursor(self):
        return self.db.cursor()


class Persons:

    def __init__(self, conn: Promus):
        self.conn = conn

    def get(self, bibbi_id: str) -> Optional[BibbiPerson]:
        results = self.list([
            QueryFilter('person.Bibsent_ID = ?', bibbi_id)
        ])
        if bibbi_id in results:
            return results[bibbi_id]
        return None

    def list(self, filters: List[QueryFilter] = None) -> BibbiPersons:

        if filters is None:
            filters = []

        query = """
        SELECT
            person.Bibsent_ID,
            person.PersonName,
            person.PersonYear,
            person.PersonNation,
            iv1.Text AS Isbn,
            item.Title,
            iv2.Text as OriginalTitle,
            item.ApproveDate,
            person.NB_ID

        FROM AuthorityPerson AS person

        JOIN ItemField AS field ON field.Authority_ID = person.PersonId AND field.FieldCode IN ('100', '600', '700')
        JOIN Item AS item ON field.Item_ID = item.Item_ID
        JOIN ItemFieldSubFieldView as iv1 ON iv1.Item_ID = Item.Item_ID AND iv1.FieldCode = '020' AND iv1.SubFieldCode = 'a'
        LEFT JOIN ItemFieldSubFieldView as iv2 ON iv2.Item_ID = Item.Item_ID AND iv2.FieldCode = '240' AND iv2.SubFieldCode = 'a'

        WHERE
            person.ReferenceNr IS NULL
            AND person.Felles_ID = person.Bibsent_ID
            AND item.ApproveDate IS NOT NULL

        %(filters)s

        ORDER BY person.Bibsent_ID
        """ % {
            'filters': ' '.join(['AND %s' % filt.stmt for filt in filters])
        }

        filter_params = [filt.param for filt in filters if filt.param is not None]

        persons: dict = {}
        with self.conn.cursor() as cursor:
            cursor.execute(query, filter_params)
            for row in cursor:
                bibbi_id = str(row[0])
                if bibbi_id not in persons:
                    persons[bibbi_id] = BibbiPerson(
                        id=bibbi_id,
                        bare_id=row[8],
                        name=row[1],
                        dates=row[2].strip() if row[2] is not None and row[2].strip() != '' else None,
                        nasj=row[3],
                    )
                vare = BibbiVare(
                    isbn=row[4].replace('-', ''),
                    titles=[row[5]],
                    approve_date=row[7],
                )
                if row[6] is not None:
                    vare.titles.append(row[6])

                persons[bibbi_id].items.append(vare)

        for bibbi_id, bibbi_person in persons.items():
            approve_dates = [item.approve_date for item in bibbi_person.items]
            bibbi_person.newest_approved = sorted(approve_dates)[-1]

        return persons
