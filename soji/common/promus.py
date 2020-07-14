import logging
import os
import sys
from collections import OrderedDict

import pyodbc  # type: ignore
from dataclasses import dataclass, field
from typing import List, Dict, Optional
from sqlparams import SQLParams
from soji.common.interfaces import BibbiPerson, BibbiVare

logger = logging.getLogger(__name__)


class MsSql:

    def __init__(self, **db_settings):
        if os.name == 'posix':
            connection_string = ';'.join([
                'DRIVER={FreeTDS}',
                'Server=%(server)s',
                'Database=%(database)s',
                'UID=%(user)s',
                'PWD=%(password)s',
                'TDS_Version=8.0',
                'Port=1433',
            ]) % db_settings
        else:
            connection_string = ';'.join([
                'Driver={SQL Server}',
                'Server=%(server)s',
                'Database=%(database)s',
                'Trusted_Connection=yes',
            ]) % db_settings
        self.connection: pyodbc.Connection = pyodbc.connect(connection_string)

    def cursor(self) -> pyodbc.Cursor:
        return self.connection.cursor()

    def commit(self) -> None:
        self.connection.commit()


BibbiPersons = Dict[str, BibbiPerson]


@dataclass
class QueryFilter:
    stmt: str
    param: Optional[str] = None


class Promus:

    def __init__(self, server=None, database=None, user=None, password=None):
        self.connection_options = {
            'server': server or os.getenv('DB_SERVER'),
            'database': database or os.getenv('DB_DB'),
            'user': user or os.getenv('DB_USER'),
            'password': password or os.getenv('DB_PASSWORD'),
        }
        self.persons: Persons = Persons(self)

    def connection(self) -> MsSql:
        # Seems like only one cursor can be opened per connection.
        # Therefore, we sometimes need to open more than one connection.
        return MsSql(**self.connection_options)


class Countries:

    def __init__(self, promus: Promus):
        self.conn = promus.connection()
        self._short_name_map = {}

    @property
    def short_name_map(self):
        if len(self._short_name_map) == 0:
            with self.conn.cursor() as cursor:
                cursor.execute('SELECT CountryShortName, ISO_3166_Alpha_2 FROM EnumCountries', [])
                for row in cursor.fetchall():
                    self._short_name_map[row[0]] = row[1]
        return self._short_name_map

class Persons:

    def __init__(self, promus: Promus):
        self.conn = promus.connection()
        self.countries = Countries(promus)

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
                        bare_id=str(row[8]),
                        name=row[1],
                        dates=row[2].strip() if row[2] is not None and row[2].strip() != '' else None,
                        nationality=row[3],
                        country_codes=self.country_codes_from_nationality(row[3])
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

    def update(self, bibbi_person: BibbiPerson, **kwargs):
        with self.conn.cursor() as cursor:
            query = 'UPDATE AuthorityPerson SET %s WHERE Bibsent_ID=?' % ', '.join(
                ['%s=?' % key for key in kwargs.keys()]
            )
            params = list(kwargs.values()) + [int(bibbi_person.id)]

            print(query)
            print(params)

            sys.exit(1)

            cursor.execute(query, params)

            if cursor.rowcount == 0:
                raise Exception('No rows affected by the query')

            self.conn.commit()

    def country_codes_from_nationality(self, value):
        if value is None:
            return []
        values = value.split('-')
        print(values)
        values = [self.countries.short_name_map[x] for x in values]
        print(values)
        return values
