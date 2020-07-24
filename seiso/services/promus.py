import logging
import os
from datetime import datetime
from pathlib import Path

import pyodbc  # type: ignore
from dataclasses import dataclass
from typing import List, Dict, Optional, Tuple, Union, Generator

from seiso.common.noraf_record import NorafJsonRecord
from seiso.common.interfaces import BibbiPerson, BibbiVare, BibbiRecord

logger = logging.getLogger(__name__)

ColumnDataTypes = List[Union[str, int, None]]


class MsSql:

    def __init__(self, update_log: Path, **db_settings):
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
        self.update_log = update_log

    def cursor(self) -> pyodbc.Cursor:
        return self.connection.cursor()

    def commit(self) -> None:
        self.connection.commit()

    def select(self, query: str, params: list = None, normalize: bool = False,
              date_fields: list = None) -> Generator[dict, None, None]:
        if 'SELECT' not in query:
            raise Exception('Not a SELECT query')
        with self.cursor() as cursor:
            cursor.execute(query, params or [])
            columns = [column[0] for column in cursor.description]
            for row in cursor.fetchall():
                row = dict(zip(columns, row))
                if normalize:
                    self.normalize_row(row, date_fields=date_fields)
                yield row

    @staticmethod
    def normalize_row(row: dict, date_fields: List[str] = None) -> None:
        """
        In-place normalization of a row:

        - Ensures values are either strings or dates
        - Empty strings are converted to NULLs
        - Numbers are converted to strings
        - All strings are trimmed
        """
        date_fields = date_fields or []
        for k in row.keys():
            if row[k] is None:
                continue
            elif k in date_fields:
                row[k] = row[k].date() if row[k] else None
            else:
                row[k] = str(row[k]).strip()
                if row[k] == '':
                    row[k] = None

    @staticmethod
    def format_log_entry(query: str, params: ColumnDataTypes) -> str:
        return '[%s] %s [%s]' % (datetime.now().isoformat(), query, ','.join([
            str(param) for param in params
        ]))

    def update(self, query: str, params: List[str]):
        with self.update_log.open('a+') as fp:
            fp.write(self.format_log_entry(query, params) + '\n')
        with self.cursor() as cursor:
            cursor.execute(query, params)
            rowcount = cursor.rowcount
            self.commit()
        return rowcount

BibbiPersons = Dict[str, BibbiPerson]


@dataclass
class QueryFilter:
    stmt: str
    param: Optional[str] = None


class Promus:

    def __init__(self, server=None, database=None, user=None, password=None, update_log='logs/promus_updates.log'):
        self.connection_options = {
            'server': server or os.getenv('PROMUS_HOST'),
            'database': database or os.getenv('PROMUS_DATABASE'),
            'user': user or os.getenv('PROMUS_USER'),
            'password': password or os.getenv('PROMUS_PASSWORD'),
            'update_log': Path(update_log),
        }
        self.connection_options['update_log'].parent.mkdir(exist_ok=True, parents=True)
        self.connection_options['update_log'].touch()
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
            for row in self.conn.select('SELECT CountryShortName, ISO_3166_Alpha_2 FROM EnumCountries', normalize=True):
                if row['ISO_3166_Alpha_2'] is not None:
                    self._short_name_map[row['CountryShortName']] = row['ISO_3166_Alpha_2'].lower()
        return self._short_name_map

class Authorities:

    table_name = None

    def __init__(self, promus: Promus):
        self.promus = promus
        self.conn = promus.connection()

    def build_update_query(self, entity: BibbiRecord, **kwargs) -> Tuple[str, ColumnDataTypes]:
        kwargs['LastChanged'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:23]  # milliseconds with 3 digits
        query = 'UPDATE %s SET %s WHERE Bibsent_ID=?' % (
            self.table_name,
            ', '.join(['%s=?' % key for key in kwargs.keys()])
        )
        params = list(kwargs.values()) + [int(entity.id)]
        return query, params

    def update(self, entity: BibbiRecord, dry_run: bool, **kwargs):
        query, params = self.build_update_query(entity, **kwargs)
        if dry_run:
            logger.info('[Dry run] %s', self.conn.format_log_entry(query, params))
        else:
            if self.conn.update(query, params) == 0:
                raise Exception('No rows affected by the UPDATE query: %s' % query)


class Persons(Authorities):

    table_name = 'AuthorityPerson'

    def __init__(self, promus: Promus):
        super(Persons, self).__init__(promus)
        self.countries = Countries(promus)

    def get(self, bibbi_id: str, with_items: bool = True) -> Optional[BibbiPerson]:
        results = self.list([
            QueryFilter('person.Bibsent_ID = ?', bibbi_id)
        ], with_items=with_items)
        if bibbi_id in results:
            return results[bibbi_id]
        return None

    def list(self, filters: List[QueryFilter] = None, with_items: bool = True) -> BibbiPersons:

        if filters is None:
            filters = []

        id_map = {}

        items_select = ''
        items_join = ''
        if with_items:
            items_select = """,
            iv1.Text AS Isbn,
            item.Title,
            iv2.Text as OriginalTitle,
            item.ApproveDate
            """
            items_join = """
            LEFT JOIN ItemField AS field ON field.Authority_ID = person.PersonId AND field.FieldCode IN ('100', '600', '700')
            LEFT JOIN Item AS item ON field.Item_ID = item.Item_ID AND item.ApproveDate IS NOT NULL
            LEFT JOIN ItemFieldSubFieldView as iv1 ON iv1.Item_ID = Item.Item_ID AND iv1.FieldCode = '020' AND iv1.SubFieldCode = 'a'
            LEFT JOIN ItemFieldSubFieldView as iv2 ON iv2.Item_ID = Item.Item_ID AND iv2.FieldCode = '240' AND iv2.SubFieldCode = 'a'            
            """

        query = """
        SELECT
            person.Bibsent_ID,
            person.PersonName,
            person.PersonYear,
            person.PersonNation,
            person.NB_ID,
            person.Gender,
            person.PersonId,
            person.Created,
            person.LastChanged
            %(items_select)s

        FROM AuthorityPerson AS person
        
        %(items_join)s

        WHERE
            person.ReferenceNr IS NULL
            AND person.Felles_ID = person.Bibsent_ID

        %(filters)s

        ORDER BY person.Bibsent_ID
        """ % {
            'filters': ' '.join(['AND %s' % filt.stmt for filt in filters]),
            'items_select': items_select,
            'items_join': items_join,
        }

        filter_params = [filt.param for filt in filters if filt.param is not None]

        persons: dict = {}
        n = 0
        for row in self.conn.select(query, filter_params, normalize=True, date_fields=[
                    'Created',
                    'LastChanged',
                    'ApproveDate',
                ]):

            bibbi_id = row['Bibsent_ID']
            id_map[row['PersonId']] = bibbi_id

            if bibbi_id not in persons:
                persons[bibbi_id] = BibbiPerson(
                    id=bibbi_id,
                    created=row['Created'],
                    modified=row['LastChanged'],
                    noraf_id=row['NB_ID'],
                    name=row['PersonName'],
                    dates=row['PersonYear'],
                    nationality=row['PersonNation'],
                    country_codes=self.country_codes_from_nationality(row['PersonNation']),
                    gender=row['Gender'],
                )

            if row.get('Isbn') is not None:
                vare = BibbiVare(
                    isbn=row['Isbn'].replace('-', ''),
                    titles=[row['Title']],
                    approve_date=row['ApproveDate'],
                )
                if row['OriginalTitle'] is not None:
                    vare.titles.append(row['OriginalTitle'])

                persons[bibbi_id].items.append(vare)

        for bibbi_id, bibbi_person in persons.items():
            approve_dates = [item.approve_date for item in bibbi_person.items]
            if len(approve_dates) == 0:
                bibbi_person.newest_approved = None
            else:
                bibbi_person.newest_approved = sorted(approve_dates)[-1]

        query = """
            SELECT
                person.ReferenceNr,
                person.PersonName
            FROM
                AuthorityPerson AS person
            WHERE
                len(person.ReferenceNr) > 0
        """
        for row in self.conn.select(query, normalize=True):
            ref_id = row['ReferenceNr']
            if ref_id in id_map:
                persons[id_map[ref_id]].alt_names.append(row['PersonName'])

        return persons

    def country_codes_from_nationality(self, value):
        if value is None:
            return []
        values = value.split('-')
        values = [self.countries.short_name_map[x] for x in values if x in self.countries.short_name_map]
        return values

    def link_to_noraf(self, bibbi_person: BibbiPerson, noraf_json_rec: NorafJsonRecord, dry_run: bool, reason: str):
        self.update(bibbi_person,
                    dry_run,
                    NB_ID = int(noraf_json_rec.id),
                    NB_PersonNation = noraf_json_rec.nationality,
                    Origin = noraf_json_rec.origin,
                    KatStatus = noraf_json_rec.status,
                    Handle_ID = noraf_json_rec.identifiers('handle')[0].split('/', 3)[-1]
                    )
        logger.info('Lenker Bibbi:%s (%s) til Noraf:%s (%s). Årsak: %s',
                    bibbi_person.id,
                    bibbi_person.name,
                    noraf_json_rec.id,
                    noraf_json_rec.name,
                    reason)

