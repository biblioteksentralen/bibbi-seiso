from __future__ import annotations
import logging
from datetime import datetime

from dataclasses import dataclass
from typing import List, Dict, Optional, Tuple, Union, Callable

from seiso.common.noraf_record import NorafJsonRecord
from seiso.common.interfaces import BibbiPerson, BibbiVare, BibbiRecord

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from seiso.services.promus import Promus

logger = logging.getLogger(__name__)

ColumnDataTypes = List[Union[str, int, None]]
BibbiPersons = Dict[str, BibbiPerson]
BibbiRecords = Dict[str, BibbiRecord]


@dataclass
class QueryFilter:
    stmt: str
    param: Optional[str] = None


@dataclass
class Column:
    name: str
    alias: Optional[str] = None
    formatter: Optional[Callable] = None
    table: Optional[str] = None

    def __str__(self):
        prefixed_name = '%s.%s' % (self.table or 'authority', self.name)
        if self.alias is not None:
            return '%s AS %s' % (prefixed_name, self.alias)
        return prefixed_name

    def format_value(self, value):
        if self.formatter is not None:
            return self.formatter(value)
        return value


class AuthorityCollection:
    table_name = None
    item_cls = BibbiRecord
    primary_key = 'AuthID'
    name_column = 'AuthName'
    select_columns = [
        Column('Bibsent_ID', 'id'),
        Column('Created', 'created'),
        Column('LastChanged', 'modified'),
    ]
    date_columns = [
        'created',
        'modified',
        'item_approve_date',
    ]
    marc_fields=()

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

    def _build_list_query_with_items(self, filters):
        return self._build_list_query(
            filters,
            columns=self.select_columns + [
                Column('Text', 'item_isbn', table='iv1'),
                Column('Title', 'item_title', table='item'),
                Column('Text', 'item_original_title', table='iv2'),
                Column('ApproveDate', 'item_approve_date', table='item'),
            ],
            joins="""
                LEFT JOIN ItemField AS field 
                    ON field.Authority_ID = authority.{primary_key} 
                    AND field.FieldCode IN ({marc_fields})
                LEFT JOIN Item AS item 
                    ON field.Item_ID = item.Item_ID 
                    AND item.ApproveDate IS NOT NULL
                LEFT JOIN ItemFieldSubFieldView as iv1 
                    ON iv1.Item_ID = Item.Item_ID 
                    AND iv1.FieldCode = '020' 
                    AND iv1.SubFieldCode = 'a'
                LEFT JOIN ItemFieldSubFieldView as iv2 
                    ON iv2.Item_ID = Item.Item_ID 
                    AND iv2.FieldCode = '240' 
                    AND iv2.SubFieldCode = 'a'
            """.format(
                primary_key=self.primary_key,
                marc_fields=', '.join(["'%s'" % marc_field for marc_field in self.marc_fields]),
            ))

    def _build_list_query(self, filters: list, columns: Optional[list] = None, joins: Optional[str] = None):
        return """
            SELECT {primary_key} AS local_id, {columns} 
            FROM {table} AS authority 
            {joins} 
            WHERE {filters} 
            ORDER BY authority.Bibsent_ID
        """.format(
            primary_key=self.primary_key,
            columns=', '.join([str(column) for column in columns or self.select_columns]),
            table=self.table_name,
            joins=joins or '',
            filters=' AND '.join([filt.stmt for filt in filters]),
        ), [filt.param for filt in filters if filt.param is not None]

    def list(self, filters: List[QueryFilter] = None, with_items: bool = True) -> BibbiRecords:
        id_map = {}

        if with_items:
            query, params = self._build_list_query_with_items(filters)
        else:
            query, params = self._build_list_query(filters)

        results: dict = {}
        for row in self.conn.select(query, params, normalize=True, date_fields=self.date_columns):
            bibbi_id = row['id']
            id_map[row['local_id']] = bibbi_id
            if bibbi_id not in results:
                results[bibbi_id] = self._make_record(row)

            if row.get('item_isbn') is not None:
                vare = BibbiVare(
                    isbn=row['item_isbn'].replace('-', ''),
                    titles=[row['item_title']],
                    approve_date=row['item_approve_date'],
                )
                if row['item_original_title'] is not None:
                    vare.titles.append(row['item_original_title'])
                results[bibbi_id].items.append(vare)

        for bibbi_id, bibbi_record in results.items():
            approve_dates = [item.approve_date for item in bibbi_record.items]
            if len(approve_dates) == 0:
                bibbi_record.newest_approved = None
            else:
                bibbi_record.newest_approved = sorted(approve_dates)[-1]

        query = """
            SELECT
                authority.ReferenceNr AS ref,
                authority.{name_column} AS name
            FROM
                {table} AS authority
            WHERE
                len(authority.ReferenceNr) > 0
        """.format(table=self.table_name, name_column=self.name_column)
        for row in self.conn.select(query, normalize=True):
            ref_id = row['ref']
            if ref_id in id_map:
                results[id_map[ref_id]].alt_names.append(row['name'])

        return results

    def get(self, bibbi_id: str, with_items: bool = True) -> Optional[BibbiRecord]:
        results = self.list([
            QueryFilter('Bibsent_ID = ?', bibbi_id)
        ], with_items=with_items)
        if bibbi_id in results:
            return results[bibbi_id]
        return None

    def _make_record(self, row):
        record_data = {
            column.alias: column.format_value(row[column.alias])
            for column in self.select_columns
        }
        return self.item_cls(**record_data)


class CountryCollection:

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


class PersonCollection(AuthorityCollection):
    table_name = 'AuthorityPerson'
    item_cls = BibbiPerson
    primary_key = 'PersonId'
    name_column = 'PersonName'
    marc_fields=(100, 600, 700)

    def __init__(self, promus: Promus):
        super(PersonCollection, self).__init__(promus)
        self.select_columns += [
            Column('NB_ID', 'noraf_id'),
            Column('PersonName', 'name'),
            Column('PersonYear', 'dates'),
            Column('PersonNation', 'nationality'),
            Column('Gender', 'gender'),
            Column('PersonNation', 'country_codes', lambda x: self.country_codes_from_nationality(x)),
        ]
        self.countries = CountryCollection(promus)

    def get(self, *args, **kwargs) -> Optional[BibbiPerson]:
        return super(PersonCollection, self).get(*args, **kwargs)

    def list(self, *args, **kwargs) -> BibbiPersons:
        return super(PersonCollection, self).list(*args, **kwargs)

    def country_codes_from_nationality(self, value):
        if value is None:
            return []
        values = value.split('-')
        values = [self.countries.short_name_map[x] for x in values if x in self.countries.short_name_map]
        return values

    def link_to_noraf(self, bibbi_person: BibbiPerson, noraf_json_rec: NorafJsonRecord, dry_run: bool, reason: str):
        self.update(bibbi_person,
                    dry_run,
                    NB_ID=int(noraf_json_rec.id),
                    NB_PersonNation=noraf_json_rec.nationality,
                    Origin=noraf_json_rec.origin,
                    KatStatus=noraf_json_rec.status,
                    Handle_ID=noraf_json_rec.identifiers('handle')[0].split('/', 3)[-1]
                    )
        logger.info('Lenker Bibbi:%s (%s) til Noraf:%s (%s). Årsak: %s',
                    bibbi_person.id,
                    bibbi_person.name,
                    noraf_json_rec.id,
                    noraf_json_rec.name,
                    reason)


class TopicCollection(AuthorityCollection):
    table_name = 'AuthorityTopic'


class CorporationCollection(AuthorityCollection):
    table_name = 'AuthorityCorp'


class GeographicCollection(AuthorityCollection):
    table_name = 'AuthorityGeographic'


class GenreCollection(AuthorityCollection):
    table_name = 'AuthorityGenre'


class AuthorityCollections():

    def __init__(self, promus: Promus):

        self.person = PersonCollection(promus)
        self.corporation = CorporationCollection(promus)
        # self.conference = ConferenceTable(promus)
        self.topic = TopicCollection(promus)
        self.genre = GenreCollection(promus)
        self.geographic = GeographicCollection(promus)

        self._all = [
            self.person,
            # self.corporation,
            # self.conference,
            #self.topic,
            #self.genre,
            #self.geographic,
        ]

    def get(self, bibbi_id: str, with_items: bool = True) -> Optional[BibbiRecord]:
        for table in self._all:
            if record := table.get(bibbi_id, with_items):
                return record
        return None

    def list(self, filters: List[QueryFilter] = None, with_items: bool = True) -> BibbiRecords:
        # TODO
        pass

