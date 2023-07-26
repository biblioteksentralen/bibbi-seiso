from __future__ import annotations
import logging
from abc import abstractmethod
from datetime import datetime

from dataclasses import dataclass, field, InitVar, fields, Field
from typing import List, Dict, Optional, Tuple, Union, Callable, Generator, Set, ClassVar

from seiso.common.noraf_record import NorafJsonRecord
from seiso.common.interfaces import Authority, Person

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from seiso.services.promus import Promus

logger = logging.getLogger(__name__)


def escape_column_name(value):
    return '"%s"' % value.replace('"', '')


@dataclass
class Query:
    query: str
    params: List[any]


@dataclass
class QueryFilter:
    stmt: str
    param: Optional[Union[str, list]] = None


@dataclass
class QueryFilters:
    filters: List[QueryFilter] = field(default_factory=list)

    def append(self, query_filter: QueryFilter):
        self.filters.append(query_filter)

    def get_where_stmt(self, initial='WHERE'):
        if len(self.filters) > 0:
            return initial + ' ' + ' AND '.join([filt.stmt for filt in self.filters])
        return ''

    def get_query_params(self):
        for _filter in self.filters:
            if isinstance(_filter.param, list):
                for _param in _filter.param:
                    yield _param
            elif _filter.param is not None:
                yield _filter.param


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


@dataclass
class PromusRecord:
    primary_key: int = None
    collection: PromusCollection = None
    special_fields: ClassVar = ('primary_key', 'collection')

    def __post_init__(self):
        pass

    def update(self, **kwargs):
        return self.collection.update_record(self, **kwargs)


@dataclass
class CurriculumRecord(PromusRecord):
    Code: str = None
    Name: str = None
    Name_N: str = None
    Name_E: str = None
    Name_S: str = None
    URI: str = None
    ValidFrom: str = None
    ValidUntil: str = None
    TeachedUntil: str = None
    ReplacedBy: str = None
    Notes: str = None
    LastChanged: datetime = None
    LastChanged_udir: datetime = None
    Approved: bool = True
    Status: str = None


@dataclass
class BibbiAuthorityRecord(PromusRecord):
    Bibsent_ID: int = None
    Created: datetime = None
    LastChanged: datetime = None
    Approved: bool = True
    _DisplayValue: str = None

    # bibbi_id: str = None
    # vocabulary: str = 'bibbi'
    # created: Optional[date] = None
    # modified: Optional[date] = None

    def set_references(self, references):
        self._references = references

    def get_references(self):
        return self._references

    def label(self):
        return self._DisplayValue

    def __str__(self):
        return f'id="{self.Bibsent_ID}" label="{self.label()}"'

    def get_items_query(self, marc_fields: Optional[Tuple[str]] = None) -> Query:
        """Henter bibliografiske poster som denne autoriteten er brukt på, evt. avgrenset på relasjonstype.

        marc_fields: For autoriteter som kan brukes både som ansvarshaver (1XX, 7XX) og emne (6XX), kan en avgrense på
                     en av delene. Som standard hentes alt.
        """
        marc_fields = marc_fields or self.collection.marc_fields

        return Query(
            """
                SELECT
                   item.Item_ID AS primary_key,
                   item.Bibbinr AS id,
                   item.Varenr AS product_key,
                   item.Title AS title
                FROM Item AS item
                INNER JOIN ItemField AS field
                    ON field.Item_ID = item.Item_ID
                    AND field.FieldCode IN ({marc_fields})
                WHERE field.Authority_ID = ?
            """.format(marc_fields=','.join(['?' for _ in marc_fields])),
            [*marc_fields, self.primary_key]
        )

        # print(query, query_params)
        # for row in self.collection.conn.select(query, query_params, normalize=False):
        #     yield Item(**row)


@dataclass
class BibbiGenreRecord(BibbiAuthorityRecord):
    Title: Optional[str] = None
    Title_N: Optional[str] = None
    GeoUnderTopic: Optional[str] = None
    GeoUnderTopic_N: Optional[str] = None

    def label(self):
        if self.GeoUnderTopic is not None:
            return self.Title + ' - ' + ' - '.join(self.GeoUnderTopic.split('$z'))
        return self.Title


@dataclass
class BibbiGeographicRecord(BibbiAuthorityRecord):
    GeoName: Optional[str] = None
    GeoName_N: Optional[str] = None
    GeoUnderTopic: Optional[str] = None
    GeoUnderTopic_N: Optional[str] = None
    GeoDetail: Optional[str] = None
    UnderTopic: Optional[str] = None
    UnderTopic_N: Optional[str] = None
    NotInUse: Optional[str] = None
    Reference: Optional[str] = None
    ReferenceNr: Optional[str] = None
    # TODO: Add remaining fields

    def label(self):
        return self._DisplayValue
        #if self.GeoUnderTopic is not None:
        #    return self.GeoName + ' - ' + self.GeoUnderTopic
        #return self.GeoName


@dataclass
class BibbiTopicRecord(BibbiAuthorityRecord):
    Title: Optional[str] = None
    Title_N: Optional[str] = None
    CorpDetail: Optional[str] = None
    CorpDetail_N: Optional[str] = None
    SortingTitle: Optional[str] = None
    UnderTopic: Optional[str] = None
    Qualifier: Optional[str] = None
    Qualifier_N: Optional[str] = None
    DeweyNr: Optional[str] = None
    TopicDetail: Optional[str] = None
    TopicLang: Optional[str] = None
    FieldCode: Optional[str] = None
    Security_ID: Optional[str] = None
    UserID: Optional[str] = None
    LastChanged: Optional[str] = None
    Created: Optional[str] = None
    Approved: Optional[str] = None
    ApproveDate: Optional[str] = None
    ApprovedUserID: Optional[str] = None
    Reference: Optional[str] = None
    ReferenceNr: Optional[str] = None
    BibbiNr: Optional[str] = None
    NotInUse: Optional[str] = None
    Source: Optional[str] = None
    BibbiReferenceNr: Optional[str] = None
    GeoUnderTopic: Optional[str] = None
    GeoUnderTopic_N: Optional[str] = None
    UnderTopic_N: Optional[str] = None
    WebDeweyNr: Optional[str] = None
    WebDeweyApproved: Optional[str] = None
    BS_Fortsettelser_Fjern: Optional[str] = None
    BS_Fortsettelser_Serietittel: Optional[str] = None
    BS_Fortsettelser_Kommentar: Optional[str] = None
    WebDeweyKun: Optional[str] = None
    Bibsent_ID: Optional[str] = None
    Comment: Optional[str] = None
    Forkortelse: Optional[str] = None
    _DisplayValue: Optional[str] = None

    def label(self):
        return self._DisplayValue
        #if self.GeoUnderTopic is not None:
        #    return self.GeoName + ' - ' + self.GeoUnderTopic
        #return self.GeoName

@dataclass
class BibbiPersonRecord(BibbiAuthorityRecord):
    """Person i Bibbi"""
    PersonId: Optional[str] = None
    PersonName: Optional[str] = None
    PersonNr: Optional[str] = None
    PersonTitle: Optional[str] = None
    PersonTitle_N: Optional[str] = None
    PersonYear: Optional[str] = None
    PersonNation: Optional[str] = None
    SortingTitle: Optional[str] = None
    MusicCast: Optional[str] = None
    MusicNr: Optional[str] = None
    Arrangment: Optional[str] = None
    Toneart: Optional[str] = None
    TopicTitle: Optional[str] = None
    SortingSubTitle: Optional[str] = None
    UnderTopic: Optional[str] = None
    UnderTopic_N: Optional[str] = None
    Qualifier: Optional[str] = None
    Qualifier_N: Optional[str] = None
    UnderMainText: Optional[str] = None
    LanguageText: Optional[str] = None
    DeweyNr: Optional[str] = None
    TopicLang: Optional[str] = None
    IssnNr: Optional[str] = None
    FieldCode: Optional[str] = None
    Security_ID: Optional[str] = None
    UserID: Optional[str] = None
    ApproveDate: Optional[str] = None
    ApprovedUserID: Optional[str] = None
    BibbiNr: Optional[str] = None
    NotInUse: Optional[str] = None
    Reference: Optional[str] = None
    ReferenceNr: Optional[str] = None
    Source: Optional[str] = None
    bibbireferencenr: Optional[str] = None
    PersonForm: Optional[str] = None
    NotNovelette: Optional[str] = None
    WebDeweyNr: Optional[str] = None
    WebDeweyApproved: Optional[str] = None
    WebDeweyKun: Optional[str] = None
    Felles_ID: Optional[str] = None
    NB_ID: Optional[str] = None
    NB_PersonNation: Optional[str] = None
    NB_Origin: Optional[str] = None
    MainPerson: Optional[str] = None
    Comment: Optional[str] = None
    Origin: Optional[str] = None
    KatStatus: Optional[str] = None
    Gender: Optional[str] = None
    Handle_ID: Optional[str] = None
    Nametype: Optional[str] = None
    KlasseSpraak_Tid: Optional[str] = None
    KlasseSpraak_Tid_Approved: Optional[str] = None
    KlasseTid: Optional[str] = None
    KlasseComic: Optional[str] = None

    #def label(self):
    #    return self.PersonName  # Todo: Add more stuff

    def get_person_repr(self) -> Person:
        return Person(
            vocabulary='bibbi',
            id=str(self.Bibsent_ID),
            name=self.PersonName,
            alt_names=[x.PersonName for x in self.get_references()],
            nationality=self.PersonNation,
            gender=self.Gender,
            dates=str(self.PersonYear),
            created=self.Created,
            modified=self.LastChanged,
            # country_codes=...
        )


@dataclass
class BibbiCorporationRecord(BibbiAuthorityRecord):
    """Korporasjon i Bibbi"""
    CorpID: Optional[str] = None
    CorpName: Optional[str] = None
    CorpName_N: Optional[str] = None
    CorpDep: Optional[str] = None
    CorpPlace: Optional[str] = None
    CorpDate: Optional[str] = None
    CorpFunc: Optional[str] = None
    CorpNr: Optional[str] = None
    CorpDetail: Optional[str] = None
    CorpDetail_N: Optional[str] = None
    SortingTitle: Optional[str] = None
    TopicTitle: Optional[str] = None
    SortingSubTitle: Optional[str] = None
    UnderTopic: Optional[str] = None
    UnderTopic_N: Optional[str] = None
    Qualifier: Optional[str] = None
    Qualifier_N: Optional[str] = None
    DeweyNr: Optional[str] = None
    TopicDetail: Optional[str] = None
    TopicLang: Optional[str] = None
    MusicNr: Optional[str] = None
    MusicCast: Optional[str] = None
    Arrangment: Optional[str] = None
    ToneArt: Optional[str] = None
    FieldCode: Optional[str] = None
    Security_ID: Optional[str] = None
    UserID: Optional[str] = None
    ApproveDate: Optional[str] = None
    ApprovedUserID: Optional[str] = None
    BibbiNr: Optional[str] = None
    NotInUse: Optional[str] = None
    Reference: Optional[str] = None
    ReferenceNr: Optional[str] = None
    Source: Optional[str] = None
    bibbireferencenr: Optional[str] = None
    GeoUnderTopic: Optional[str] = None
    GeoUnderTopic_N: Optional[str] = None
    WebDeweyNr: Optional[str] = None
    WebDeweyApproved: Optional[str] = None
    WebDeweyKun: Optional[str] = None
    NB_ID: Optional[str] = None
    NB_Origin: Optional[str] = None
    Felles_ID: Optional[str] = None
    MainPerson: Optional[str] = None
    Origin: Optional[str] = None
    KatStatus: Optional[str] = None
    Comment: Optional[str] = None
    Lov: Optional[str] = None
    Handle_ID: Optional[str] = None


@dataclass
class BibbiConferenceRecord(BibbiAuthorityRecord):
    """Arrangement/hendelse/møte/konferanse i Bibbi"""
    ConfID: Optional[str] = None
    ConfName: Optional[str] = None
    ConfName_N: Optional[str] = None
    ConfPlace: Optional[str] = None
    ConfDate: Optional[str] = None
    ConfNr: Optional[str] = None
    ConfDetail: Optional[str] = None
    SortingTitle: Optional[str] = None
    TopicTitle: Optional[str] = None
    SortingSubTitle: Optional[str] = None
    UnderTopic: Optional[str] = None
    UnderTopic_N: Optional[str] = None
    Qualifier: Optional[str] = None
    DeweyNr: Optional[str] = None
    TopicDetail: Optional[str] = None
    TopicLang: Optional[str] = None
    FieldCode: Optional[str] = None
    Security_ID: Optional[str] = None
    UserID: Optional[str] = None
    ApproveDate: Optional[str] = None
    ApprovedUserID: Optional[str] = None
    BibbiNr: Optional[str] = None
    NotInUse: Optional[str] = None
    Reference: Optional[str] = None
    ReferenceNr: Optional[str] = None
    Source: Optional[str] = None
    bibbireferencenr: Optional[str] = None
    WebDeweyNr: Optional[str] = None
    WebDeweyApproved: Optional[str] = None
    WebDeweyKun: Optional[str] = None
    NB_ID: Optional[str] = None
    NB_Origin: Optional[str] = None
    Felles_ID: Optional[str] = None
    MainPerson: Optional[str] = None
    Origin: Optional[str] = None
    KatStatus: Optional[str] = None
    Comment: Optional[str] = None
    Handle_ID: Optional[str] = None


ColumnDataTypes = List[Union[str, int, None]]
BibbiPersons = Dict[str, BibbiPersonRecord]
BibbiAuthorities = Dict[str, BibbiAuthorityRecord]


@dataclass
class PromusCollection:
    promus: InitVar[Promus]
    table_name: str
    record_type: type
    primary_key_column: str
    last_changed_column: Optional[str] = None
    marc_fields: Set[int] = field(default_factory=lambda: {})
    data_fields: Tuple[Field] = field(default_factory=lambda: ())

    def __post_init__(self, promus: Promus):
        self._promus = promus
        self._conn = promus.connection()
        self.data_fields = tuple(f for f in fields(self.record_type) if f.name not in self.record_type.special_fields)

    def _record_factory(self, row):
        return self.record_type(
            collection=self,
            **row
        )

    def list(self, filters: Optional[QueryFilters] = None) -> Generator[PromusRecord, None, None]:
        """List authorities for this table"""
        filters = filters or QueryFilters()
        query = """
            SELECT {primary_key} AS primary_key,
                   {select_columns}
            FROM {table} AS authority 
            {where_stmt}
        """.format(
            primary_key=self.primary_key_column,
            select_columns=', '.join([f.name for f in self.data_fields]),
            table=self.table_name,
            where_stmt=filters.get_where_stmt(),
        )
        query_params = [*filters.get_query_params()]
        # print(query, query_params)
        logger.debug(f'Promus query: "{query}" with params: {repr(query_params)}')
        for row in self._conn.select(query, query_params, normalize=False):
            yield self._record_factory(row)

    def insert(self, **kwargs):
        query = 'INSERT INTO {table} ({keys}) VALUES ({values})'.format(
            table=self.table_name,
            keys=', '.join(['%s' % escape_column_name(key) for key in kwargs.keys()]),
            values=', '.join(['?' for _ in kwargs.keys()]),
        )
        params = list(kwargs.values())
        if self._conn.update(query, params) == 0:
            raise Exception('No rows affected by the INSERT query: %s' % query)

    def update_record(self, record: PromusRecord, **kwargs) -> List[Change]:
        if not isinstance(record, self.record_type):
            raise ValueError('record must be instance of ' + str(self.record_type))

        changes = []
        for key, value in kwargs.items():
            if not hasattr(record, key):
                raise ValueError('Key does not exist on %s: %s' % (type(record), key))
            existing_value = getattr(record, key)
            if value != existing_value:
                if isinstance(value, datetime) and isinstance(existing_value, datetime):
                    if abs((value - existing_value).total_seconds()) <= 1:
                        continue  # ignore round-off errors
                changes.append(Change(column=key, new_value=value, old_value=getattr(record, key), record=record))

                setattr(record, key, value)

        self.generic_update(self.primary_key_column,
                            int(record.primary_key),
                            {change.column: change.new_value for change in changes})

        return changes

    def generic_update(self, where_key: str, where_value: Union[str, int], updates: dict):

        if len(updates.items()) > 0:

            if self.last_changed_column is not None:
                updates[self.last_changed_column] = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:23]  # milliseconds with 3 digits,

            set_stmt = ', '.join(['%s=?' % escape_column_name(key) for key in updates.keys()])
            set_params = list(updates.values())

            query = f"UPDATE {self.table_name} SET {set_stmt} WHERE {escape_column_name(where_key)}=?"
            params = set_params + [where_value]

            if self._conn.update(query, params) == 0:
                raise Exception('No rows affected by the UPDATE query: %s' % query)

    def all(self, **kwargs) -> Generator[PromusRecord, None, None]:
        return self.list(filters=QueryFilters([
            QueryFilter(
                '"%s" = ?' % key.replace('"', ''),
                value
            )
            for key, value in kwargs.items()
        ]))

    def first(self, **kwargs) -> Optional[PromusRecord]:
        for res in self.all(**kwargs):
            return res


@dataclass
class Change:
    column: str
    new_value: Optional[str] = None
    old_value: Optional[str] = None
    record: Optional[PromusRecord] = None


@dataclass
class BibbiAuthorityCollection(PromusCollection):
    last_changed_column: str = 'LastChanged'
    # record_type = BibbiAuthority
    # name_column = 'AuthName'

    def list_references(self) -> Dict[int, PromusRecord]:
        references = {}
        query = """
            SELECT {primary_key_column} AS primary_key,
                   {select_columns}
            FROM {table} AS authority
            WHERE ISNULL(authority.ReferenceNr, '') <> ''
        """.format(primary_key_column=self.primary_key_column,
                   select_columns=', '.join([f.name for f in self.data_fields]),
                   table=self.table_name)
        for row in self._conn.select(query, normalize=False):
            if 'ReferenceNr' not in row:
                print('Table does not support references')
                return {}
            ref_id = row['ReferenceNr']
            references[ref_id] = references.get(ref_id, []) + [self._record_factory(row)]
        return references

    def list(self, filters: Optional[QueryFilters] = None) -> Generator[PromusRecord, None, None]:
        """List authorities for this table"""
        references = self.list_references()  # @TODO: Cache
        if filters is None:
            filters = QueryFilters()
        filters.append(QueryFilter("ISNULL(authority.ReferenceNr, '') = ''"))
        for record in super().list(filters):
            if isinstance(record, BibbiAuthorityRecord):  # just to make PyCharm happy
                record.set_references(references.get(record.primary_key, []))
                yield record


class LinkedToNoraf(PromusCollection):

    def link_to_noraf(self, bibbi_record: BibbiAuthorityRecord, noraf_json_rec: NorafJsonRecord, reason: str):
        logger.info('Lenker Bibbi:%s (%s) til Noraf:%s (%s). Årsak: %s',
                    bibbi_record.Bibsent_ID,
                    bibbi_record.label(),
                    noraf_json_rec.id,
                    noraf_json_rec.name,
                    reason)

        updates = {
            'NB_ID': int(noraf_json_rec.id),
            'Origin': noraf_json_rec.origin,
            'KatStatus': noraf_json_rec.status,
            'Handle_ID': noraf_json_rec.identifiers('handle')[0].split('/', 3)[-1],
        }
        if isinstance(bibbi_record, BibbiPersonRecord):
            updates['NB_PersonNation'] = noraf_json_rec.nationality

            if noraf_json_rec.dates is not None and bibbi_record.PersonYear is None:
                updates['PersonYear'] = noraf_json_rec.dates

        # Merk: Vi bruker Felles_ID fordi vi må oppdatere biautoritetene også.
        # Det er egentlig litt teit at vi dupliserer informasjonen slik, men det er slik det gjøres i Promus,
        # så vi må gjøre det på samme måte.
        self.generic_update(
            where_key='Felles_ID',
            where_value=int(bibbi_record.Bibsent_ID),
            updates=updates
        )


@dataclass
class Country(PromusRecord):
    CountryShortName: str = None
    ISO_3166_Alpha_2: str = None


@dataclass
class CountryCollection(PromusCollection):
    table_name: str = 'EnumCountries'
    record_type: type = Country
    primary_key_column: str = 'CountryID'

    def __post_init__(self, promus: Promus):
        super().__post_init__(promus)
        self._short_name_map = {
            country.CountryShortName: country.ISO_3166_Alpha_2
            for country in self.list()
            if isinstance(country, Country) and country.ISO_3166_Alpha_2 is not None
        }

    @property
    def get_short_name_map(self):
        return self._short_name_map


@dataclass
class PersonCollection(BibbiAuthorityCollection, LinkedToNoraf):
    table_name: str = 'AuthorityPerson'
    record_type: type = BibbiPersonRecord
    primary_key_column: str = 'PersonId'
    marc_fields: Set[int] = field(default_factory=lambda: {100, 600, 700})

    def list(self, *args, **kwargs) -> Generator[BibbiPersonRecord, None, None]:
        return super().list(*args, **kwargs)


@dataclass
class CorporationCollection(BibbiAuthorityCollection, LinkedToNoraf):
    table_name: str = 'AuthorityCorp'
    record_type: type = BibbiCorporationRecord
    primary_key_column: str = 'CorpID'
    marc_fields: Set[int] = field(default_factory=lambda: {110, 610, 710})


@dataclass
class ConferenceCollection(BibbiAuthorityCollection, LinkedToNoraf):
    table_name: str = 'AuthorityConf'
    record_type: type = BibbiConferenceRecord
    primary_key_column: str = 'ConfID'
    marc_fields: Set[int] = field(default_factory=lambda: {111, 611, 711})


@dataclass
class CurriculumCollection(PromusCollection):
    table_name: str = 'AuthorityCurriculum'
    record_type: type = CurriculumRecord
    primary_key_column: str = 'CurriculumID'
    marc_fields: Set[int] = field(default_factory=lambda: {659})
    last_changed_column: str = 'LastChanged'


@dataclass
class TopicCollection(BibbiAuthorityCollection):
    table_name: str = 'AuthorityTopic'
    record_type: type = BibbiTopicRecord
    primary_key_column: str = 'AuthID'
    marc_fields: Set[int] = field(default_factory=lambda: {650})

    def list(self, filters: Optional[QueryFilters] = None) -> Generator[BibbiTopicRecord, None, None]:
        return super(TopicCollection, self).list(filters)

@dataclass
class GeographicCollection(BibbiAuthorityCollection):
    table_name: str = 'AuthorityGeographic'
    record_type: type = BibbiGeographicRecord
    primary_key_column: str = 'TopicId'
    marc_fields: Set[int] = field(default_factory=lambda: {651})

    def list(self, filters: Optional[QueryFilters] = None) -> Generator[BibbiGeographicRecord, None, None]:
        return super(GeographicCollection, self).list(filters)


@dataclass
class GenreCollection(BibbiAuthorityCollection):
    table_name: str = 'AuthorityGenre'
    record_type: type = BibbiGenreRecord
    primary_key_column: str = 'TopicId'
    marc_fields: Set[int] = field(default_factory=lambda: {655})

    def list(self, filters: Optional[QueryFilters] = None) -> Generator[BibbiGenreRecord, None, None]:
        return super(GenreCollection, self).list(filters)


class AuthorityCollections:

    def __init__(self, promus: Promus):

        self.person = PersonCollection(promus)
        self.corporation = CorporationCollection(promus)
        self.conference = ConferenceCollection(promus)
        self.topic = TopicCollection(promus)
        self.genre = GenreCollection(promus)
        self.geographic = GeographicCollection(promus)
        self.curriculum = CurriculumCollection(promus)

        # Ting med Bibsent_ID?
        self._all = [
            self.person,
            # self.curriculum,
            self.corporation,
            self.conference,
            #self.topic,
            #self.genre,
            #self.geographic,
        ]

    # @TODO: Refactor: This method doesn't really work. We cannot assume that there are common fields for all authority tables, must make a selection first.
    def first(self, **kwargs) -> Optional[BibbiAuthorityRecord]:
        for table in self._all:
            if record := table.first(**kwargs):
                return record
        return None

    def list(self, filters: List[QueryFilter] = None) -> BibbiAuthorities:
        # TODO
        pass

    # def has_field(self, field_name):
    #     if field_name == 'Bibsent_ID':
    #         return AuthorityCollections([x for x in self._all if isinstance(x, PersonCollection, CorporationCollection, ConferenceCollection)])


@dataclass
class Item:
    id: str
    primary_key: int
    product_key: str
    title: str


@dataclass
class ItemCollection:
    promus: InitVar[Promus]

    def __post_init__(self, promus: Promus):
        # self._promus = promus
        self._conn = promus.connection()
        table_name = 'Item'

    def by_authority(self, authority: PromusRecord) -> Generator[Item, None, None]:
        query = """
            SELECT
               item.Bibbinr AS id,
               item.Item_ID AS primary_key, 
               item.Varenr AS product_key,
               item.Title AS title
            FROM Item AS item
            INNER JOIN ItemField AS field
                ON field.Item_ID = item.Item_ID
                AND field.FieldCode IN ({marc_fields})
            WHERE field.Authority_ID = ?
        """.format(marc_fields=','.join(['?' for _ in authority.collection.marc_fields]))
        query_params = [*authority.collection.marc_fields, authority.primary_key]

        # print(query, query_params)
        for row in self._conn.select(query, query_params, normalize=False):
            yield Item(**row)
