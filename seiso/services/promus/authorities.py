from __future__ import annotations
import logging
from datetime import datetime

from dataclasses import dataclass, field
from typing import (
    Generic,
    Optional,
    Type,
    TypeVar,
    Union,
    Callable,
    Generator,
    Set,
)

from seiso.common.noraf_record import NorafJsonRecord
from seiso.common.interfaces import Person

from typing import TYPE_CHECKING

from seiso.services.promus.promus_record import (
    PromusCollection,
    PromusRecord,
    Query,
    QueryFilter,
    QueryFilters,
)

if TYPE_CHECKING:
    from seiso.services.promus import Promus

logger = logging.getLogger(__name__)


@dataclass
class Column:
    name: str
    alias: Optional[str] = None
    formatter: Optional[Callable] = None
    table: Optional[str] = None

    def __str__(self):
        prefixed_name = "%s.%s" % (self.table or "authority", self.name)
        if self.alias is not None:
            return "%s AS %s" % (prefixed_name, self.alias)
        return prefixed_name

    def format_value(self, value):
        if self.formatter is not None:
            return self.formatter(value)
        return value


@dataclass
class CurriculumRecord(PromusRecord):
    Code: Optional[str] = None
    Name: Optional[str] = None
    Name_N: Optional[str] = None
    Name_E: Optional[str] = None
    Name_S: Optional[str] = None
    URI: Optional[str] = None
    ValidFrom: Optional[str] = None
    ValidUntil: Optional[str] = None
    TeachedUntil: Optional[str] = None
    ReplacedBy: Optional[str] = None
    Notes: Optional[str] = None
    LastChanged: Optional[datetime] = None
    LastChanged_udir: Optional[datetime] = None
    Approved: bool = True
    Status: Optional[str] = None


@dataclass
class BibbiAuthorityRecord(PromusRecord):
    Bibsent_ID: Optional[int] = None
    Created: Optional[datetime] = None
    LastChanged: Optional[datetime] = None
    Approved: bool = True
    _DisplayValue: Optional[str] = None

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

    def get_items_query(self, marc_fields: Optional[set[int]] = None) -> Query:
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
            """.format(marc_fields=",".join(["?" for _ in marc_fields])),
            [*marc_fields, self.primary_key],
        )

        # print(query, query_params)
        # for row in self.collection.conn.select(query, query_params, normalize=False):
        #     yield Item(**row)


TBibbiAuthorityRecord = TypeVar("TBibbiAuthorityRecord", bound=BibbiAuthorityRecord)


@dataclass
class BibbiGenreRecord(BibbiAuthorityRecord):
    Title: Optional[str] = None
    Title_N: Optional[str] = None
    GeoUnderTopic: Optional[str] = None
    GeoUnderTopic_N: Optional[str] = None

    def label(self):
        if self.GeoUnderTopic is not None:
            return self.Title + " - " + " - ".join(self.GeoUnderTopic.split("$z"))
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
        # if self.GeoUnderTopic is not None:
        #    return self.GeoName + ' - ' + self.GeoUnderTopic
        # return self.GeoName


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
    Comment: Optional[str] = None
    Forkortelse: Optional[str] = None
    _DisplayValue: Optional[str] = None

    def label(self):
        return self._DisplayValue
        # if self.GeoUnderTopic is not None:
        #    return self.GeoName + ' - ' + self.GeoUnderTopic
        # return self.GeoName


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

    # def label(self):
    #    return self.PersonName  # Todo: Add more stuff

    def get_person_repr(self) -> Person:
        if self.PersonName is None:
            raise Exception("PersonName is required")
        return Person(
            vocabulary="bibbi",
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


ColumnDataTypes = list[Union[str, int, None]]
BibbiPersons = dict[str, BibbiPersonRecord]
BibbiAuthorities = dict[str, BibbiAuthorityRecord]


@dataclass
class BibbiAuthorityCollection(
    Generic[TBibbiAuthorityRecord], PromusCollection[TBibbiAuthorityRecord]
):
    last_changed_column: str = "LastChanged"
    # record_type: TP
    # record_type = BibbiAuthority
    # name_column = 'AuthName'

    def list_references(self) -> dict[int, list[PromusRecord]]:
        references: dict[int, list[PromusRecord]] = {}
        query = """
            SELECT {primary_key_column} AS primary_key,
                   {select_columns}
            FROM {table} AS authority
            WHERE ISNULL(authority.ReferenceNr, '') <> ''
        """.format(
            primary_key_column=self.primary_key_column,
            select_columns=", ".join([f.name for f in self.data_fields]),
            table=self.table_name,
        )
        for row in self._conn.select(query, normalize=False):
            if "ReferenceNr" not in row:
                print("Table does not support references")
                return {}
            ref_id: int = row["ReferenceNr"]
            promus_rec: PromusRecord = self._record_factory(row)
            references[ref_id] = references.get(ref_id, []) + [promus_rec]
        return references

    def list_records(
        self, filters: Optional[QueryFilters] = None
    ) -> Generator[TBibbiAuthorityRecord, None, None]:
        """List authorities for this table"""
        references = self.list_references()  # @TODO: Cache
        if filters is None:
            filters = QueryFilters()
        filters.append(QueryFilter("ISNULL(authority.ReferenceNr, '') = ''"))
        for record in super().list_records(filters):
            record.set_references(references.get(record.primary_key, []))
            yield record

    def get(self, bibbi_id: str) -> Optional[TBibbiAuthorityRecord]:
        return self.first(Bibsent_ID=bibbi_id)


class LinkedToNoraf(
    Generic[TBibbiAuthorityRecord], PromusCollection[TBibbiAuthorityRecord]
):
    def link_to_noraf(
        self,
        bibbi_record: TBibbiAuthorityRecord,
        noraf_json_rec: NorafJsonRecord,
        reason: str,
    ):
        logger.info(
            "Lenker Bibbi:%s (%s) til Noraf:%s (%s). Årsak: %s",
            bibbi_record.Bibsent_ID,
            bibbi_record.label(),
            noraf_json_rec.id,
            noraf_json_rec.name,
            reason,
        )

        updates = {
            "NB_ID": int(noraf_json_rec.id),
            "Origin": noraf_json_rec.origin,
            "KatStatus": noraf_json_rec.status,
            "Handle_ID": noraf_json_rec.identifiers("handle")[0].split("/", 3)[-1],
        }
        if isinstance(bibbi_record, BibbiPersonRecord):
            updates["NB_PersonNation"] = noraf_json_rec.nationality

            if noraf_json_rec.dates is not None and bibbi_record.PersonYear is None:
                updates["PersonYear"] = noraf_json_rec.dates

        if bibbi_record.Bibsent_ID:
            # Merk: Vi bruker Felles_ID fordi vi må oppdatere biautoritetene også.
            # Det er egentlig litt teit at vi dupliserer informasjonen slik, men det er slik det gjøres i Promus,
            # så vi må gjøre det på samme måte.
            self.generic_update(
                where_key="Felles_ID",
                where_value=bibbi_record.Bibsent_ID,
                updates=updates,
            )


@dataclass
class PersonCollection(
    BibbiAuthorityCollection[BibbiPersonRecord], LinkedToNoraf[BibbiPersonRecord]
):
    table_name: str = "AuthorityPerson"
    record_type: Type[BibbiPersonRecord] = BibbiPersonRecord
    primary_key_column: str = "PersonId"
    marc_fields: Set[int] = field(default_factory=lambda: {100, 600, 700})


@dataclass
class CorporationCollection(
    BibbiAuthorityCollection[BibbiCorporationRecord],
    LinkedToNoraf[BibbiCorporationRecord],
):
    table_name: str = "AuthorityCorp"
    record_type: type = BibbiCorporationRecord
    primary_key_column: str = "CorpID"
    marc_fields: Set[int] = field(default_factory=lambda: {110, 610, 710})


@dataclass
class ConferenceCollection(
    BibbiAuthorityCollection[BibbiConferenceRecord],
    LinkedToNoraf[BibbiConferenceRecord],
):
    table_name: str = "AuthorityConf"
    record_type: type = BibbiConferenceRecord
    primary_key_column: str = "ConfID"
    marc_fields: Set[int] = field(default_factory=lambda: {111, 611, 711})


@dataclass
class CurriculumCollection(PromusCollection[CurriculumRecord]):
    table_name: str = "AuthorityCurriculum"
    record_type: type = CurriculumRecord
    primary_key_column: str = "CurriculumID"
    marc_fields: Set[int] = field(default_factory=lambda: {659})
    last_changed_column: str = "LastChanged"


@dataclass
class TopicCollection(BibbiAuthorityCollection[BibbiTopicRecord]):
    table_name: str = "AuthorityTopic"
    record_type: type = BibbiTopicRecord
    primary_key_column: str = "AuthID"
    marc_fields: Set[int] = field(default_factory=lambda: {650})


@dataclass
class GeographicCollection(BibbiAuthorityCollection[BibbiGeographicRecord]):
    table_name: str = "AuthorityGeographic"
    record_type: type = BibbiGeographicRecord
    primary_key_column: str = "TopicId"
    marc_fields: Set[int] = field(default_factory=lambda: {651})


@dataclass
class GenreCollection(BibbiAuthorityCollection[BibbiGenreRecord]):
    table_name: str = "AuthorityGenre"
    record_type: type = BibbiGenreRecord
    primary_key_column: str = "TopicId"
    marc_fields: Set[int] = field(default_factory=lambda: {655})


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
            # self.topic,
            # self.genre,
            # self.geographic,
        ]

    # @TODO: Refactor: This method doesn't really work. We cannot assume that there are common fields for all authority tables, must make a selection first.
    def first(self, **kwargs) -> Optional[BibbiAuthorityRecord]:
        raise Exception("Not implemented")

    def list(self, filters: Optional[list[QueryFilter]] = None) -> BibbiAuthorities:
        raise Exception("Not implemented")

    # def has_field(self, field_name):
    #     if field_name == 'Bibsent_ID':
    #         return AuthorityCollections([x for x in self._all if isinstance(x, PersonCollection, CorporationCollection, ConferenceCollection)])
