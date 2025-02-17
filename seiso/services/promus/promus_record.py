from __future__ import annotations
from datetime import datetime

from dataclasses import dataclass, field, InitVar, fields, Field
import logging
from typing import (
    Generic,
    Optional,
    Type,
    TypeVar,
    Union,
    Generator,
    ClassVar,
)

from seiso.services.promus.promus import Promus

logger = logging.getLogger(__name__)


def escape_column_name(value):
    return '"%s"' % value.replace('"', "")


@dataclass
class Change:
    column: str
    new_value: Optional[str] = None
    old_value: Optional[str] = None
    record: Optional[PromusRecord] = None


@dataclass
class Query:
    query: str
    params: list


@dataclass
class QueryFilter:
    stmt: str
    param: Optional[Union[str, list]] = None


@dataclass
class QueryFilters:
    filters: list[QueryFilter] = field(default_factory=list)

    def append(self, query_filter: QueryFilter):
        self.filters.append(query_filter)

    def get_where_stmt(self, initial="WHERE"):
        if len(self.filters) > 0:
            return initial + " " + " AND ".join([filt.stmt for filt in self.filters])
        return ""

    def get_query_params(self):
        for _filter in self.filters:
            if isinstance(_filter.param, list):
                for _param in _filter.param:
                    yield _param
            elif _filter.param is not None:
                yield _filter.param


@dataclass
class PromusRecord:
    primary_key: int  # = None
    collection: PromusCollection  # = None
    special_fields: ClassVar = ("primary_key", "collection")

    def __post_init__(self):
        pass

    def update(self, **kwargs):
        return self.collection.update_record(self, **kwargs)


TPromusRecord = TypeVar("TPromusRecord", bound=PromusRecord)


@dataclass
class PromusCollection(Generic[TPromusRecord]):
    promus: InitVar[Promus]
    table_name: str
    record_type: Type[TPromusRecord]
    primary_key_column: str
    last_changed_column: Optional[str] = None
    marc_fields: set[int] = field(default_factory=set)
    data_fields: list[Field] = field(default_factory=list)

    def __post_init__(
        self,
        promus: Promus,
    ):
        self._promus = promus
        self._conn = promus.connection()
        special_fields: list[str] = getattr(self.record_type, "special_fields", [])
        data_fields = [
            f for f in fields(self.record_type) if f.name not in special_fields
        ]
        self.data_fields = data_fields

    def _record_factory(self, row):
        return self.record_type(collection=self, **row)

    def list_records(
        self, filters: Optional[QueryFilters] = None
    ) -> Generator[TPromusRecord, None, None]:
        """List authorities for this table"""
        filters = filters or QueryFilters()
        query = """
            SELECT {primary_key} AS primary_key,
                   {select_columns}
            FROM {table} AS authority 
            {where_stmt}
        """.format(
            primary_key=self.primary_key_column,
            select_columns=", ".join([f.name for f in self.data_fields]),
            table=self.table_name,
            where_stmt=filters.get_where_stmt(),
        )
        query_params = [*filters.get_query_params()]
        # print(query, query_params)
        logger.debug(f'Promus query: "{query}" with params: {repr(query_params)}')
        for row in self._conn.select(query, query_params, normalize=False):
            yield self._record_factory(row)

    def insert(self, **kwargs):
        query = "INSERT INTO {table} ({keys}) VALUES ({values})".format(
            table=self.table_name,
            keys=", ".join(["%s" % escape_column_name(key) for key in kwargs.keys()]),
            values=", ".join(["?" for _ in kwargs.keys()]),
        )
        params = list(kwargs.values())
        if self._conn.update(query, params) == 0:
            raise Exception("No rows affected by the INSERT query: %s" % query)

    def update_record(self, record: TPromusRecord, **kwargs) -> list[Change]:
        if not isinstance(record, self.record_type):
            raise ValueError("record must be instance of " + str(self.record_type))

        changes: list[Change] = []
        for key, value in kwargs.items():
            if not hasattr(record, key):
                raise ValueError("Key does not exist on %s: %s" % (type(record), key))
            existing_value = getattr(record, key)
            if value != existing_value:
                if isinstance(value, datetime) and isinstance(existing_value, datetime):
                    if abs((value - existing_value).total_seconds()) <= 1:
                        continue  # ignore round-off errors
                changes.append(
                    Change(
                        column=key,
                        new_value=value,
                        old_value=getattr(record, key),
                        record=record,
                    )
                )

                setattr(record, key, value)

        self.generic_update(
            self.primary_key_column,
            int(record.primary_key),
            {change.column: change.new_value for change in changes},
        )

        return changes

    def generic_update(
        self, where_key: str, where_value: Union[str, int], updates: dict
    ):
        if len(updates.items()) > 0:
            if self.last_changed_column is not None:
                updates[self.last_changed_column] = datetime.now().strftime(
                    "%Y-%m-%d %H:%M:%S.%f"
                )[:23]  # milliseconds with 3 digits,

            set_stmt = ", ".join(
                ["%s=?" % escape_column_name(key) for key in updates.keys()]
            )
            set_params = list(updates.values())

            query = f"UPDATE {self.table_name} SET {set_stmt} WHERE {escape_column_name(where_key)}=?"
            params = set_params + [where_value]

            if self._conn.update(query, params) == 0:
                raise Exception("No rows affected by the UPDATE query: %s" % query)

    def all(self, **kwargs) -> Generator[TPromusRecord, None, None]:
        return self.list_records(
            filters=QueryFilters(
                [
                    QueryFilter('"%s" = ?' % key.replace('"', ""), value)
                    for key, value in kwargs.items()
                ]
            )
        )

    def first(self, **kwargs) -> Optional[TPromusRecord]:
        for res in self.all(**kwargs):
            return res
        return None
