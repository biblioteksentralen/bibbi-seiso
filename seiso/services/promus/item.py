from dataclasses import InitVar, dataclass
from typing import Generator

from seiso.services.promus.promus import Promus
from seiso.services.promus.promus_record import PromusRecord


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
        """.format(
            marc_fields=",".join(["?" for _ in authority.collection.marc_fields])
        )
        query_params: list[str | int | None] = [
            *authority.collection.marc_fields,
            authority.primary_key,
        ]

        # print(query, query_params)
        for row in self._conn.select(query, query_params, normalize=False):
            yield Item(**row)
