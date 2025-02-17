from dataclasses import dataclass
from typing import Optional
from seiso.services.promus.authorities import PromusRecord
from seiso.services.promus.promus import Promus
from seiso.services.promus.promus_record import PromusCollection


@dataclass
class Country(PromusRecord):
    CountryShortName: str
    ISO_3166_Alpha_2: Optional[str] = None


@dataclass
class CountryCollection(PromusCollection):
    table_name: str = "EnumCountries"
    record_type: type = Country
    primary_key_column: str = "CountryID"

    def __post_init__(self, promus: Promus):
        super().__post_init__(promus)
        self._short_name_map = {
            country.CountryShortName: country.ISO_3166_Alpha_2
            for country in self.list_records()
            if isinstance(country, Country) and country.ISO_3166_Alpha_2 is not None
        }

    @property
    def short_name_map(self):
        return self._short_name_map


class EnumsCollections:
    def __init__(self, promus: Promus):
        self.countries = CountryCollection(promus)
