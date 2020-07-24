from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime, date
from typing import List, Optional, Union, Callable, Generator, Dict

from requests import Session


@dataclass
class BibbiVare:
    isbn: str
    approve_date: datetime
    titles: List[str] = field(default_factory=list)


@dataclass
class Authority:
    """Autoritet i Bibbi eller Noraf"""
    id: str   # MERK: Vi bruker alltid Bibsent_ID som id!
    name: str
    created: Optional[date] = None
    modified: Optional[date] = None
    alt_names: List[str] = field(default_factory=list)

    def __str__(self):
        return self.name


@dataclass
class Person(Authority):
    """Person i Bibbi eller Noraf"""
    gender: Optional[str] = None
    dates: Optional[str] = None
    country_codes: List[str] = field(default_factory=list)  # MARC 043
    nationality: Optional[str] = None  # MARC 386

    def __str__(self):
        out = self.name
        if self.dates is not None:
            out += ' (%s)' % self.dates
        return out


@dataclass
class Corporation(Authority):
    """Korporasjon i Bibbi eller Noraf"""


@dataclass
class BibbiRecord(Authority):
    """Autoritetspost i Bibbi"""
    noraf_id: Optional[str] = None
    newest_approved: Optional[datetime] = None
    items: List[BibbiVare] = field(default_factory=list)


@dataclass
class BibbiPerson(BibbiRecord, Person):
    """Person i Bibbi"""


@dataclass
class NorafPerson(Person):
    """Person i Noraf. Ikke nødvendigvis en komplett post, kan være en referanse"""
    pass

@dataclass
class NorafCorporation(Corporation):
    """Korporasjon i Noraf. Ikke nødvendigvis en komplett post, kan være en referanse"""
    pass


IdentifierMap = Dict[str, List[str]]

@dataclass
class NorafRecord(Authority):
    """Fullstendig Noraf-post for person, korporasjon eller annet"""
    other_ids: IdentifierMap = field(default_factory=dict)


@dataclass
class NorafPersonRecord(NorafPerson, NorafRecord):
    """Fullstendig Noraf-post for person"""
    pass

class NorafCorporationRecord(NorafCorporation, NorafRecord):
    pass



@dataclass
class ViafPerson(Person):
    """Person i VIAF"""
    pass


@dataclass
class Candidate:
    person: Union[NorafPerson, ViafPerson]
    title: str
    isbns: List[str] = field(default_factory=list)


@dataclass
class NoMatch:
    strategy: str = ''
    target: None = None
    name_similarity: str = ''
    date_similarity: str = ''
    title_similarity: str = ''


@dataclass
class Match:
    strategy: str
    target: Union[NorafPerson, ViafPerson]
    name_similarity: str = ''
    date_similarity: str = ''
    title_similarity: str = ''


@dataclass
class Strategy:
    name: str
    query: str
    provider: Callable[
        [
            str,
            Session,
        ],
        Generator[Candidate, None, None]
    ]
    matcher: Callable[
        [
            BibbiPerson,
            BibbiVare,
            Candidate,
            Strategy,
        ],
        Optional[Match]
    ]
