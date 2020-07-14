from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional, Union, Callable, Generator

from requests import Session


@dataclass
class BibbiVare:
    isbn: str
    approve_date: datetime
    titles: List[str] = field(default_factory=list)


@dataclass
class Person:
    id: str
    name: str
    dates: Optional[str] = None
    country_codes: List[str] = field(default_factory=list)
    nationality: Optional[str] = None

    def __str__(self):
        out = '%s %s' % (self.id, self.name)
        if self.dates is not None:
            out += ' %s' % self.dates
        return out


@dataclass
class BibbiPerson(Person):
    # Bibbi person
    bare_id: Optional[str] = None
    newest_approved: Optional[datetime] = None
    items: List[BibbiVare] = field(default_factory=list)


@dataclass
class BarePerson(Person):
    # Reference to a BARE person record, not a complete record
    alt_names: List[str] = field(default_factory=list)


@dataclass
class BareRecord():
    # A complete BARE record
    id: str
    name: str
    bibbi_ids: List[str] = field(default_factory=list)


@dataclass
class BarePersonRecord(BareRecord, BarePerson):
    pass


@dataclass
class ViafPerson(Person):
    alt_names: List[str] = field(default_factory=list)


@dataclass
class Candidate:
    person: Union[BarePerson, ViafPerson]
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
    target: Union[BarePerson, ViafPerson]
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
