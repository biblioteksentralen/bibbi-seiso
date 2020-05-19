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
class BibbiPerson:
    name: str
    nasj: str
    dates: str
    newest_approved: Optional[datetime] = None
    items: List[BibbiVare] = field(default_factory=list)


@dataclass
class BarePerson:
    id: str
    name: str
    dates: str = ''
    alt_names: List[str] = field(default_factory=list)


@dataclass
class ViafPerson:
    id: str
    name: str = ''
    dates: str = ''
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
