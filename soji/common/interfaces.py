from dataclasses import dataclass, field
from typing import List, Optional, Union


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
class Strategy:
    name: str
    query: str
    provider: callable
    matcher: callable
