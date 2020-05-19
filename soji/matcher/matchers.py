import re
from typing import Optional, Union
from dataclasses import dataclass
import unidecode
from fuzzywuzzy import fuzz
from ..common.interfaces import BarePerson, ViafPerson, Candidate, Strategy
from ..common.promus import BibbiPerson, BibbiVare


def fuzzy_match(value1: str, value2: str) -> int:
    value1 = unidecode.unidecode(value1).lower()
    value2 = unidecode.unidecode(value2).lower()
    value1 = re.sub('[,.;-]', '', value1)
    value2 = re.sub('[,.;-]', '', value2)
    value1 = value1.replace('the ', '')
    value2 = value2.replace('the ', '')

    if len(value1) < 3 or len(value2) < 3:
        return False

    return max([
        # Attempts to account for partial string matches better.
        # Calls ratio using the shortest string against all n-length substrings
        # of the larger string and returns the highest score.
        fuzz.partial_ratio(value1, value2),

        # Attempts to account for similar strings out of order.
        # Calls ratio on both strings after sorting the tokens in each string.
        fuzz.token_sort_ratio(value1, value2),
    ])


def compare_dates(date1: str, date2: str) -> bool:
    return date1[:4] == date2[:4]


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


def match_names_and_dates(bibbi_person: BibbiPerson, candidate: Candidate, strategy: Strategy) -> Optional[Match]:
    name_similarity = fuzzy_match(candidate.person.name, bibbi_person.name)
    if name_similarity == 100 or (strategy.name == 'isbn' and name_similarity > 75):
        match = Match(
            strategy=strategy.name,
            target=candidate.person,
        )

        if candidate.person.name == bibbi_person.name:
            match.name_similarity = 'LIKE'
        elif name_similarity:
            match.name_similarity = 'DELVIS'
        else:
            match.name_similarity = 'Fuzzy: %d' % name_similarity

        if compare_dates(candidate.person.dates, bibbi_person.dates):
            if bibbi_person.dates == '':
                match.date_similarity = 'Mangler'
            else:
                match.date_similarity = 'Lik verdi'
        elif bibbi_person.dates != '' and candidate.person.dates != '':
            match.date_similarity = 'ULIKE verdier'
        elif bibbi_person.dates == '':
            match.date_similarity = 'Mangler i BIBBI'
        else:
            match.date_similarity = 'Mangler i BARE'

        return match


def title_matcher(bibbi_person: BibbiPerson, bibbi_item: BibbiVare, candidate: Candidate, strategy: Strategy) -> Optional[Match]:
    match = match_names_and_dates(bibbi_person, candidate, strategy)
    if match:
        for bibbi_title in bibbi_item.titles:
            title_similarity = fuzzy_match(bibbi_title, candidate.title)
            if bibbi_title == candidate.title:
                match.title_similarity = '"%s"' % bibbi_title
                return match
            elif title_similarity == 100:
                # match.sim = 'T-100'
                match.title_similarity = 'DELVIS: "%s" <--> "%s"' % (bibbi_title, candidate.title)
                return match
            elif title_similarity > 75:
                # match.sim = 'T-%d' % title_similarity
                match.title_similarity = 'FUZZY: "%s" <--> "%s"' % (bibbi_title, candidate.title)
                return match


def isbn_matcher(bibbi_person: BibbiPerson, bibbi_item: BibbiVare, candidate: Candidate, strategy: Strategy) -> Optional[Match]:
    match = match_names_and_dates(bibbi_person, candidate, strategy)
    if bibbi_item.isbn in candidate.isbns:
        match.title_similarity = 'ISBN: %s' % bibbi_item.isbn
        return match


# def viaf_matcher_without_bare(bibbi_person: BibbiPerson, bibbi_item: BibbiVare, candidate: Candidate, strategy) -> Optional[Match]:
#     if not isinstance(candidate.person, ViafPerson):
#         return
#
#     for bibbi_title in bibbi_item.titles:
#         title_similarity = fuzzy_match(bibbi_title, candidate.title)
#         if bibbi_title == candidate.title:
#             return Match(
#                 strategy='viaf_only',
#                 target=candidate.person,
#                 title_similarity='"%s"' % bibbi_title,
#             )
#         elif title_similarity == 100:
#             return Match(
#                 strategy='viaf_only',
#                 target=candidate.person,
#                 title_similarity='DELVIS: "%s" <--> "%s"' % (bibbi_title, candidate.title)
#             )
#         elif title_similarity > 75:
#             return Match(
#                 strategy='viaf_only',
#                 target=candidate.person,
#                 title_similarity='FUZZY: "%s" <--> "%s"' % (bibbi_title, candidate.title)
#             )

