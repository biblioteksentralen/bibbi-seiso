import re
from typing import Optional
import unidecode  # type: ignore
from fuzzywuzzy import fuzz  # type: ignore
from ..common.interfaces import Candidate, Strategy, Match, BibbiPerson, BibbiVare


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


def compare_dates(date1: Optional[str], date2: Optional[str]) -> bool:
    year1 = date1[:4] if date1 is not None else date1
    year2 = date2[:4] if date2 is not None else date2
    return year1 == year2


def match_names_and_dates(bibbi_person: BibbiPerson,
                          candidate: Candidate,
                          strategy: Strategy
                          ) -> Optional[Match]:
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
            if bibbi_person.dates is None:
                match.date_similarity = 'Mangler'
            else:
                match.date_similarity = 'Lik verdi'
        elif bibbi_person.dates is not None and candidate.person.dates is not None:
            match.date_similarity = 'ULIKE verdier'
        elif bibbi_person.dates is None:
            match.date_similarity = 'Mangler i Bibbi'
        else:
            match.date_similarity = 'Mangler i Noraf'

        return match
    return None


def title_matcher(bibbi_person: BibbiPerson,
                  bibbi_item: BibbiVare,
                  candidate: Candidate,
                  strategy: Strategy
                  ) -> Optional[Match]:
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
    return None


def isbn_matcher(bibbi_person: BibbiPerson,
                 bibbi_item: BibbiVare,
                 candidate: Candidate,
                 strategy: Strategy) -> Optional[Match]:
    match = match_names_and_dates(bibbi_person, candidate, strategy)
    if match is not None and bibbi_item.isbn in candidate.isbns:
        match.title_similarity = 'ISBN: %s' % bibbi_item.isbn
        return match
    return None


# def viaf_matcher_without_noraf(bibbi_person: BibbiPerson,
# bibbi_item: BibbiVare, candidate: Candidate, strategy) -> Optional[Match]:
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
