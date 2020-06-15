import sys
import logging
from typing import Generator
from requests import Session
from json import JSONDecodeError
from .interfaces import Candidate, BarePerson

logger = logging.getLogger(__name__)


def alma_search(query: str, session: Session = None) -> dict:
    session = session or Session()
    response = session.get('https://ub-lsm.uio.no/alma/search', params={
        'query': query,
        'nz': 'true',
    })
    try:
        return response.json()
    except JSONDecodeError:
        logger.error('Received INVALID JSON response from Alma')
        logger.error('Our query: %s', query)
        logger.error('Response: %s', response.text)
        sys.exit(1)


def get_alma_candidates(query: str, session: Session = None) -> Generator[Candidate, None, None]:
    data = alma_search(query, session)
    for result in data['results']:
        for creator in result.get('creators', []):
            if 'id' in creator:
                bare_id = creator['id'].replace('(NO-TrBIB)', '')
                yield Candidate(
                    person=BarePerson(
                        id=bare_id,
                        name=creator['name'],
                        dates=creator.get('dates')
                    ),
                    title=result.get('title'),
                    isbns=[isbn.replace('-', '') for isbn in result.get('isbns')],
                )
