import sys
from typing import Generator
from requests import Session
from json import JSONDecodeError
from .interfaces import Candidate, BarePerson


def get_alma_candidates(query: str, session: Session = None) -> Generator[Candidate, None, None]:

    session = session or Session()
    response = session.get('https://ub-lsm.uio.no/alma/search', params={
        'query': query,
        'nz': 'true',
    })
    try:
        response = response.json()
    except JSONDecodeError:
        print(query)
        print("GOT INVALID RESPONSE")
        # print(response.text)
        sys.exit(1)

    for result in response['results']:
        for creator in result.get('creators', []):
            if 'id' in creator:
                bare_id = creator['id'].replace('(NO-TrBIB)', '')
                yield Candidate(
                    person=BarePerson(
                        id=bare_id,
                        name=creator['name'],
                        dates=creator.get('dates', '')
                    ),
                    title=result.get('title'),
                    isbns=[isbn.replace('-', '') for isbn in result.get('isbns')],
                )
