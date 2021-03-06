from __future__ import annotations
from typing import Generator, Union
from lxml import etree  # type: ignore
from requests import Session
import logging
from seiso.common.interfaces import Candidate, NorafPerson, ViafPerson
from seiso.common.xml import XmlNode

logger = logging.getLogger(__name__)


def get_person_from_viaf_cluster(cluster: XmlNode) -> Union[ViafPerson, NorafPerson]:
    person = None

    # Main heading
    for heading in cluster.all(':mainHeadings/:mainHeadingEl'):
        heading_source, heading_id = heading.text(':id').split('|')

        if heading_source == 'BIBSYS':
            person = NorafPerson(
                id=heading_id,
                created=None,
                modified=None,
                name=heading.text(':datafield/:subfield[@code="a"]', xpath=True),
                dates=heading.text_or_none(':datafield/:subfield[@code="d"]', xpath=True)
            )

    # x400s
    if person is not None:
        for heading in cluster.all(':x400s/:x400'):
            if 'BIBSYS' in heading.all_text(':sources/:s'):
                for subfield in heading.all(':datafield/:subfield'):
                    if subfield.get('code') == 'a':
                        person.alt_names.append(subfield.text())
        return person

    return ViafPerson(
        id=cluster.text(':viafID'),
        created=None,
        modified=None,
        name='',
        dates=None
    )


def get_viaf_candidates(query: str, session: Session = None) -> Generator[Candidate, None, None]:
    """
    Selv om VIAF-API-et kan returnere både JSON og XML, er JSON-representasjonen litt sub-par.
    Lister med noraf ett element returneres f.eks. som et objekt i stedet, noe som gjør at man alltid
    må sjekke om noe er liste eller objekt. Derfor bruker vi XML.
    """

    session = session or Session()

    response = session.get(
        'https://www.viaf.org/viaf/search',
        params={'query': query},
        headers={'Accept': 'application/xml'},
        stream=True
    )

    data = XmlNode(etree.fromstring(response.content), 'http://viaf.org/viaf/terms#')
    clusters = list(data.all('.//:VIAFCluster'))

    logger.debug('VIAF search returned %d clusters', len(clusters))

    for cluster in clusters:
        if cluster.text(':nameType') != 'Personal':
            logger.debug('Ignoring VIAF cluster of type %s', cluster.text(':nameType'))
            continue

        person = get_person_from_viaf_cluster(cluster)

        # ISBNs
        isbns = list(cluster.all_text(':ISBNs/:data/:text'))

        # Works
        work_titles = list(cluster.all_text(':titles/:work/:title'))

        # Works
        for work_title in work_titles:
            yield(Candidate(
                person=person,
                title=work_title,
                isbns=isbns,
            ))
