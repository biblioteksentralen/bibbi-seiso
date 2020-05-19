from __future__ import annotations
from typing import Generator, TypedDict, Optional
from lxml import etree
from requests import Session
from .interfaces import Candidate, BarePerson, ViafPerson


class XmlNode:

    def __init__(self, node: etree._Element, namespace: str):
        self.node = node
        self.ns = namespace
        self.nsmap = {'main': self.ns}

    def make(self, node: etree._Element):
        return XmlNode(node, self.ns)

    def path(self, path: str, xpath: bool = False):
        if xpath:
            return path.replace(':', 'main:')
        return path.replace(':',  '{%s}' % self.ns)

    def all(self, path: str, xpath: bool = False) -> Generator[XmlNode, None, None]:
        if xpath:
            for node in self.node.xpath(self.path(path, xpath=True), namespaces=self.nsmap):
                yield self.make(node)
        for node in self.node.findall(self.path(path)):
            yield self.make(node)

    def first(self, path: str, xpath: bool = False) -> Optional[XmlNode]:
        if xpath:
            return next(self.all(path, xpath))

    def text(self, path: str = None, xpath: bool = False) -> Optional[str]:
        if path is None:
            return self.node.text
        node = self.first(self.path(path, xpath=xpath))
        if node is not None:
            return node.text()

    def all_text(self, path: str) -> Generator[str, None, None]:
        for node in self.node.findall(self.path(path)):
            yield node.text

    def __getattr__(self, name):
        return getattr(self.node, name)


def get_viaf_candidates(query: str, session: Session = None) -> Generator[Candidate, None, None]:
    """
    Selv om VIAF-API-et kan returnere både JSON og XML, er JSON-representasjonen litt sub-par.
    Lister med bare ett element returneres f.eks. som et objekt i stedet, noe som gjør at man alltid
    må sjekke om noe er liste eller objekt. Derfor bruker vi XML.
    """

    session = session or Session()

    response = session.get(
        'https://www.viaf.org/viaf/search',
        params={'query': query},
        headers={'Accept': 'application/xml'}
    )

    data = XmlNode(etree.fromstring(response.text.encode('utf-8')), 'http://viaf.org/viaf/terms#')

    for cluster in data.all('.//:VIAFCluster'):
        if cluster.text(':nameType') != 'Personal':
            continue

        person = None

        # Main heading
        for heading in cluster.all(':mainHeadings/:mainHeadingEl'):
            heading_source, heading_id = heading.text(':id').split('|')

            if heading_source == 'BIBSYS':
                person = BarePerson(
                    id=heading_id,
                    name=heading.text(':datafield/:subfield[@code="a"]', xpath=True),
                    dates=heading.text(':datafield/:subfield[@code="d"]', xpath=True)
                )

        # x400s
        if person is not None:
            for heading in cluster.all(':x400s/:x400'):
                if 'BIBSYS' in heading.all_text(':sources/:s'):
                    for subfield in heading.all(':datafield/:subfield'):
                        if subfield.get('code') == 'a':
                            person.alt_names.append(subfield.text())

        else:
            person = ViafPerson(id=cluster.text(':viafID'))

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
