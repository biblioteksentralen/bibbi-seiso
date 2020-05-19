from __future__ import annotations

import logging
from typing import Generator

from lxml import etree  # type:ignore
from requests import Session

from soji.common.interfaces import BarePersonRecord, BareRecord
from soji.common.xml import XmlNode

logger = logging.getLogger(__name__)


class Bare:

    def __init__(self, session: Session = None):
        self.session = session or Session()

    def search(self, query: str) -> Generator[BareRecord, None, None]:
        response = self.session.get('https://authority.bibsys.no/authority/rest/sru', params={
            'operation': 'searchRetrieve',
            'query': query,
            'recordSchema': 'marcxchange',
            'version': '1.2',
        }).text

        doc = XmlNode(
            etree.fromstring(response.encode('utf-8')),
            'info:lc/xmlns/marcxchange-v1'
        )

        for rec in doc.all('.//:record'):

            if tag100 := rec.first(':datafield[@tag="100"]'):
                yield BarePersonRecord(
                    id=rec.text(':controlfield[@tag="001"]'),
                    name=tag100.text(':subfield[@code="a"]'),
                    dates=tag100.text(':subfield[@code="d"]', default=''),
                    bibbi_ids=rec.all_text(
                        ':datafield[@tag="024"][./:subfield[@code="2"]/text() = "bibbi"]/:subfield[@code="a"]',
                        xpath=True
                    ),
                    alt_names=[],  # @TODO: From 400x
                )
            else:
                logger.debug('Not implemented yet')

    def get_record(self, identifier: str):
        for rec in self.search('rec.identifier="%s"' % identifier):
            if rec.id == identifier:
                return rec
        return None
