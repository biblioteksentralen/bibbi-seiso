from __future__ import annotations

import logging
from typing import Generator, Optional

from lxml import etree  # type: ignore
from requests import Session

from soji.common.bare_record import BibsysJsonRecord
from soji.common.xml import XmlNode

from soji.common.interfaces import BarePersonRecord, BareRecord

logger = logging.getLogger(__name__)

TYPE_PERSON = 'PERSON'


class BareRecordNotFound(Exception):
    pass


class Bare:

    def __init__(self, apikey: Optional[str] = None, session: Optional[Session] = None):
        self.session = session or Session()
        if apikey is not None:
            self.session.headers.update({'Authorization': 'apikey %s' % apikey})

    def sru_search(self, query: str) -> Generator[BareRecord, None, None]:
        response = self.session.get('https://authority.bibsys.no/authority/rest/sru', params={
            'operation': 'searchRetrieve',
            'query': query,
            'recordSchema': 'marcxchange',
            'version': '1.2',
        }, stream=True)

        doc = XmlNode(
            etree.fromstring(response.content),
            'info:lc/xmlns/marcxchange-v1'
        )

        for rec in doc.all('.//:record'):
            if main_tag := rec.first(':datafield[@tag="100"]'):
                yield BarePersonRecord(
                    id=rec.text(':controlfield[@tag="001"]'),
                    name=main_tag.text(':subfield[@code="a"]'),
                    dates=main_tag.text_or_none(':subfield[@code="d"]'),
                    bibbi_ids=rec.all_text(
                        ':datafield[@tag="024"][./:subfield[@code="2"]/text() = "bibbi"]/:subfield[@code="a"]',
                        xpath=True
                    ),
                    country_codes=rec.all_text(':datafield[@tag="043"]/:subfield[@code="c"]'),
                    nationality=rec.text_or_none(
                        ':datafield[@tag="386"][./:subfield[@code="2"] = "bs-nasj"]/:subfield[@code="a"]',
                        xpath=True
                    ),
                    alt_names=rec.all_text(
                        ':datafield[@tag="400"]/:subfield[@code="a"]',
                        xpath=True
                    )
                )
            else:
                logger.error('Not implemented yet')

    def get(self, identifier: str) -> BibsysJsonRecord:
        response = self.session.get(
            'https://authority.bibsys.no/authority/rest/authorities/v2/{id}'.format(id=identifier),
            params={
                'format': 'json',
            },
            stream=True
        )
        if response.status_code == 404:
            raise BareRecordNotFound(identifier)

        # The API doesn't specify encoding, so we have to do it manually
        return BibsysJsonRecord(response.content.decode('utf-8'))

    def put(self, record: BibsysJsonRecord):
        response = self.session.put(
            'https://authority.bibsys.no/authority/rest/authorities/v2/{id}'.format(id=record.id),
            json=record.as_dict()
        )
        response.raise_for_status()
