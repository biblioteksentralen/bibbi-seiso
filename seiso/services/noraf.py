from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from typing import Generator, Optional, Dict
from pathlib import Path
from urllib.parse import urljoin

from lxml import etree  # type: ignore
from requests import Session, HTTPError
from sickle.oaiexceptions import NoRecordsMatch

from seiso.console.helpers import log_path
from seiso.services.oai import OaiPmh

from seiso.common.noraf_record import NorafJsonRecord, NorafXmlRecord
from seiso.common.xml import XmlNode
from seiso.common.interfaces import NorafRecord

logger = logging.getLogger(__name__)

TYPE_PERSON = 'PERSON'
TYPE_CORPORATION = 'CORPORATION'
TYPE_CONFERENCE = 'CONFERENCE'


class NorafRecordNotFound(IOError):
    pass


class NorafUpdateFailed(IOError):
    def __init__(self, record_id, http_error):
        self.record_id = record_id
        self.http_error = http_error
        self.message = 'Failed to update Noraf record %s: %s' % (record_id, http_error.response.text)


class Noraf:

    api_base_url = 'https://authority.bibsys.no/authority/rest/authorities/v2'
    oai_pmh_endpoint = 'https://authority.bibsys.no/authority/rest/oai'
    sru_endpoint = 'https://authority.bibsys.no/authority/rest/sru'

    def __init__(self,
                 apikey: Optional[str] = None,
                 session: Optional[Session] = None,
                 update_log: Optional[Path] = None,
                 read_only_mode: bool = True):
        if update_log is None:
            update_log = log_path('noraf_updates.log')
        self.read_only_mode = read_only_mode
        self.session = session or Session()
        self.session.headers.update({'User-Agent': 'BibbiSeiso/1.0 (Dan.Michael.Heggo@bibsent.no)'})
        if apikey is not None:
            self.session.headers.update({'Authorization': 'apikey %s' % apikey})
        self.update_log = Path(update_log)
        self.update_log.parent.mkdir(exist_ok=True, parents=True)
        self.update_log.touch()

    def get(self, identifier: str) -> NorafJsonRecord:
        response = self.session.get(
            urljoin(self.api_base_url, identifier),
            params={
                'format': 'json',
            },
            stream=True
        )
        if response.status_code == 404:
            raise NorafRecordNotFound(identifier)

        # The API doesn't specify encoding, so we have to do it manually
        return NorafJsonRecord(response.content.decode('utf-8'))

    def put(self, record: NorafJsonRecord, reason: str) -> None:
        if self.read_only_mode:
            logger.info("Read only mode, will not update NORAF record")
            record.dirty = False
            return
        response = self.session.put(
            urljoin(self.api_base_url, record.id),
            json=record.as_dict()
        )
        try:
            response.raise_for_status()
        except HTTPError as err:
            logger.error('Failed to update Noraf record %s: %s', record.id, err.response.text)
            raise NorafUpdateFailed(record.id, err)
        self.log_update(record, reason)
        record.dirty = False

    def post(self, record: NorafJsonRecord) -> NorafJsonRecord:
        if self.read_only_mode:
            logger.info("Read only mode, will not update NORAF record")
            return record
        response = self.session.post(
            self.api_base_url,
            json=record.as_dict()
        )
        try:
            response.raise_for_status()
        except HTTPError as err:
            logger.error('Failed to post Noraf record %s', err.response.text)
            raise

        record = NorafJsonRecord(response.text)
        logger.info('Posted new record to Noraf: %s', record.id)
        return record

    def delete(self, record: NorafJsonRecord) -> NorafJsonRecord:
        if self.read_only_mode:
            logger.info("Read only mode, will not delete NORAF record")
            return record
        response = self.session.delete(
            urljoin(self.api_base_url, record.id)
        )
        try:
            response.raise_for_status()
        except HTTPError as err:
            logger.error('Failed to delete Noraf record %s', err.response.text)
            raise

        record = NorafJsonRecord(response.text)
        logger.info('Deleted Noraf record: %s', record.id)
        return record

    def log_update(self, record, reason: str) -> None:
        line = '[%s] Oppdaterte %s - Årsak: %s' % (
            datetime.now().isoformat(),
            record.id,
            reason
        )
        with self.update_log.open('a+') as fp:
            fp.write(line + '\n')

    def search(self, query: str) -> Generator[NorafRecord, None, None]:
        start_row = 1
        max_row = 50
        while True:
            response = self.session.get(urljoin(self.api_base_url, 'query'), params={
                'q': query,
                'format': 'json',
                'start': start_row,
                'max': max_row,
            }, stream=True)

            # The API doesn't specify encoding, so we have to do it manually
            results = json.loads(response.content.decode('utf-8'))

            for res in results['results']:
                yield NorafJsonRecord(res)

            start_row += max_row
            if start_row > int(results['numFound']):
                break

    def sru_search(self, query: str) -> Generator[NorafRecord, None, None]:
        response = self.session.get(self.sru_endpoint, params={
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
            if parsed_rec := NorafXmlRecord.parse(rec):
                yield parsed_rec
            else:
                logger.error('%s - Record type not supported yet', rec.text(':controlfield[@tag="001"]'))
