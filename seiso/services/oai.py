from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime
from json import JSONDecodeError
from time import time
from typing import Optional
from pathlib import Path

from lxml import etree  # type: ignore
from sickle import Sickle
from seiso.common.xml import XmlNode

logger = logging.getLogger(__name__)

TYPE_PERSON = 'PERSON'


@dataclass
class HarvestSummary:
    started: datetime
    ended: Optional[datetime] = None
    resumption_token: Optional[str] = None
    fetched: int = 0
    deleted: int = 0
    full: bool = True

    def serialize(self) -> dict:
        return {
            'started': self.started.isoformat(),
            'ended': self.ended.isoformat() if self.ended else None,
            'resumption_token': self.resumption_token,
            'fetched': self.fetched,
            'deleted': self.deleted,
            'full': self.full,
        }

    def save_summary(self, dest: Path):
        with dest.open('w', encoding='utf-8') as fp:
            json.dump(self.serialize(), fp, indent=3)

    @classmethod
    def load(cls, src: Path):
        try:
            with src.open('r', encoding='utf-8') as fp:
                data = json.load(fp)
                data['started'] = datetime.fromisoformat(data['started'])
                if data['ended'] is not None:
                    data['ended'] = datetime.fromisoformat(data['ended'])
                return cls(**data)
        except IOError:
            return None
        except JSONDecodeError:
            logger.warning('Invalid JSON: %s', str(src))
            return None


class OaiPmh:

    def __init__(self, endpoint_url: str):
        self.endpoint_url = endpoint_url

    def harvest(self, storage_dir: Path, callback=None, **kwargs):

        harvest_summary_file = storage_dir.joinpath('summary.json')
        last_harvest = HarvestSummary.load(harvest_summary_file)

        if last_harvest is not None and last_harvest.resumption_token is not None:
            harvest_type = 'full' if last_harvest.full else 'incremental'
            logger.info('Resuming %s harvest started at: %s. Fetched %d records so far.',
                        harvest_type,
                        last_harvest.started.isoformat(),
                        last_harvest.fetched)
            current_harvest = last_harvest
        else:
            if last_harvest is not None and last_harvest.ended is not None:
                kwargs['from'] = last_harvest.started.strftime('%Y-%m-%d')
                logger.info('Starting incremental harvest from: %s', kwargs['from'])
                current_harvest = HarvestSummary(started=datetime.now(), full=False)
            else:
                logger.info('Starting full harvest')
                current_harvest = HarvestSummary(started=datetime.now(), full=True)

        harvest_options = {
            'metadataPrefix': 'marcxchange',
            'set': 'bibsys_authorities',
            'resumptionToken': current_harvest.resumption_token,
            **kwargs
        }

        sickle = Sickle(self.endpoint_url, max_retries=10, timeout=60)
        records = sickle.ListRecords(**harvest_options)

        t0 = time()
        for record in records:
            ident = record.header.identifier
            record_id = ident.split(':')[-1]
            file_dir = storage_dir.joinpath(record_id[:2])
            filename = file_dir.joinpath('%s.xml' % record_id)

            if record.deleted:
                current_harvest.deleted += 1
                if filename.exists():
                    logger.info('Removing deleted record: %s', str(filename))
                    os.remove(str(filename))
            else:
                doc = XmlNode(
                    etree.fromstring(record.raw),
                    'info:lc/xmlns/marcxchange-v1',
                    {'oai': 'http://www.openarchives.org/OAI/2.0/'}
                )

                marc = doc.first('oai:metadata/:record', True)

                if callback is not None:
                    callback(marc)

                if len(record_id) > 2:
                    file_dir.mkdir(exist_ok=True)
                    with filename.open('wb') as fp:
                        fp.write(marc.serialize())
                    logger.debug('Stored record %s', record_id)

                    current_harvest.fetched += 1
                    if current_harvest.fetched % 1000 == 0:
                        current_harvest.resumption_token = records.resumption_token.token
                        current_speed = current_harvest.fetched / (time() - t0)
                        current_harvest.save_summary(harvest_summary_file)
                        logger.info('Received %d records @ %.2f recs/sec ', current_harvest.fetched, current_speed)

        current_harvest.ended = datetime.now()
        current_harvest.resumption_token = None
        logger.info('Harvest completed. Fetched %d records and %d deleted records',
                    current_harvest.fetched, current_harvest.deleted)
        current_harvest.save_summary(harvest_summary_file)
