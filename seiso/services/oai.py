from __future__ import annotations

import json
import logging
import os
import sys
from dataclasses import dataclass, field
from datetime import datetime
from hashlib import md5
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
class OaiPmhSettings:
    endpoint: str
    metadata_prefix: str
    metadata_schema: str
    storage_dir: Path
    oai_set: Optional[str] = None
    request_args: Optional[dict] = field(default_factory=dict)


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

    def __init__(self, settings: OaiPmhSettings):
        self.settings = settings

    def harvest(self, callback=None, **kwargs):

        harvest_summary_file = self.settings.storage_dir.joinpath('summary.json')
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

        if current_harvest.resumption_token is not None:
            harvest_options = {
                'resumptionToken': current_harvest.resumption_token,
            }
        else:
            harvest_options = {
                'metadataPrefix': self.settings.metadata_prefix,
                'set': self.settings.oai_set,
                **kwargs
            }

        print(self.settings.endpoint, harvest_options)

        sickle = Sickle(self.settings.endpoint, max_retries=0, timeout=10, **self.settings.request_args)
        records = sickle.ListRecords(**harvest_options)

        t0 = time()
        for record in records:
            ident = record.header.identifier
            record_id = ident.split(':')[-1]
            # We use md5 just to get a slightly more uniform distribution,
            # since the prefixes and suffixes are often very non-uniform.
            file_dir = self.settings.storage_dir.joinpath(md5(record_id.encode('utf-8')).hexdigest()[:2])
            filename = file_dir.joinpath('%s.xml' % record_id)

            if record.deleted:
                current_harvest.deleted += 1
                if filename.exists():
                    logger.info('Removing deleted record: %s', str(filename))
                    os.remove(str(filename))
            else:
                doc = XmlNode(
                    etree.fromstring(record.raw),
                    self.settings.metadata_schema,
                    {'oai': 'http://www.openarchives.org/OAI/2.0/', 'schema': self.settings.metadata_schema}
                )

                marc = doc.first('oai:metadata/schema:record', True)

                if callback is not None:
                    callback(marc)

                if len(record_id) > 2:
                    file_dir.mkdir(exist_ok=True)

                    if marc is None:
                        print(doc.serialize())
                        sys.exit(1)
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
