"""
Verify all links from Bibbi to Noraf
"""
import argparse
import logging
import os
import time
import pickle
from pathlib import Path

from dotenv import load_dotenv

from seiso.common.noraf_record import NorafJsonRecord
from seiso.common.interfaces import BibbiPerson
from seiso.common.logging import setup_logging
from seiso.console.helpers import Report, ReportHeader, storage_path
from seiso.services.noraf import Noraf, TYPE_PERSON, NorafRecordNotFound, NorafUpdateFailed
from seiso.services.promus import Promus
from seiso.services.promus.authorities import QueryFilter

logger = setup_logging(level=logging.INFO)

load_dotenv()


class Processor:

    cache_filename = 'bibbi_records.cache'

    def __init__(self, noraf: Noraf, promus: Promus):
        self.noraf: Noraf = noraf
        self.promus: Promus = promus
        self.overview_report: Report = Report()
        self.error_report: Report = Report()

    def get_bibbi_records(self):
        cache_file = Path(self.cache_filename)
        if cache_file.exists() and cache_file.stat().st_mtime > time.time() - 1800:  # 30 min
            with cache_file.open('rb') as fp:
                bibbi_records = pickle.load(fp)
        else:
            if cache_file.exists():
                logger.info('Cache file exists, but is too old: %d', time.time() - cache_file.stat().st_mtime)
            logger.info('Fetching records from Promus')
            bibbi_records = self.get_promus_records()
            with cache_file.open('wb') as fp:
                pickle.dump(bibbi_records, fp)

            # Store a copy of all names
            # with open('bibbi_names.json', 'w', encoding='utf-8') as fp:
            #     obj = [{'id': res.id, 'name': res.name} for res in bibbi_records.values()]
            #     json.dump(obj, fp, indent=2)

        return bibbi_records

    def run(self):
        bibbi_records = self.get_bibbi_records()

        logger.info('Checking %d persons linked to Noraf', len(bibbi_records))
        reports_path = storage_path('reports')

        n = 0
        for bibbi_rec in bibbi_records.values():
            noraf_id = bibbi_rec.noraf_id
            try:
                noraf_rec = self.noraf.get(noraf_id)
                self.check_link(bibbi_rec, noraf_rec)
            except NorafRecordNotFound:
                self.error_report.add_person_row(bibbi_rec, [
                    '{NORAF}' + noraf_id,
                    'Posten ble ikke funnet. Den kan ha blitt hardslettet.',
                ])

            n += 1
            if n % 500 == 0:
                self.overview_report.save_json(reports_path.joinpath('bibbi-noraf-overgang - oversikt personer.json'))
                time.sleep(10)

        self.overview_report.save_json(reports_path.joinpath('bibbi-noraf-overgang - oversikt personer.json'))

        self.overview_report.save_excel(reports_path.joinpath('bibbi-noraf-overgang - oversikt personer.xlsx'), headers=[
            ReportHeader('Bibbi-post', 'ID', 15),
            ReportHeader('', '1XX $a', 30),
            ReportHeader('', '4XX', 40),
            ReportHeader('', '1XX $d', 20),
            ReportHeader('', 'Sist endret', 15),

            ReportHeader('Noraf-post', 'ID', 20),
            ReportHeader('', '1XX $a', 30),
            ReportHeader('', '4XX', 40),
            ReportHeader('', '1XX $d', 20),
            ReportHeader('', 'Sist endret', 15),
            ReportHeader('', 'Status', 10),
            ReportHeader('', 'Kilde', 15),

            ReportHeader('Andre Bibbi-poster', 'lenket til samme Noraf-post', 30),
        ])

        self.error_report.save_excel(reports_path.joinpath('bibbi-noraf-overgang - feil.xlsx'), headers=[
            ReportHeader('Bibbi-post', 'ID', 15),
            ReportHeader('', '1XX $a', 30),
            ReportHeader('', '4XX', 40),
            ReportHeader('', '1XX $d', 20),
            ReportHeader('', 'Sist endret', 15),

            ReportHeader('Noraf-post', 'ID', 20),
            ReportHeader('', 'Feil', 80),
        ])

    def get_promus_records(self):
        return self.promus.authorities.person.list([
            QueryFilter('ReferenceNr IS NULL'),
            QueryFilter('Felles_ID = Bibsent_ID'),
            QueryFilter('NB_ID IS NOT NULL'),
        ], with_items=False)

    def replace_promus_link(self, bibbi_rec, old_noraf_rec, new_noraf_rec_id):
        replacement_record = self.noraf.get(new_noraf_rec_id)

        msg = 'replace_promus_link: Noraf-posten %s (%s) har blitt erstattet av %s (%s)' % (
            old_noraf_rec.id,
            str(old_noraf_rec),
            replacement_record.id,
            str(replacement_record)
        )
        logger.warning(msg)
        self.promus.authorities.person.link_to_noraf(bibbi_rec, replacement_record, False, reason=msg)
        time.sleep(10)
        return replacement_record

    def check_link(self, bibbi_rec: BibbiPerson, noraf_rec: NorafJsonRecord):
        logger.debug('%s "%s" <> %s "%s"', bibbi_rec.id, bibbi_rec.name, noraf_rec.id, noraf_rec.name)
        noraf_update_reasons = []

        # Check record type
        if noraf_rec.record_type != TYPE_PERSON:
            logger.error('%s - Invalid record type: %s', noraf_rec.id, noraf_rec.record_type)
            self.error_report.add_person_row(bibbi_rec, [
                '{NORAF}' + noraf_rec.id,
                'Ugyldig posttype: ' + noraf_rec.record_type,
            ])
            time.sleep(8)
            return

        # Check if target Noraf record has been deleted/replaced
        if noraf_rec.deleted:
            if noraf_rec.replaced_by is not None and len(noraf_rec.replaced_by) > 1:
                noraf_rec = self.replace_promus_link(bibbi_rec, noraf_rec, noraf_rec.replaced_by)
            else:
                recs = list(self.noraf.sru_search('bib.identifierAuthority=%s' % bibbi_rec.id))
                recs = [x for x in recs if bibbi_rec.id in x.other_ids.get('bibbi', [])]
                if len(recs) == 1:
                    noraf_rec = self.replace_promus_link(bibbi_rec, noraf_rec, recs[0].id)
                elif len(recs) > 1:
                    self.error_report.add_person_row(bibbi_rec, [
                        '{NORAF}' + noraf_rec.id,
                        'Noraf-posten har blitt slettet. Fant mer enn én annen Noraf-post som lenker til Bibbi-posten.',
                    ])
                    time.sleep(8)
                    return
                else:
                    self.error_report.add_person_row(bibbi_rec, [
                        '{NORAF}' + noraf_rec.id,
                        'Noraf-posten har blitt slettet uten at Bibbi-ID-en har blitt overført til en ny post.',
                    ])
                    time.sleep(8)
                    return

        # 2. Ensure Bibbi identifier is set
        if len(noraf_rec.identifiers('bibbi')) == 0:
            noraf_rec.set_identifiers('bibbi', [bibbi_rec.id])
            noraf_update_reasons.append('La til Bibbi-lenker')

        # 3. Ensure nationality is set correctly

        # to_remove = [field for field in noraf_rec.all('386') if field.value('2') == 'bs-nasj']
        # for field in to_remove:
        #     noraf_rec.remove(field)
        #     noraf_update_reasons.append('Fjernet 386 $2 bs-nasj')
        #
        # fields = [field for field in noraf_rec.all('386') if field.value('2') == 'bibbi']
        # if len(fields) == 0 and bibbi_rec.nationality is not None:
        #     noraf_rec.add(BibsysJsonMarcField.construct('386', ' ', ' ', [
        #         {
        #             "subcode": "a",
        #             "value": bibbi_rec.nationality,
        #         },
        #         {
        #             "subcode": "m",
        #             "value": "Nasjonalitet/regional gruppe",
        #         },
        #         {
        #             "subcode": "2",
        #             "value": "bibbi",
        #         },
        #     ]))
        #     noraf_update_reasons.append('La til 386 $2 bibbi')

        # Update record if dirty
        if noraf_rec.dirty:
            try:
                self.noraf.put(noraf_rec, reason='verify_bibbi_noraf: ' + ', '.join(noraf_update_reasons))
            except NorafUpdateFailed as err:
                self.error_report.add_person_row(bibbi_rec, [
                    '{NORAF}' + noraf_rec.id,
                    'Den eksisterende Noraf-posten inneholder feil: ' + err.message
                ])
                return

            time.sleep(10)

        self.overview_report.add_person_row(bibbi_rec, [
            '{NORAF}' + noraf_rec.id,
            noraf_rec.name,
            ' || '.join(noraf_rec.alt_names),
            noraf_rec.dates or '',
            noraf_rec.modified.strftime('%Y-%m-%d'),
            noraf_rec.status,
            noraf_rec.origin,
            ' || '.join([x for x in noraf_rec.identifiers('bibbi') if x != bibbi_rec.id]),
        ])

        # time.sleep(1)


def main():
    parser = argparse.ArgumentParser(description='Verify all Bibbi-Noraf mappings')

    parser.add_argument('-v', '--verbose', action='store_true', help='More verbose output.')

    args = parser.parse_args()

    if args.verbose:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)

    noraf_key = os.getenv('BARE_KEY')
    if noraf_key is None:
        logger.warning('No API key set')
    noraf = Noraf(noraf_key)

    promus = Promus()

    Processor(noraf, promus).run()
