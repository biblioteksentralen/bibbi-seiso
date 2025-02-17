"""
Verify all links from Bibbi to Noraf
"""
import argparse
import json
import logging
import os
import time
import pickle
from pathlib import Path
from textwrap import dedent
from typing import List

from dotenv import load_dotenv

from seiso.common.noraf_record import NorafJsonRecord
from seiso.common.logging import setup_logging
from seiso.console.helpers import Report, ReportHeader, storage_path
from seiso.services.noraf import Noraf, TYPE_PERSON, NorafRecordNotFound, NorafUpdateFailed, TYPE_CORPORATION, \
    TYPE_CONFERENCE
from seiso.services.promus import Promus
from seiso.services.promus.authorities import (
    BibbiCorporationRecord,
    BibbiPersonRecord,
    QueryFilter,
    QueryFilters,
    BibbiAuthorityRecord,
)

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
            bibbi_records = {
                TYPE_PERSON: list(self.get_promus_records(self.promus.authorities.person)),
                TYPE_CORPORATION: list(self.get_promus_records(self.promus.authorities.corporation)),
                TYPE_CONFERENCE: list(self.get_promus_records(self.promus.authorities.conference)),
            }

            # with cache_file.open('wb') as fp:
            #    pickle.dump(bibbi_records, fp)

            # Store a copy of all names
            # with open('bibbi_names.json', 'w', encoding='utf-8') as fp:
            #     obj = [{'id': res.id, 'name': res.name} for res in bibbi_records.values()]
            #     json.dump(obj, fp, indent=2)

        return bibbi_records

    @staticmethod
    def add_row(report, bibbi_record: BibbiAuthorityRecord, columns: List[str]):
        report.add_row([
            '{BIBBI}' + str(bibbi_record.Bibsent_ID),
            bibbi_record.label(),
            ' || '.join([ref.label() for ref in bibbi_record.get_references()]),
            # person_rec.dates or '',
            bibbi_record.LastChanged.strftime('%Y-%m-%d') if bibbi_record.LastChanged else '',
        ] + columns)

    def run(self):

        already_checked = []

        already_checked_file = 'poster_som_allerede_er_sjekket.json'
        if os.path.exists(already_checked_file):
            with open(already_checked_file, 'r', encoding='utf-8') as fp:
                already_checked = json.loads(fp.read())
                print("Already checked: %d" % len(already_checked))

        all_records = self.get_bibbi_records()

        for record_type, bibbi_records in all_records.items():

            logger.info('Checking %d Bibbi records of type %s', len(bibbi_records), record_type)
            reports_path = storage_path('reports')

            n = 0
            for bibbi_rec in bibbi_records:
                if bibbi_rec.NB_ID in already_checked:
                    continue
                noraf_id = str(bibbi_rec.NB_ID)
                try:
                    noraf_rec = self.noraf.get(noraf_id)
                    self.check_link(record_type, bibbi_rec, noraf_rec)
                except NorafRecordNotFound:
                    self.add_row(self.error_report, bibbi_rec, [
                        '{NORAF}' + noraf_id,
                        'Posten ble ikke funnet. Den kan ha blitt hardslettet.',
                    ])

                n += 1
                already_checked.append(bibbi_rec.NB_ID)
                if n % 500 == 0:
                    self.overview_report.save_json(reports_path.joinpath(f'bibbi-noraf-overgang - {record_type}.json'))
                    with open(already_checked_file, 'w', encoding='utf-8') as fp:
                        fp.write(json.dumps(already_checked))
                        print("Oppdaterte %s, poster sjekket: %d" % ( already_checked_file, len(already_checked)))

                    time.sleep(10)

            self.overview_report.save_json(reports_path.joinpath(f'bibbi-noraf-overgang - {record_type}.json'))

            self.overview_report.save_excel(
                reports_path.joinpath(f"bibbi-noraf-overgang - {record_type}.xlsx"),
                headers=[
                    ReportHeader("Bibbi-post", "ID", 15),
                    ReportHeader("", "1XX $a", 30),
                    ReportHeader("", "4XX", 40),
                    ReportHeader("", "1XX $d", 20),
                    ReportHeader("Noraf-post", "ID", 20),
                    ReportHeader("", "1XX $a", 30),
                    ReportHeader("", "4XX", 40),
                    ReportHeader("", "1XX $d", 20),
                    ReportHeader("", "Sist endret", 15),
                    ReportHeader("", "Status", 10),
                    ReportHeader("", "Kilde", 15),
                    ReportHeader(
                        "Andre Bibbi-poster", "lenket til samme Noraf-post", 30
                    ),
                ],
            )

            self.error_report.save_excel(
                reports_path.joinpath("bibbi-noraf-overgang - feil.xlsx"),
                headers=[
                    ReportHeader("Bibbi-post", "ID", 15),
                    ReportHeader("", "1XX $a", 30),
                    ReportHeader("", "4XX", 40),
                    ReportHeader("", "1XX $d", 20),
                    ReportHeader("Noraf-post", "ID", 20),
                    ReportHeader("", "Feil", 80),
                ],
            )

    @staticmethod
    def get_promus_records(table):
        return table.list(QueryFilters([
            QueryFilter('ReferenceNr IS NULL'),
            QueryFilter('Felles_ID = Bibsent_ID'),
            QueryFilter("ISNULL(NB_ID, '') != ''"),
        ]))

    def replace_promus_link(
        self,
        record_type: str,
        bibbi_rec: BibbiAuthorityRecord,
        old_noraf_rec: NorafJsonRecord,
        new_noraf_rec_id: str,
    ):
        replacement_record = self.noraf.get(new_noraf_rec_id)

        msg = 'replace_promus_link: Noraf-posten %s (%s) har blitt erstattet av %s (%s)' % (
            old_noraf_rec.id,
            str(old_noraf_rec),
            replacement_record.id,
            str(replacement_record)
        )
        logger.warning(msg)
        if isinstance(bibbi_rec, BibbiPersonRecord):
            self.promus.authorities.person.link_to_noraf(
                bibbi_rec, replacement_record, msg
            )
        elif isinstance(bibbi_rec, BibbiCorporationRecord):
            self.promus.authorities.corporation.link_to_noraf(
                bibbi_rec, replacement_record, msg
            )
        else:
            logger.warning(f"Record type not supported for linking: {record_type}")
        time.sleep(10)
        return replacement_record

    def check_link(self, record_type: str, bibbi_rec: BibbiAuthorityRecord, noraf_rec: NorafJsonRecord):
        logger.debug('%s "%s" <> %s "%s"', bibbi_rec.Bibsent_ID, bibbi_rec.label(), noraf_rec.id, noraf_rec.name)
        noraf_update_reasons = []

        bibbi_id = str(bibbi_rec.Bibsent_ID)
        bibbi_uri = f"https://id.bs.no/bibbi/{bibbi_id}"

        # 1. Check if record has been deleted or replaced
        if noraf_rec.deleted:
            if noraf_rec.replaced_by is not None and len(noraf_rec.replaced_by) > 1:
                noraf_rec = self.replace_promus_link(
                    record_type, bibbi_rec, noraf_rec, noraf_rec.replaced_by
                )
            else:
                recs = list(self.noraf.sru_search('bib.identifierAuthority=%s' % bibbi_id))
                recs = [
                    x
                    for x in recs
                    if bibbi_id in x.other_ids.get("bibbi", [])
                    or bibbi_uri in x.other_ids.get("bibbi", [])
                ]
                if len(recs) == 1:
                    noraf_rec = self.replace_promus_link(
                        record_type, bibbi_rec, noraf_rec, recs[0].id
                    )
                elif len(recs) > 1:
                    self.add_row(self.error_report, bibbi_rec, [
                        '{NORAF}' + noraf_rec.id,
                        'Noraf-posten har blitt slettet. Fant mer enn én annen Noraf-post som lenker til Bibbi-posten.',
                    ])
                    time.sleep(8)
                    return
                else:
                    self.add_row(self.error_report, bibbi_rec, [
                        '{NORAF}' + noraf_rec.id,
                        'Noraf-posten har blitt slettet uten at Bibbi-ID-en har blitt overført til en ny post.',
                    ])
                    time.sleep(8)
                    return

        # 2. Check that record type matches expected record type
        if noraf_rec.record_type != record_type:
            logger.error(f'NORAF record {noraf_rec.id} is of type {noraf_rec.record_type}, but expected {record_type}')
            self.add_row(self.error_report, bibbi_rec, [
                '{NORAF}' + noraf_rec.id,
                'Ugyldig posttype: ' + noraf_rec.record_type,
            ])
            time.sleep(8)
            return

        # 3. Ensure that a reverse mapping exists (from NORAF to BIBBI)
        if len(noraf_rec.identifiers('bibbi')) == 0:
            bibbi_uri = 'https://id.bs.no/bibbi/' + bibbi_id
            noraf_rec.set_identifiers('bibbi', [bibbi_uri])
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
                self.add_row(self.error_report, bibbi_rec, [
                    '{NORAF}' + noraf_rec.id,
                    'Den eksisterende Noraf-posten inneholder feil: ' + err.message
                ])
                return

            time.sleep(10)

        self.add_row(
            self.overview_report,
            bibbi_rec,
            [
                "{NORAF}" + noraf_rec.id,
                noraf_rec.name,
                " || ".join(noraf_rec.alt_names),
                noraf_rec.dates or "",
                noraf_rec.modified.strftime("%Y-%m-%d"),
                noraf_rec.status,
                noraf_rec.origin,
                " || ".join(
                    [
                        x
                        for x in noraf_rec.identifiers("bibbi")
                        if x != bibbi_id and x != bibbi_uri
                    ]
                ),
            ],
        )

        # time.sleep(1)


def main():
    parser = argparse.ArgumentParser(
        description=dedent(
            """
            Scriptet validerer og oppdaterer alle Bibbi-Noraf-mappingene for personer, korporasjoner og konferanser.
            For hver mapping, henter det en oppdatert autoritetspost fra NORAF-API-et 
            (eksempel: <https://authority.bibsys.no/authority/rest/authorities/v2/1602158143313?format=json>)
            og sjekker følgende:
 
              (1) Er NORAF-posten fortsatt i bruk?
                (1.1) Hvis posten har blitt erstattet av en annen post, legger vi inn den nye NORAF-ID-en i Promus.           
                (1.2) Hvis posten har blitt slettet uten at vi finner en erstatningspost, skriver vi ut en advarsel. 
              (2) Har posten samme type som BIBBI-posten (person, korporasjon, konferanse)? 
              (3) Har NORAF-posten mapping tilbake til BIBBI-posten? Hvis den ikke har det, legger vi det inn.
            
            Foreløpig gjør det ikke noe med nasjonalitet, datoer osv., men det kan nok legges til.

            Scriptet lager to sett med Excel-filer:
              (1) bibbi-noraf-overgang - (type).xlsx : Oversikt over alle mappingene
              (2) bibbi-noraf-overgang - feil.xlsx : Oversikt over alle feil som ikke kunne rettes automatisk
            """
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument('-v', '--verbose', action='store_true', help='More verbose output.')
    parser.add_argument('--dry-run', action='store_true', help='Dry run mode.')

    args = parser.parse_args()

    if args.verbose:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)

    noraf_key = os.getenv('BARE_KEY')
    if noraf_key is None:
        logger.warning('No API key set')
    noraf = Noraf(noraf_key, read_only_mode=args.dry_run)

    promus = Promus(read_only_mode=args.dry_run)

    Processor(noraf, promus).run()
