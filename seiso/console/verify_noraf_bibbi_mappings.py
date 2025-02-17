"""
Verify all links from Noraf to Bibbi. Note: We need a recent OAI-PMH dump to do this.
"""
from __future__ import annotations
import argparse
import logging
import os
import json
from dataclasses import dataclass, field
from pathlib import Path
from textwrap import dedent
from typing import Sequence, Optional

import mdmail
from dotenv import load_dotenv

from seiso.common.noraf_record import NorafXmlRecord
from seiso.common.interfaces import NorafPersonRecord, NorafRecord, NorafCorporationRecord
from seiso.common.logging import setup_logging
from tqdm import tqdm
from lxml import etree
from seiso.common.xml import XmlNode
from seiso.console.helpers import Report, ReportHeader, storage_path
from seiso.services.noraf import Noraf, NorafRecordNotFound
from seiso.services.promus import Promus
from seiso.services.promus.authorities import (
    CorporationCollection,
    PersonCollection,
    QueryFilter,
    QueryFilters,
    BibbiAuthorityRecord,
    BibbiPersonRecord,
    BibbiCorporationRecord,
    BibbiConferenceRecord,
)
from seiso.constants import bibbi_uri_namespace

log = setup_logging(level=logging.INFO)

load_dotenv()

mail_recipients = os.getenv('MAIL_RECIPIENT')
mailgun_api_key = os.getenv('MAILGUN_API_KEY')
mailgun_domain = os.getenv('MAILGUN_DOMAIN')
mail_smtp={
    'host': os.getenv('MAIL_SMTP_HOST'),
    'port': os.getenv('MAIL_SMTP_PORT'),
    'tls': True,
    'user': os.getenv('MAIL_SMTP_USER'),
    'password': os.getenv('MAIL_SMTP_PASSWORD'),
}
mail_sender_name = os.getenv('MAIL_SENDER_NAME')
mail_sender_email = os.getenv('MAIL_SENDER_EMAIL')
mail_subject_template = "Resultater fra sjekk av BIBBI-NORAF-mappingene"
mail_body_template = """

| Problem | Funn | Forslag
|----|---|---|
%(notifications)s

Hilsen vokabularovervåkningstjenesten
"""


@dataclass
class Notification:
    record_id: Optional[str] = None
    record_link: Optional[str] = None
    issue: Optional[str] = None
    details: Optional[str] = None
    suggestions: list[str] = field(default_factory=list)

    def __str__(self):
        if len(self.suggestions) == 0:
            suggestions = ''
        else:
            suggestions = '<ul><li>' + '</li><li>'.join([f'For å lenke til {str(m)}: <tt>poetry run noraf link --replace {self.record_id} {m.id}' for m in self.suggestions]) + '</tt></li></ul>'
        return f"{self.record_link}: {self.issue} | {self.details} | {suggestions}"


def send_email(notifications: list[str]):
    log.info('Sending email notification about %d notifications', len(notifications))
    params = {
        'notifications': '\n'.join([str(notification) for notification in notifications]),
    }
    mdmail.send(mail_body_template % params,
                subject=mail_subject_template % params,
                from_email=f"{mail_sender_name} <{mail_sender_email}>",
                to_email=mail_recipients,
                smtp=mail_smtp)


@dataclass
class SimpleBibbiRecord:
    uri: str
    type: str
    label: str
    noraf_id: str
    original: BibbiAuthorityRecord
    name: Optional[str] = None
    dates: Optional[str] = None

    def __str__(self):
        return f'bibbi_id="{self.id}" label="{self.label}"'

    @staticmethod
    def create(src: BibbiAuthorityRecord) -> SimpleBibbiRecord:
        if not (isinstance(src, BibbiPersonRecord) or isinstance(src, BibbiCorporationRecord)
                or isinstance(src, BibbiConferenceRecord)):
            raise ValueError("Unsupported record type: %s" % str(src))
        uri = bibbi_uri_namespace + str(src.Bibsent_ID)
        rec = SimpleBibbiRecord(
            uri=uri,
            type=src.__class__.__name__,
            label=src.label(),
            noraf_id=str(src.NB_ID),
            original=src,
        )
        if isinstance(src, BibbiPersonRecord):
            rec.dates = src.PersonYear
            rec.name = src.PersonName
        elif isinstance(src, BibbiCorporationRecord):
            rec.name = src.CorpName
            rec.dates = src.CorpDate
        return rec

    def md_link(self):
        return f"[{self.id}: {self.label}]({self.uri})"


class IsDir(argparse.Action):

    def __call__(self, parser, namespace, values, option_string=None):
        prospective_dir = Path(values)
        if not prospective_dir.is_dir():
            raise argparse.ArgumentTypeError('{}:{} is not a directory'.format(self.dest, prospective_dir))
        setattr(namespace, self.dest, prospective_dir)


class Processor:

    def __init__(self, noraf: Noraf, promus: Promus, harvest_dir: Path, use_cache: bool):
        self.noraf: Noraf = noraf
        self.promus: Promus = promus
        self.harvest_dir: Path = harvest_dir
        self.use_cache: bool = use_cache
        self.dead_link_report: Report = Report()
        self.one_to_many_report: Report = Report()
        self.non_symmetric_report: Report = Report()
        self.stats: dict[str, int] = {}
        self._bibbi_noraf_mapping: dict[str, str] = {}
        self.notifications: list[Notification] = []

    @staticmethod
    def add_row(
        report,
        noraf_record: NorafPersonRecord | NorafCorporationRecord,
        columns: list[str],
    ):
        report.add_row([
            '{NORAF}' + str(noraf_record.id),
            noraf_record.name,
            ' || '.join(noraf_record.alt_names),
            noraf_record.dates or '',
            noraf_record.modified.strftime('%Y-%m-%d') if noraf_record.modified else '',
        ] + columns)

    @property
    def bibbi_noraf_mapping(self):
        """Lazy-load the Bibbi-to-Noraf concordance from Promus."""
        if len(self._bibbi_noraf_mapping) == 0:
            for table in ('AuthorityPerson', 'AuthorityCorp', 'AuthorityConf'):
                for row in self.promus.connection().select(
                    f"SELECT Bibsent_ID, NB_ID FROM {table} WHERE ISNULL(Bibsent_ID, '') <> ''",
                    normalize=True
                ):
                    self._bibbi_noraf_mapping[row['Bibsent_ID']] = row['NB_ID']

            log.info('Fant %d Bibbi-autoritetsposter i Promus med lenke til Noraf' % len(self._bibbi_noraf_mapping))
        return self._bibbi_noraf_mapping

    def find_harvest_files(self):
        """Filter files in the Noraf harvest directory that contain mappings to Bibbi"""
        filelist_cache = self.harvest_dir.joinpath('bibbi_list.json')

        if self.use_cache:
            with filelist_cache.open('r', encoding='utf-8') as fp:
                bibbi_files = json.load(fp)
            return bibbi_files

        all_files = []
        for dirpath, dirnames, filenames in os.walk(str(self.harvest_dir)):
            all_files += [os.path.join(dirpath, filename) for filename in filenames if filename.endswith('.xml')]
        bibbi_files = []
        for filename in tqdm(all_files, desc='Scanning OAI-PMH harvest...'):
            with open(filename, 'r', encoding='utf-8') as fp:
                if '>bibbi<' in fp.read():
                    bibbi_files.append(filename)
        with filelist_cache.open('w', encoding='utf-8') as fp:
            json.dump(bibbi_files, fp, indent=2)
        return bibbi_files

    def run(self):
        self.notifications = []
        reports_path = storage_path('reports')
        filenames = self.find_harvest_files()
        log.info('Fant %d Noraf-autoritetsposter med lenke til Bibbi', len(filenames))
        self.stats = {}
        for filename in tqdm(filenames, desc='Checking NORAF records'):
            with open(filename, 'rb') as fp:
                doc = XmlNode(
                    etree.parse(fp),
                    'info:lc/xmlns/marcxchange-v1',
                    {'oai': 'http://www.openarchives.org/OAI/2.0/'}
                )
                noraf_rec = NorafXmlRecord.parse(doc)
                if noraf_rec is None:
                    log.warning('Klarte ikke å tolke Noraf-posten: %s' % filename)
                else:
                    self.process_noraf_record(noraf_rec)
                    
            #if len(self.notifications) > 10:
            #    break

        self.dead_link_report.save_excel(
            reports_path.joinpath('noraf-bibbi-overgang - døde lenker.xlsx'), headers=[
                ReportHeader('Kildepost', 'ID', 20),
                ReportHeader('', '1XX $a', 30),
                ReportHeader('', '4XX', 40),
                ReportHeader('', '1XX $d', 20),
                ReportHeader('', 'Sist endret', 20),
                ReportHeader('Slettet Bibbi-post', 'ID', 20),
                ReportHeader('', 'Vurdering', 90),
                ReportHeader('', 'Forslag til erstatning', 20),
            ])

        self.one_to_many_report.save_excel(
            reports_path.joinpath('noraf-bibbi-overgang - en-til-flere-mappinger.xlsx'),
            headers=[
                ReportHeader('Noraf-post', 'ID', 16),
                ReportHeader('', '1XX $a', 30),
                ReportHeader('', '4XX', 50),
                ReportHeader('', '1XX $d', 15),
                ReportHeader('', 'Sist endret', 15),
                ReportHeader('Lenkede Bibbi-poster', 'Post 1', 15),
                ReportHeader('', '', 30),
                ReportHeader('', 'Post 2', 15),
                ReportHeader('', '', 30),
                ReportHeader('', 'Post 3', 15),
                ReportHeader('', '', 30),
            ])

        self.non_symmetric_report.save_excel(
            reports_path.joinpath('noraf-bibbi-overgang - ikke-symmetriske.xlsx'), headers=[
                ReportHeader('Noraf-post A', 'ID', 16),
                ReportHeader('', '1XX $a', 30),
                ReportHeader('', '4XX', 50),
                ReportHeader('', '1XX $d', 15),
                ReportHeader('', 'Sist endret', 15),
                ReportHeader('-> Bibbi-post B', 'ID', 15),
                ReportHeader('', '1XX $a', 30),
                ReportHeader('-> Noraf-post C != A', 'ID', 20),
                ReportHeader('', '1XX $a', 30),
            ])

        # Frekvens av antall Bibbi-lenker
        print('n || Antall Noraf-poster med n Bibbi-lenker')
        for k, v in self.stats.items():
            print('%s: %s' % (k, v))

        # send_email(self.notifications)

    def process_dead_link(
        self, noraf_rec: NorafPersonRecord | NorafCorporationRecord, bibbi_id: str
    ):
        """Behandle et tilfelle der en Noraf-post N1 lenker til en ikke-eksisterende Bibbi-post B1."""
        notification = Notification(
            record_id=noraf_rec.id,
            record_link=f'[{noraf_rec.id}: {str(noraf_rec)}](https://bsaut.toolforge.org/show/{noraf_rec.id})',
            issue=f'lenker til en slettet Bibbi-post: {bibbi_id}'
        )
        log.debug(f"{notification.record_link}: {notification.issue}")
        suggestion = ''

        # ------------------
        # Case 1) Hvis N1 også lenker til en annen Bibbi-post B2, fjerner vi N1-B1-lenken.

        bibbi_ids = noraf_rec.other_ids.get('bibbi', [])
        for bibbi_id2 in bibbi_ids:
            if bibbi_id2 != bibbi_id:
                promus_result = self.promus.authorities.first(Bibsent_ID=bibbi_id2)
                if promus_result is not None:
                    simple_rec = SimpleBibbiRecord.create(promus_result)
                    if simple_rec is not None:
                        notification.details = (
                            f"Noraf-posten lenker også til en annen Bibbi-post, som eksisterer: "
                            f"{simple_rec.md_link()}. Fjerner derfor lenken til {bibbi_id} fra Noraf-posten."
                        )
                        self.update_noraf_record(
                            noraf_rec,
                            remove_ids=[bibbi_id],
                            reason=notification.details,
                        )
                        self.notifications.append(notification)
                        return

        # ------------------
        # Case 2) Hvis det finnes en annen Bibbi-post B2 med samme navn og levetid, lenker vi N1 til den.

        def find_name_matches(noraf_rec: NorafPersonRecord | NorafCorporationRecord):
            table: PersonCollection | CorporationCollection
            if isinstance(noraf_rec, NorafPersonRecord):
                table = self.promus.authorities.person
                name_field = 'PersonName'
            elif isinstance(noraf_rec, NorafCorporationRecord):
                table = self.promus.authorities.corporation
                name_field = 'CorpName'
            else:
                raise ValueError("Unsupported noraf record type")

            for rec in table.list_records(
                QueryFilters(
                    [
                        QueryFilter("ReferenceNr IS NULL"),  # Ikke en henvisning
                        QueryFilter("Felles_ID = Bibsent_ID"),  # Ikke en biautoritet
                        QueryFilter(
                            "ISNULL(WebDeweyNr, '') = ''"
                        ),  # Ikke <entitet som emne>
                        QueryFilter(
                            f"{name_field} IN ("
                            + ",".join(
                                ["?" for _ in range(len(noraf_rec.alt_names) + 1)]
                            )
                            + ")",
                            [noraf_rec.name, *noraf_rec.alt_names],
                        ),
                    ]
                )
            ):
                yield SimpleBibbiRecord.create(rec)

        name_matches = list(find_name_matches(noraf_rec))
        name_date_matches = [
            match for match in name_matches
            if noraf_rec.dates is not None and match.dates is not None and noraf_rec.dates[:4] == match.dates[:4]
        ]

        if len(name_date_matches) == 1:
            best_match: SimpleBibbiRecord = name_date_matches[0]
            notification.details = f"Fant én autoritet i Bibbi med samme navn og fødselsår. Oppdaterer Noraf-posten " \
                                   f"til å peke til denne: {best_match.md_link()}."
            self.update_noraf_record(
                noraf_rec,
                remove_ids=[bibbi_id],
                add_ids=[best_match.uri],
                reason=notification.details,
            )
            self.notifications.append(notification)
            return

        # ------------------
        # Case 3) Vi legger mappingen til i rapporten for manuell sjekk,

        if len(name_matches) == 1:
            match = name_matches[0]
            if match.dates != noraf_rec.dates:
                notification.details = f'Fant én autoritet i Bibbi med samme navn, men ulik levetid: {match.md_link()}'
            else:
                notification.details = f'Fant én autoritet i Bibbi med samme navn, men mangler levetid: {match.md_link()}'

            if match.noraf_id:
                notification.details += f" - allerede lenket til en annen Noraf-post: " \
                                        f"[{match.noraf_id}](https://bsaut.toolforge.org/show/{match.noraf_id}) " \
                                        f". Det kan være en duplikat i Noraf, eller det kan være den første Noraf-posten bør avlenkes Bibbi"
            else:
                notification.suggestions.append(match)
                suggestion = match.id
            # f'For å lenke disse, kjør: poetry run noraf link --replace {noraf_rec.id} {match.id}'

        elif len(name_matches) > 1:
            notification.details = "Flere treff i Bibbi med samme navn"
            for match in name_matches:
                notification.suggestions.append(match)
        else:
            notification.details = 'Ingen treff i Bibbi ved eksakt søk på navn, prøv manuelt søk'
        self.notifications.append(notification)

        self.add_row(self.dead_link_report, noraf_rec, [
            bibbi_id,
            notification.details,
            suggestion,
        ])

    def process_non_symmetric_link(
        self, noraf_rec: NorafPersonRecord | NorafCorporationRecord, bibbi_id: str
    ):
        promus_result = self.promus.authorities.first(Bibsent_ID=bibbi_id)
        if promus_result is None:
            log.warning(f"Bibbi-posten {bibbi_id} ble ikke funnet")
            return
        bibbi_rec = SimpleBibbiRecord.create(promus_result)

        if bibbi_rec is None or bibbi_rec.noraf_id == int(noraf_rec.id):
            # This can happen if the ID cache is stale
            return

        log.warning(f"Symmetry error: {noraf_rec.id} <> {bibbi_rec.uri}")

        if bibbi_rec.noraf_id is None:
            # Noraf-posten N1 lenker til Bibbi-posten B1, men Bibbi-posten B1 lenker ikke til noe.
            # => Legger til lenke tilbake fra Bibbi-posten B1 til Noraf-posten N1
            log.info(f'Oppdaterer Promus: Legger til tilbakelenke fra Bibbi:{bibbi_rec.id} til Noraf:{noraf_rec.id}')
            noraf_json_rec = self.noraf.get(noraf_rec.id)
            self.promus.authorities.person.link_to_noraf(
                bibbi_rec.original,
                noraf_json_rec,
                reason='Det eksisterte en lenke fra Noraf-posten %s til Bibbi-posten %s' % (noraf_rec.id, bibbi_rec.id)
            )
            return

        try:
            target_noraf_rec = self.noraf.get(bibbi_rec.noraf_id)
        except NorafRecordNotFound:
            target_noraf_rec = None
        if target_noraf_rec is None or target_noraf_rec.deleted is True:
            # Noraf-posten N1 lenker til Bibbi-posten B1, men Bibbi-posten B1 lenker til en slettet Noraf-post.
            # => Legger til lenke tilbake fra Bibbi-posten B1 til Noraf-posten N1
            log.info(
                f"Bibbi:{bibbi_rec.uri} peker til en slettet post Noraf:{bibbi_rec.noraf_id}"
            )
            log.info(
                f"Oppdaterer Promus: Bibbi:{bibbi_rec.uri} fra Noraf:{bibbi_rec.noraf_id} til Noraf:{noraf_rec.id}"
            )
            noraf_json_rec = self.noraf.get(noraf_rec.id)
            self.promus.authorities.person.link_to_noraf(
                bibbi_rec.original,
                noraf_json_rec,
                reason=f"Det eksisterte en lenke fra Noraf-posten {noraf_rec.id} til Bibbi-posten {bibbi_rec.uri}. "
                f"Bibbi-posten lenket til en slettet Noraf-post {bibbi_rec.noraf_id}",
            )
            return

        # Noraf-posten N1 lenker til Bibbi-posten B1, men Bibbi-posten B1 lenker til en annen, ikke-slettet
        # Noraf-post N2.
        self.add_row(
            self.non_symmetric_report,
            noraf_rec,
            [
                "{BIBBI}" + bibbi_rec.uri,
                bibbi_rec.label,
                "{NORAF}" + target_noraf_rec.id,
                str(target_noraf_rec),
            ],
        )

    def remove_duplicate_links(self, noraf_rec: NorafRecord):
        noraf_json_rec = self.noraf.get(noraf_rec.id)
        bibbi_ids = list(noraf_json_rec.identifiers('bibbi'))
        distinct_bibbi_ids = list(set(bibbi_ids))
        if len(distinct_bibbi_ids) != len(bibbi_ids):
            log.warning('Noraf-posten %s inneholdt samme Bibbi-ID flere ganger: %s. Fjerner dubletter.',
                        noraf_rec.id, ', '.join(bibbi_ids))
            noraf_json_rec.set_identifiers('bibbi', distinct_bibbi_ids)
            self.noraf.put(noraf_json_rec, reason='Fjernet duplikate Bibbi-ID-er')
        return distinct_bibbi_ids

    def update_noraf_record(
        self,
        noraf_rec: NorafRecord,
        reason: str,
        add_ids: Optional[Sequence[str]] = None,
        remove_ids: Optional[Sequence[str]] = None,
    ):
        log.info(f"Planning to update NORAF record {noraf_rec.id}. Reason: {reason}")
        noraf_json_rec = self.noraf.get(noraf_rec.id)
        if remove_ids is not None:
            for value in remove_ids:
                noraf_json_rec.remove_identifier('bibbi', value)
        if add_ids is not None:
            for value in add_ids:
                noraf_json_rec.add_identifier('bibbi', value)
        self.noraf.put(noraf_json_rec, reason=reason)

    def process_noraf_record(
        self, noraf_rec: NorafPersonRecord | NorafCorporationRecord
    ):

        # Get Bibbi IDs
        bibbi_ids: list[str] = noraf_rec.other_ids.get("bibbi", [])

        # Look for and fix duplicates first
        distinct_bibbi_ids = list(set(bibbi_ids))
        if distinct_bibbi_ids != bibbi_ids:
            bibbi_ids = self.remove_duplicate_links(noraf_rec)

        # Update stats
        n_links = len(bibbi_ids)
        self.stats[str(n_links)] = self.stats.get(str(n_links), 0) + 1

        # If more than one link, add to the one-to-many report
        if n_links > 1:
            row = []
            for bibbi_id in bibbi_ids:
                full_rec = self.promus.authorities.first(Bibsent_ID=bibbi_id)
                if full_rec is not None:
                    row.append('{BIBBI}' + bibbi_id)
                    row.append(full_rec.label())
            self.add_row(self.one_to_many_report, noraf_rec, row)

        # Check if all links are valid
        for bibbi_id in bibbi_ids:

            if bibbi_id not in self.bibbi_noraf_mapping:
                # Case 1: Bibbi-posten har blitt slettet
                self.process_dead_link(noraf_rec, bibbi_id)

            elif self.bibbi_noraf_mapping[bibbi_id] != noraf_rec.id:
                # Case 2: Noraf-posten N1 lenker til Bibbi-posten B1, men Bibbi-posten B1
                #   a) lenker til en annen Noraf-post N2
                #   b) lenker ikke til noe
                self.process_non_symmetric_link(noraf_rec, bibbi_id)


def main():
    default_harvest_dir = storage_path("oai-harvest/noraf", create=False)

    parser = argparse.ArgumentParser(
        description=dedent(
            """
            Scriptet sjekker alle mappinger fra NORAF til BIBBI ved å traversere en OAI-PMH-dump av NORAF.
            Før scriptet kjøres bør dumpen oppdateres med kommandoen
            
                noraf harvest

            For hver post, henter det en oppdatert Bibbi-autoritetspost fra Promus og sjekker følgende:
                
              (1) Hvis NORAF-posten har flere mappinger til samme BIBBI-post, fjernes duplikater
              (2) Eksisterer Bibbi-posten fortsatt?

            Foreløpig gjør det ikke noe med nasjonalitet, datoer osv., men det kan nok legges til.

            Scriptet lager tre Excel-filer:
              (1) "noraf-bibbi-overgang - døde lenker.xlsx" gir en oversikt over alle NORAF-poster som peker til 
                  ikke-eksisterende BIBBI-poster                  
              (2) "noraf-bibbi-overgang - en-til-flere-mappinger.xlsx" gir en oversikt over alle NORAF-poster
                  som peker til mer enn én BIBBI-post
              (3) "noraf-bibbi-overgang - ikke-symmetriske.xlsx" gir en oversikt over alle NORAF-poster som peker 
                  til en BIBBI-post som ikke peker tilbake til samme NORAF-post.
            """
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument('harvest_dir',
                        nargs='?',
                        action=IsDir,
                        default=default_harvest_dir,
                        help='destination dir for the xml files')
    parser.add_argument('--use-cache',
                        action='store_true',
                        help='use cached version of file list')
    parser.add_argument('-v', '--verbose', action='store_true', help='More verbose output.')
    parser.add_argument('--dry-run', action='store_true', help='Dry run mode.')
    args = parser.parse_args()

    if args.verbose:
        log.setLevel(logging.DEBUG)
    else:
        log.setLevel(logging.INFO)

    noraf_key = os.getenv('BARE_KEY')
    if noraf_key is None:
        log.warning('No API key set')

    noraf = Noraf(noraf_key, read_only_mode=args.dry_run)
    promus = Promus(read_only_mode=args.dry_run)

    Processor(noraf, promus, args.harvest_dir, args.use_cache).run()

