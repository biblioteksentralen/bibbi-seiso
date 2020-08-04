"""
Verify all links from Noraf to Bibbi. Note: We need a recent OAI-PMH dump to do this.
"""
import argparse
import logging
import os
import json
from pathlib import Path
from typing import Optional, Sequence

from dotenv import load_dotenv

from seiso.common.noraf_record import NorafXmlRecord
from seiso.common.interfaces import NorafPersonRecord, BibbiRecord, BibbiPerson, NorafRecord
from seiso.common.logging import setup_logging
from tqdm import tqdm
from lxml import etree
from seiso.common.xml import XmlNode
from seiso.console.helpers import Report, ReportHeader, storage_path
from seiso.services.noraf import Noraf
from seiso.services.promus import Promus, QueryFilter

log = setup_logging(level=logging.INFO)


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
        self.stats: dict = {}
        self._bibbi_noraf_mapping = {}

    @property
    def bibbi_noraf_mapping(self):
        """Lazy-load the Bibbi-to-Noraf concordance from Promus."""
        if len(self._bibbi_noraf_mapping) == 0:
            self._bibbi_noraf_mapping = {
                row['Bibsent_ID']: row['NB_ID']
                for row in self.promus.connection().select(
                    'SELECT Bibsent_ID, NB_ID FROM AuthorityPerson WHERE Bibsent_ID is not NULL',
                    normalize=True
                )
            }
            log.info('Leste %d Bibbi-personidentifikatorer fra Promus' % len(self._bibbi_noraf_mapping))
        return self._bibbi_noraf_mapping

    def find_files(self):
        """Filter files in the Noraf harvest directory that contain mappings to Bibbi"""
        filelist_cache = self.harvest_dir.joinpath('bibbi_list.json')

        if self.use_cache:
            with filelist_cache.open('r', encoding='utf-8') as fp:
                bibbi_files = json.load(fp)
            return bibbi_files

        log.info('Scanning harvest directory. This may take a few minutes')
        all_files = []
        for dirpath, dirnames, filenames in os.walk(str(self.harvest_dir)):
            all_files += [os.path.join(dirpath, filename) for filename in filenames if filename.endswith('.xml')]
        bibbi_files = []
        for filename in tqdm(all_files):
            with open(filename, 'r') as fp:
                if '>bibbi<' in fp.read():
                    bibbi_files.append(filename)
        with filelist_cache.open('w', encoding='utf-8') as fp:
            json.dump(bibbi_files, fp, indent=2)
        return bibbi_files

    def run(self):
        reports_path = storage_path('reports')
        filenames = self.find_files()
        log.info('Sjekker %d Noraf-poster med Bibbi-lenker', len(filenames))
        self.stats = {}
        for filename in tqdm(filenames):
            with self.harvest_dir.joinpath(filename).open('rb') as fp:
                doc = XmlNode(
                    etree.parse(fp),
                    'info:lc/xmlns/marcxchange-v1',
                    {'oai': 'http://www.openarchives.org/OAI/2.0/'}
                )
                noraf_rec = NorafXmlRecord.parse(doc)
                if noraf_rec is None:
                    log.warning('Inneholder ikke Noraf-post: %s' % filename)
                else:
                    self.process_noraf_record(noraf_rec)

        self.dead_link_report.save_excel(
            reports_path.joinpath('noraf-bibbi-overgang - døde lenker.xlsx'), headers=[
                ReportHeader('Noraf-post', 'ID', 20),
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

    def process_dead_link(self, noraf_rec: NorafPersonRecord, bibbi_id: str):
        """Behandle et tilfelle der en Noraf-post N1 lenker til en ikke-eksisterende Bibbi-post B1."""
        log.debug('[Noraf:%s %s] er lenket til ikke-eksisterende post [Bibbi:%s]', noraf_rec.id, str(noraf_rec), bibbi_id)
        conclusion = ''
        suggestion = ''

        # ------------------
        # Case 1) Hvis N1 også lenker til en annen Bibbi-post B2, fjerner vi N1-B1-lenken.

        bibbi_ids = noraf_rec.other_ids.get('bibbi', [])
        for bibbi_id2 in bibbi_ids:
            if bibbi_id2 != bibbi_id:
                bibbi_rec2 = self.promus.persons.get(bibbi_id2, with_items=False)
                if bibbi_rec2 is not None:
                    self.update_noraf_record(
                        noraf_rec,
                        remove_ids=[bibbi_id],
                        reason='Noraf-posten er også lenket til Bibbi-posten <%s>, som eksisterer. ' \
                        'Fjerner derfor lenken til %s' % (str(bibbi_rec2), bibbi_id)
                    )
                    return

        # ------------------
        # Case 2) Hvis det finnes en annen Bibbi-post B2 med samme navn og levetid, lenker vi N1 til den.

        matches = list(self.promus.persons.list([
            QueryFilter('person.PersonName = ?', noraf_rec.name)
        ]).values())
        date_matches = [match for match in matches if noraf_rec.dates is not None and match.dates == noraf_rec.dates]

        if len(date_matches) == 1:
            log.info('Ett treff i Bibbi med samme navn og samme levetid: <%s>' % (
                str(date_matches[0])
            ))
            self.update_noraf_record(
                noraf_rec,
                remove_ids=[bibbi_id],
                add_ids=[matches[0].id],
                reason='Erstatter død lenke. Fant ett treff i Bibbi med samme navn og samme levetid: %s' % str(matches[0])
            )
            return

        # ------------------
        # Case 3) Vi legger mappingen til i rapporten for manuell sjekk,

        if len(matches) == 1:
            if matches[0].dates != noraf_rec.dates:
                conclusion = 'Ett treff i Bibbi med samme navn (OBS: Ulik levetid!): <%s>' % (
                    str(matches[0])
                )
            else:
                conclusion = 'Ett treff i Bibbi med samme navn: <%s>' % (
                    str(matches[0])
                )
                suggestion = matches[0].id
                # suggestion = 'poetry run noraf link --replace %s %s' % (noraf_rec.id, matches[0].id)
        elif len(matches) > 1:
            conclusion = 'Flere treff i Bibbi'
            for match in matches:
                log.info('  - For å lenke til <%s>, kjør:' % (str(match),))
                log.info('     > poetry run noraf link --replace %s %s' % (noraf_rec.id, match.id))
        else:
            conclusion = 'Null treff i Bibbi ved eksakt søk på navn'

        self.dead_link_report.add_person_row(noraf_rec, [
            bibbi_id,
            conclusion,
            suggestion,
        ])

    def process_non_symmetric_link(self, noraf_rec: NorafPersonRecord, bibbi_id: str):

        bibbi_rec = self.promus.persons.get(bibbi_id, with_items=False)
        if bibbi_rec is None or bibbi_rec.noraf_id == noraf_rec.id:
            # This can happen if the ID cache is stale
            return

        log.warning('Symmetry error: %s <> %s', noraf_rec.id, bibbi_rec.id)

        if bibbi_rec.noraf_id is None:
            # Noraf-posten N1 lenker til Bibbi-posten B1, men Bibbi-posten B1 lenker ikke til noe.
            # => Legger til lenke tilbake fra Bibbi-posten B1 til Noraf-posten N1
            log.info('Oppdaterer Promus: Legger til manglende tilbakelenke fra Bibbi:%s til Noraf:%s',
                     bibbi_rec.id, noraf_rec.id)
            noraf_json_rec = self.noraf.get(noraf_rec.id)
            self.promus.persons.link_to_noraf(bibbi_rec, noraf_json_rec, False,
                                              reason='Det eksisterte en lenke fra Noraf-posten %s til Bibbi-posten %s'
                                                     % (noraf_rec.id, bibbi_rec.id)
                                              )
            return

        target_noraf_rec = self.noraf.get(bibbi_rec.noraf_id)
        if target_noraf_rec is None or target_noraf_rec.deleted is True:
            # Noraf-posten N1 lenker til Bibbi-posten B1, men Bibbi-posten B1 lenker til en slettet Noraf-post.
            # => Legger til lenke tilbake fra Bibbi-posten B1 til Noraf-posten N1
            log.info('Bibbi:%s peker til en slettet post Noraf:%s', bibbi_rec.id, bibbi_rec.noraf_id)
            log.info('Oppdaterer Promus: Bibbi:%s fra Noraf:%s til Noraf:%s',
                     bibbi_rec.id, bibbi_rec.noraf_id, noraf_rec.id)
            noraf_json_rec = self.noraf.get(noraf_rec.id)
            self.promus.persons.link_to_noraf(bibbi_rec, noraf_json_rec, False,
                                              reason='Det eksisterte en lenke fra Noraf-posten %s til Bibbi-posten %s. '
                                                     'Bibbi-posten lenket til en slettet Noraf-post %s'
                                                     % (noraf_rec.id, bibbi_rec.id, bibbi_rec.noraf_id)
                                              )
            return

        # Noraf-posten N1 lenker til Bibbi-posten B1, men Bibbi-posten B1 lenker til en annen, ikke-slettet
        # Noraf-post N2.
        self.non_symmetric_report.add_person_row(noraf_rec, [
            '{BIBBI}' + bibbi_rec.id,
            str(bibbi_rec),
            '{NORAF}' + target_noraf_rec.id,
            str(target_noraf_rec),
        ])

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

    def update_noraf_record(self, noraf_rec: NorafRecord, reason: str,
                            add_ids: Sequence[str] = None, remove_ids: Sequence[str] = None):
        noraf_json_rec = self.noraf.get(noraf_rec.id)
        if remove_ids is not None:
            for value in remove_ids:
                noraf_json_rec.remove_identifier('bibbi', value)
        if add_ids is not None:
            for value in add_ids:
                noraf_json_rec.add_identifier('bibbi', value)
        self.noraf.put(noraf_json_rec, reason=reason)

    def process_noraf_record(self, noraf_rec, ):

        # Get Bibbi IDs
        bibbi_ids = noraf_rec.other_ids.get('bibbi', [])

        # Look for and fix duplicates first
        distinct_bibbi_ids = list(set(bibbi_ids))
        if distinct_bibbi_ids != bibbi_ids:
            bibbi_ids = self.remove_duplicate_links(noraf_rec)

        # Update stats
        n_links = len(bibbi_ids)
        self.stats[n_links] = self.stats.get(n_links, 0) + 1

        # If more than one link, add to the one-to-many report
        if n_links > 1:
            full_recs = [
                self.promus.persons.get(bibbi_id, with_items=False)
                for bibbi_id in bibbi_ids
            ]
            row = []
            for full_rec in full_recs:
                if full_rec is not None:
                    row.append('{BIBBI}' + full_rec.id)
                    row.append(str(full_rec))
            self.one_to_many_report.add_person_row(noraf_rec, row)

        # Check if all links are valid
        bibbi_ids_remove = []
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
    load_dotenv()
    default_harvest_dir = storage_path('noraf-harvest', create=False)

    parser = argparse.ArgumentParser(description='Verify all Noraf-Bibbi mappings')
    parser.add_argument('harvest_dir',
                        nargs='?',
                        action=IsDir,
                        default=default_harvest_dir,
                        help='destination dir for the xml files')
    parser.add_argument('--use-cache',
                        action='store_true',
                        help='use cached version of file list')
    parser.add_argument('-v', '--verbose', action='store_true', help='More verbose output.')
    args = parser.parse_args()

    if args.verbose:
        log.setLevel(logging.DEBUG)
    else:
        log.setLevel(logging.INFO)

    noraf = Noraf(os.getenv('BARE_KEY'))
    promus = Promus()

    Processor(noraf, promus, args.harvest_dir, args.use_cache).run()

