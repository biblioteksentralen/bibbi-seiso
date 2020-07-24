import os
from dotenv import load_dotenv
from tqdm import tqdm

from requests import Session
from cachecontrol import CacheControlAdapter
from cachecontrol.heuristics import ExpiresAfter

from seiso.services.noraf import Noraf
from seiso.services.promus import MsSql

session = Session()
session.mount('https://', CacheControlAdapter(
    heuristic=ExpiresAfter(days=1),
    max_retries=10,
))


class Promus:

    def __init__(self, db=None):
        self.db = db or MsSql(server=os.getenv('PROMUS_HOST'), database=os.getenv('PROMUS_DATABASE'),
                              user=os.getenv('PROMUS_USER'), password=os.getenv('PROMUS_PASSWORD'))

    def select(self, query, args=[]):
        with self.db.cursor() as cursor:
            cursor.execute(query, args)
            for row in cursor:
                yield row

    def update(self, query, args):
        with self.db.cursor() as cursor:
            print('"%s" @ [%s]' % (query, ', '.join(args)))
            # cursor.execute(query, args)
        # self.db.commit()

    def fetch_persons(self):
        for row in self.select(
            """
            SELECT PersonId, Bibsent_ID, NB_ID, PersonName
            FROM AuthorityPerson AS person
            WHERE ReferenceNr IS NULL
            AND Approved = '1'
            AND Felles_ID = Bibsent_ID
            AND Bibsent_ID IS NOT NULL
            AND NB_ID IS NOT NULL
            """
        ):
            yield {
                'local_id': str(row[0]),
                'bibbi_id': str(row[1]),
                'noraf_id': str(row[2]),
                'name': str(row[3]),
            }

    def fetch_all_bibsent_ids(self):
        tables = {}
        for table in ['Person', 'Conf', 'Corp', 'FreeGenre', 'Genre', 'Geographic', 'Music',  'Title', 'Topic',
                      'TopicMusic']:
            for row in self.select("SELECT PrimaryKeyField FROM AuthorityTable WHERE TableName = ?",
                                   ['Authority' + table]):
                tables[table] = row[0]

        ids = dict()
        for table, pkey in tables.items():
            for row in self.select("SELECT Bibsent_ID, %s FROM Authority%s WHERE Bibsent_ID IS NOT NULL" % (pkey, table)):
                bs_id = str(row[0])
                if bs_id in ids:
                    print('Bibsent_ID %s funnet i både %s:%s og %s:%s' % (bs_id, ids[bs_id][0], ids[bs_id][1], table, row[1]))
                ids[bs_id] = [table, row[1]]
        return ids


def search_noraf(noraf: Noraf, identifier: str):
    for noraf_rec in noraf.sru_search('bib.identifierAuthority="%s"' % identifier):
        bibbi_ids = noraf_rec.other_ids.get('bibbi', [])
        if len(bibbi_ids) > 1:
            with open('noraf_poly.txt', 'a+', buffering=1) as fp:
                fp.write('- Noraf-posten "%s" er koblet (024) til %d Bibbi-ID-er:\n' % (str(noraf_rec), len(bibbi_ids)))
                for pid in noraf_rec.other_ids.get('bibbi', []):
                    if pid in bibbi_persons_by_local_id:
                        fp.write('  - PersonID %s: %s\n' % (pid, bibbi_persons_by_local_id[pid]['name']))
                    if pid in bibbi_persons_by_bibbi_id:
                        fp.write('  - BibbiID %s: %s\n' % (pid, bibbi_persons_by_bibbi_id[pid]['name']))

        if identifier in noraf_rec.other_ids.get('bibbi', []):
            yield noraf_rec


def main():

    load_dotenv()

    promus = Promus()

    all_bibbi_ids = promus.fetch_all_bibsent_ids()
    print('Fetched %d Bibbi IDs' % len(all_bibbi_ids))

    bibbi_persons = list(promus.fetch_persons())
    bibbi_persons_by_local_id = {p['local_id']: p for p in bibbi_persons}
    bibbi_persons_by_bibbi_id = {p['bibbi_id']: p for p in bibbi_persons}


    print('Fetched %d persons from Promus' % len(bibbi_persons))

    noraf = Noraf()

    with open('feil.txt', 'w', buffering=1) as fp:
        for bibbi_person in tqdm(bibbi_persons):

            intro = '[[ Person_ID=%(local_id)s Bibsent_ID=%(bibbi_id)s NB_ID=%(noraf_id)s Navn=%(name)s ]] ' % bibbi_person

            noraf_rec = noraf.get_record(bibbi_person['noraf_id'])
            if noraf_rec is not None:
                bibbi_ids = noraf_rec.other_ids.get('bibbi', [])
                if bibbi_person['bibbi_id'] in bibbi_ids:
                    err = None  # Ok, valid record!
                elif len(bibbi_ids) == 0:
                    err = 'Noraf-posten "%s" mangler Bibbi-ID i 024.' % str(noraf_rec)
                elif bibbi_person['local_id'] in bibbi_ids:
                    err = 'Noraf-posten "%s" har PersonID i 024.' % str(noraf_rec)
                else:
                    err = 'Noraf-posten "%s" har følgende Bibbi-ID-er i 024: %s' % (str(noraf_rec), ' OG '.join(bibbi_ids))

            else:

                noraf_recs = list(search_noraf(noraf, bibbi_person['bibbi_id']))
                if len(noraf_recs) != 0:
                    err = 'Noraf-posten ble ikke funnet, men Bibbi-ID ble funnet i en annen Noraf-post: %s' % str(noraf_recs[0])

                else:

                    noraf_recs = list(search_noraf(noraf, bibbi_person['local_id']))
                    if len(noraf_recs) != 0:
                        err = 'Noraf-posten ble ikke funnet, men PersonID ble funnet i en annen Noraf-post: %s' % str(noraf_recs[0])

                    else:
                        err = 'Verken BIBSENT_ID, PersonId eller NB_ID ble funnet i Noraf'

            if err is not None:
                fp.write(intro + err + '\n')
