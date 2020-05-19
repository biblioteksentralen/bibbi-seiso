import pydash
import pyodbc
import os
import re
import sys
from typing import Generator
from typing import TypedDict
from dotenv import load_dotenv
from openpyxl import Workbook
from openpyxl.styles import PatternFill, Border, Side, Alignment, Protection, Font
import unidecode
from json import JSONDecodeError
from lxml import etree
from tqdm import tqdm

from requests import Session
from requests.adapters import HTTPAdapter
from cachecontrol import CacheControlAdapter
from cachecontrol.heuristics import ExpiresAfter
from cachecontrol.caches import FileCache


session = Session()
session.mount('https://', CacheControlAdapter(
    heuristic=ExpiresAfter(days=1),
    max_retries=10,
))


class MsSql:

    def __init__(self, **db_settings):
        if os.name == 'posix':
            conn_string = ';'.join([
                'DRIVER={FreeTDS}',
                'Server=%(server)s',
                'Database=%(database)s',
                'UID=BIBSENT\\dmheg',
                'PWD=%(password)s',
                'TDS_Version=8.0',
                'Port=1433',
            ]) % db_settings
        else:
            conn_string = ';'.join([
                'Driver={SQL Server}',
                'Server=%(server)s',
                'Database=%(database)s',
                'Trusted_Connection=yes',
            ]) % db_settings
        self.conn: pyodbc.Connection = pyodbc.connect(conn_string)

    def cursor(self) -> pyodbc.Cursor:
        return self.conn.cursor()

    def commit(self):
        self.conn.commit()


class Promus:

    def __init__(self, db=None):
        self.db = db or MsSql(server=os.getenv('DB_SERVER'), database=os.getenv('DB_DB'),
                              user=os.getenv('DB_USER'), password=os.getenv('DB_PASSWORD'))

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
            WHERE ReferenceNr IS NULL AND Approved = '1' AND Felles_ID = Bibsent_ID AND Bibsent_ID IS NOT NULL AND NB_ID IS NOT NULL
            """
        ):
            yield {
                'local_id': str(row[0]),
                'bibbi_id': str(row[1]),
                'bare_id': str(row[2]),
                'name': str(row[3]),
            }

    def fetch_all_bibsent_ids(self):
        tables = {}
        for table in ['Person','Conf', 'Corp', 'FreeGenre', 'Genre', 'Geographic', 'Music',  'Title', 'Topic', 'TopicMusic']:
            for row in self.select("SELECT PrimaryKeyField FROM AuthorityTable WHERE TableName = ?", ['Authority' + table]):
                tables[table] = row[0]

        print(tables)


        ids = dict()
        for table, pkey in tables.items():
            for row in self.select("SELECT Bibsent_ID, %s FROM Authority%s WHERE Bibsent_ID IS NOT NULL" % (pkey, table)):
                bs_id = str(row[0])
                if bs_id in ids:
                    print('Bibsent_ID %s funnet i både %s:%s og %s:%s' % (bs_id, ids[bs_id][0], ids[bs_id][1], table, row[1]))
                ids[bs_id] = [table, row[1]]
        return ids


class BareRecord:

    def __init__(self, record_el):
        self.id = record_el.xpath('marc:controlfield[@tag="001"]/text()', namespaces={'marc': 'info:lc/xmlns/marcxchange-v1'})[0]

        self.name = ''
        for node in record_el.xpath('marc:datafield[@tag="100"]/marc:subfield[@code="a"]', namespaces={'marc': 'info:lc/xmlns/marcxchange-v1'}):
            self.name = node.text


        # TODO: Sjekke 400-felt!!

        self.dates = ''
        for node in record_el.xpath('marc:datafield[@tag="100"]/marc:subfield[@code="d"]', namespaces={'marc': 'info:lc/xmlns/marcxchange-v1'}):
            self.dates = node.text

        self.bibbi_ids = []
        for node in record_el.xpath('marc:datafield[@tag="024"][./marc:subfield[@code="2"]/text() = "bibbi"]/marc:subfield[@code="a"]', namespaces={'marc': 'info:lc/xmlns/marcxchange-v1'}):
            self.bibbi_ids.append(node.text)

    def as_dict(self):
        return {
            'id': self.id,
            'name': self.name,
            'dates': self.dates,
        }
    
    def __str__(self):
        out = '%s %s' % (self.id, self.name)        
        if self.dates != '':
            out +=  ' %s' % self.dates
        return out


def search_bare(identifier: str):
    response = session.get('https://authority.bibsys.no/authority/rest/sru', params={
        'operation': 'searchRetrieve',
        'query': 'bib.identifierAuthority="%s"' % identifier,
        'recordSchema': 'marcxchange',
        'version': '1.2',
    }).text

    root = etree.fromstring(response.encode('utf-8'))

    for rec in root.xpath('//marc:record', namespaces={'marc': 'info:lc/xmlns/marcxchange-v1'}):
        bare_rec = BareRecord(rec)

        if len(bare_rec.bibbi_ids) > 1:
            with open('bare_poly.txt', 'a+', buffering=1) as fp:
                fp.write('- BARE-posten "%s" er koblet (024) til %d Bibbi-ID-er:\n' % (str(bare_rec), len(bare_rec.bibbi_ids)))
                for pid in bare_rec.bibbi_ids:
                    if pid in bibbi_persons_by_local_id:
                        fp.write('  - PersonID %s: %s\n' % (pid, bibbi_persons_by_local_id[pid]['name']))
                    if pid in bibbi_persons_by_bibbi_id:
                        fp.write('  - BibbiID %s: %s\n' % (pid, bibbi_persons_by_bibbi_id[pid]['name']))

        if identifier in bare_rec.bibbi_ids:
            yield bare_rec


def get_bare(identifier: str):
    response = session.get('https://authority.bibsys.no/authority/rest/sru', params={
        'operation': 'searchRetrieve',
        'query': 'rec.identifier="%s"' % identifier,
        'recordSchema': 'marcxchange',
        'version': '1.2',
    }).text

    root = etree.fromstring(response.encode('utf-8'))

    for rec in root.xpath('//marc:record', namespaces={'marc': 'info:lc/xmlns/marcxchange-v1'}):
        bare_rec = BareRecord(rec)
        if bare_rec.id == identifier:
            return bare_rec


load_dotenv()

promus = Promus()

all_bibbi_ids = promus.fetch_all_bibsent_ids()
print('Fetched %d Bibbi IDs' % len(all_bibbi_ids))

bibbi_persons = list(promus.fetch_persons())
bibbi_persons_by_local_id = {p['local_id']: p for p in bibbi_persons}
bibbi_persons_by_bibbi_id = {p['bibbi_id']: p for p in bibbi_persons}


print('Fetched %d persons from Promus' % len(bibbi_persons))

with open('feil.txt', 'w', buffering=1) as fp:
    for bibbi_person in tqdm(bibbi_persons):

        intro = '[[ Person_ID=%(local_id)s Bibsent_ID=%(bibbi_id)s NB_ID=%(bare_id)s Navn=%(name)s ]] ' % bibbi_person

        bare_rec = get_bare(bibbi_person['bare_id'])
        if bare_rec is not None:
            if bibbi_person['bibbi_id'] in bare_rec.bibbi_ids:
                err = None  # Ok, valid record!
            elif len(bare_rec.bibbi_ids) == 0:
                err = 'BARE-posten "%s" mangler Bibbi-ID i 024.' % str(bare_rec)
            elif bibbi_person['local_id'] in bare_rec.bibbi_ids:
                err = 'BARE-posten "%s" har PersonID i 024.' % str(bare_rec)
            else:
                err = 'BARE-posten "%s" har følgende Bibbi-ID-er i 024: %s' % (str(bare_rec), ' OG '.join(bare_rec.bibbi_ids))

        else:

            bare_recs = list(search_bare(bibbi_person['bibbi_id']))
            if len(bare_recs) != 0:
                err = 'BARE-posten ble ikke funnet, men Bibbi-ID ble funnet i en annen BARE-post: %s' % str(bare_recs[0])

            else:

                bare_recs = list(search_bare(bibbi_person['local_id']))
                if len(bare_recs) != 0:
                    err = 'BARE-posten ble ikke funnet, men PersonID ble funnet i en annen BARE-post: %s' % str(bare_recs[0])

                else:
                    err = 'Verken BIBSENT_ID, PersonId eller NB_ID ble funnet i BARE'
            
        if err is not None:
            fp.write(intro + err + '\n')
