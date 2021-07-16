import argparse
import time
from datetime import datetime
from textwrap import dedent

from dotenv import load_dotenv
import logging
from seiso.common.logging import setup_logging
from seiso.common.mailer import send_email, vocabulary_changes_notification
from seiso.services.promus import Promus
from seiso.services.promus.authorities import CurriculumRecord, Change
from dataclasses import dataclass
from typing import Optional, Generator
from SPARQLWrapper import SPARQLWrapper2


logger = setup_logging()


@dataclass
class Fagkode:
    uri: str
    kode: str
    tittel_nob: str
    status: str
    sist_endret_dato: datetime
    tittel_nno: Optional[str] = None
    tittel_eng: Optional[str] = None
    tittel_sme: Optional[str] = None
    gyldig_fra_dato: Optional[str] = None
    gyldig_til_dato: Optional[str] = None
    siste_undervisningssemester: Optional[str] = None
    erstattes_av: Optional[str] = None
    merkelapper: Optional[str] = None


def hent_fagkoder() -> Generator[Fagkode, None, None]:
    sparql = SPARQLWrapper2("https://sparql-data.udir.no/repositories/201906")
    sparql.setQuery("""
    PREFIX d: <http://psi.udir.no/kl06/>
    PREFIX u: <http://psi.udir.no/ontologi/kl06/>
    select
        ?uri 
        ?kode 
        ?tittel_nob
        ?tittel_nno
        ?tittel_eng
        ?tittel_sme
        ?status
        (REPLACE(SUBSTR(?sist_endret, 1, 23), "T", " ") AS ?sist_endret_dato)
        (SUBSTR(?gyldig_fra, 1, 10) AS ?gyldig_fra_dato)
        (SUBSTR(?gyldig_til, 1, 10) AS ?gyldig_til_dato)
        ?siste_undervisningssemester
        (GROUP_CONCAT(?ny_kode; separator=", ") AS ?erstattes_av)
        (GROUP_CONCAT(?merkelapp; separator=", ") AS ?merkelapper)
    where { 
        ?uri a u:fagkode ;
             u:kode ?kode ;
             u:tittel ?tittel_nob ;
             u:sist-endret ?sist_endret ;
             u:status ?status ;
        .
        
        FILTER(LANG(?tittel_nob) = "nob")
    
        OPTIONAL {
            ?uri u:merkelapper/u:tittel ?merkelapp
            FILTER(LANG(?merkelapp) = "nob")
        }
    
        OPTIONAL {
            ?uri u:tittel ?tittel_nno .
            FILTER(LANG(?tittel_nno) = "nno")
        }
    
        OPTIONAL {
            ?uri u:tittel ?tittel_eng .
            FILTER(LANG(?tittel_eng) = "eng")
        }
    
        OPTIONAL {
            ?uri u:tittel ?tittel_sme .
            FILTER(LANG(?tittel_sme) = "sme")
        }
    
        OPTIONAL { ?uri u:gyldig-fra ?gyldig_fra }
        OPTIONAL { ?uri u:gyldig-til ?gyldig_til  }
        OPTIONAL { ?uri u:erstattes-av/u:kode ?ny_kode . }
        OPTIONAL {
            ?uri u:naar-gis-det-undervisning-siste-semester/u:tittel ?siste_undervisningssemester
            FILTER(LANG(?siste_undervisningssemester) = "nob")
        }
    } 
    GROUP BY
        ?uri 
        ?kode 
        ?tittel_nob
        ?tittel_nno
        ?tittel_eng
        ?tittel_sme
        ?status
        ?sist_endret
        ?gyldig_fra
        ?gyldig_til
        ?siste_undervisningssemester
    """)

    for res in sparql.query().bindings:
        values = {
            key: None if (value is None or value.value == '') else value.value
            for key, value in res.items()
        }
        values['sist_endret_dato'] = datetime.fromisoformat(values['sist_endret_dato'])
        yield Fagkode(**values)


def main():
    """
    Scriptet oppdaterer tabellen AuthorityCurriculum med data fra Utdanningsdirektoratet sitt SPARQL-endepunkt
    """
    load_dotenv()
    parser = argparse.ArgumentParser()
    parser.add_argument('-v', '--verbose', action='store_true', help='More verbose output.')
    parser.add_argument('--dry-run', action='store_true', help='Dry run mode.')

    args = parser.parse_args()
    if args.verbose:
        logger.setLevel(logging.DEBUG)
        for handler in logger.handlers:
            if isinstance(handler, logging.StreamHandler):
                handler.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)

    if args.dry_run:
        print("Modus: Tørrkjøring, ingen endringer vil bli gjort")
    else:
        print("Modus: Vanlig kjøring, scriptet vil utføre endringer i Promus")
    time.sleep(3)
    promus = Promus(read_only_mode=args.dry_run)

    # --- Where the fun starts

    print("Henter data fra Udir")
    fagkoder = list(hent_fagkoder())
    print("Hentet %d fagkoder fra Udir" % len(fagkoder))

    print("Henter data fra Promus")
    promus_records = {rec.Code: rec for rec in promus.authorities.curriculum.all()}
    print("Hentet %d fagkoder fra Promus" % len(promus_records))

    promus_record_keys = set(promus_records.keys())
    fagkoder_keys = set([fagkode.kode for fagkode in fagkoder])
    ugyldige = promus_record_keys.difference(fagkoder_keys)
    if len(ugyldige):
        print("Ugyldige koder ble funnet i Promus. Rett opp disse først:", ugyldige)
        return

    def is_approved(fagkode: Fagkode):
        # Er fagkoden i bruk?
        if fagkode.status in ('utgaatt', 'ugyldig'):
            return False
        if fagkode.gyldig_til_dato is not None and fagkode.gyldig_til_dato < datetime.now().strftime('%Y-%m-%d'):
            return False
        return True

    new_concepts = []
    changed_concepts = []

    for fagkode in fagkoder:
        promus_record: CurriculumRecord = promus_records.get(fagkode.kode)
        data = {
            'Code': fagkode.kode,
            'URI': fagkode.uri,
            'Name': fagkode.tittel_nob,
            'Name_N': fagkode.tittel_nno,
            'Name_E': fagkode.tittel_eng,
            'Name_S': fagkode.tittel_sme,
            'Status': fagkode.status.replace('https://data.udir.no/kl06/v201906/status/status_', ''),
            'ValidFrom': fagkode.gyldig_fra_dato,
            'ValidUntil': fagkode.gyldig_til_dato,
            'ReplacedBy': fagkode.erstattes_av,
            'Notes': fagkode.merkelapper,
            'TeachedUntil': fagkode.siste_undervisningssemester,
            'LastChanged_udir': fagkode.sist_endret_dato,
            'Approved': is_approved(fagkode),
        }
        if promus_record:
            changes = promus_record.update(**data)
            for n, change in enumerate(changes):
                changed_concepts.append(format_changed_concept_row(fagkode, change, n == 0))
        else:
            promus.authorities.curriculum.insert(**data)
            new_concepts.append(format_new_concept_row(fagkode))

    if len(new_concepts) > 0 or len(changed_concepts) > 0:
        send_change_notification_email(new_concepts, changed_concepts)


def send_change_notification_email(new_concepts, changed_concepts):
    if len(new_concepts):
        new_concepts_str = dedent("""
        Følgende nye fagkoder ble importert:

        | Kode | Navn (nob) | URI
        |----|---|---|
        """) + '\n'.join(new_concepts)
    else:
        new_concepts_str = 'Ingen nye fagkoder ble importert'

    if len(changed_concepts):
        changed_concepts_str = dedent("""
        Følgende endringer ble importert for eksisterende fagkoder:

        | Kode | Navn (nob) | Endringer
        |----|---|---|
        """) + '\n'.join(changed_concepts)
    else:
        changed_concepts_str = '(ingen)'

    send_email(
        mail=vocabulary_changes_notification(),
        params={
            'vocabulary_name': 'Grep fagkoder',
            'new_concepts': new_concepts_str,
            'changed_concepts': changed_concepts_str,
        }
    )


def format_new_concept_row(fagkode: Fagkode):
    return '| %s | %s | <%s>' % (fagkode.kode, fagkode.tittel_nob, fagkode.uri)


def format_changed_concept_row(fagkode: Fagkode, change: Change, first_entry: bool):
    if change.old_value is None:
        change_str = '%s ble lagt til: «%s»' % (change.column, str(change.new_value))
    else:
        change_str = '%s ble endret fra «%s» til «%s»' % (change.column, str(change.old_value), str(change.new_value))

    if first_entry:
        return '| %s | %s | %s' % (fagkode.kode, fagkode.tittel_nob, change_str)
    else:
        return '| | | %s' % (change_str,)
