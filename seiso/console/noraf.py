import argparse
import json
import logging
import os
import sys
from pathlib import Path

import questionary
from dotenv import load_dotenv
from seiso.console.verify_noraf_bibbi_mappings import SimpleBibbiRecord

from seiso.common.noraf_record import NorafJsonRecord
from seiso.common.logging import setup_logging
from seiso.console.helpers import storage_path
from seiso.services.noraf import Noraf
from seiso.services.promus import Promus

logger = setup_logging()


class WritableDir(argparse.Action):

    def __call__(self, parser, namespace, values, option_string=None):
        prospective_dir = Path(values)
        if not prospective_dir.exists():
            prospective_dir.mkdir(parents=True)
        if not prospective_dir.is_dir():
            raise argparse.ArgumentTypeError('{}:{} is not a valid path'.format(self.dest, prospective_dir))
        setattr(namespace, self.dest, prospective_dir)


def put_action(noraf: Noraf, args: argparse.Namespace) -> None:
    record = NorafJsonRecord(args.source_file.read())
    noraf.put(record, reason='Manuell oppdatering')
    logger.info('Updated record. https://bsaut.toolforge.org/show/%s', record.id)


def post_action(noraf: Noraf, args: argparse.Namespace) -> None:
    record = NorafJsonRecord(args.source_file.read())
    record = noraf.post(record)
    logger.info('https://bsaut.toolforge.org/show/%s', record.id)


def delete_action(noraf: Noraf, args: argparse.Namespace) -> None:
    record = noraf.get(args.record_id)
    record = noraf.delete(record)
    logger.info('https://bsaut.toolforge.org/show/%s', record.id)


def get_action(noraf: Noraf, args: argparse.Namespace) -> None:
    record = noraf.get(args.record_id)
    json.dump(record.as_dict(), args.dest_file, indent=2)


def link_action(noraf: Noraf, args: argparse.Namespace) -> None:
    promus = Promus(read_only_mode=args.dry_run)
    logger.debug('Connected to Promus')
    noraf_rec = noraf.get(args.noraf_id)
    bibbi_rec = SimpleBibbiRecord.create(promus.authorities.first(Bibsent_ID=args.bibbi_id))

    logger.info('[Bibbi] %s %s (%s)', bibbi_rec.name, bibbi_rec.dates or '', bibbi_rec.id)
    logger.info('[Noraf] %s %s (%s)', noraf_rec.name, noraf_rec.dates or '', noraf_rec.id)

    if bibbi_rec.noraf_id == noraf_rec.id:
        logger.info('Bibbi record alredy linked to Noraf record')
    else:
        promus.authorities.person.link_to_noraf(bibbi_rec.original, noraf_rec, 'Manuell lenking')

    bibbi_ids = noraf_rec.identifiers('bibbi')
    if len(bibbi_ids) == 0:
        noraf_rec.set_identifiers('bibbi', [bibbi_rec.id])
        noraf.put(noraf_rec, reason='Manuell lenking til Bibbi')
        logger.info('Added Bibbi identifier to Noraf record %s', noraf_rec.id)

    elif bibbi_rec.id not in bibbi_ids:
        logger.warning('Noraf-posten er allerede lenket til en eller flere andre Bibbi-poster: %s',
                       ', '.join(bibbi_ids))
        if args.replace:
            logger.warning('Fjerner eksisterende lenker')
            noraf_rec.set_identifiers('bibbi', [bibbi_rec.id])
            noraf.put(noraf_rec, reason='Manuell lenking til Bibbi (erstattet eksisterende)')
            logger.info('Replaced Bibbi identifiers on Noraf record %s', noraf_rec.id)

        elif questionary.confirm('Vil du legge til en lenke til Bibbi-posten %s også?' % bibbi_rec.id).ask():
            noraf_rec.add_identifier('bibbi', bibbi_rec.id)
            noraf.put(noraf_rec, reason='Manuell lenking til Bibbi (erstattet eksisterende)')
            logger.info('Added Bibbi identifier to Noraf record %s', noraf_rec.id)

    else:
        logger.info('Noraf-posten er allerede lenket til Bibbi-posten')


def main():
    """
    Scriptet oppdaterer personposter i Bibbi (via SQL) og Noraf (via REST-API) basert på inputt
    fra Excel-filen noraf_forslag_fra_isbn_tittel_match.xlsx
    """
    load_dotenv()

    parser = argparse.ArgumentParser(description='Operations on Noraf records')
    parser.add_argument('-v', '--verbose', action='store_true', help='More verbose output.')
    parser.add_argument('--dry-run', action='store_true', help='Dry run mode.')

    subparsers = parser.add_subparsers(dest='cmd')
    subparsers.required = True

    parser_get = subparsers.add_parser('get', help='Get a single record from Noraf')
    parser_get.add_argument('record_id',
                            help='Noraf record ID')
    parser_get.add_argument('dest_file',
                            nargs='?',
                            type=argparse.FileType('w', encoding='utf-8'),
                            default=sys.stdout,
                            help='output file - default is stdout')
    parser_get.set_defaults(func=get_action)

    parser_put = subparsers.add_parser('put', help='Put a single record to Noraf')
    parser_put.add_argument('source_file',
                            nargs='?',
                            default=sys.stdin,
                            type=argparse.FileType('r', encoding='utf-8'),
                            help='input file - default is stdin')
    parser_put.set_defaults(func=put_action)

    parser_post = subparsers.add_parser('post', help='Post a single record to Noraf')
    parser_post.add_argument('source_file',
                             nargs='?',
                             default=sys.stdin,
                             type=argparse.FileType('r', encoding='utf-8'),
                             help='input file - default is stdin')
    parser_post.set_defaults(func=post_action)

    parser_delete = subparsers.add_parser('delete', help='Delete a single record from Noraf')
    parser_delete.add_argument('record_id',
                               help='Noraf record ID')
    parser_delete.set_defaults(func=delete_action)

    parser_link = subparsers.add_parser('link', help='Validate and create/update a Bibbi-Noraf-link')
    parser_link.add_argument('--replace', help='Remove existing Bibbi IDs first', action='store_true')
    parser_link.add_argument('bibbi_id', help='Bibbi record ID')
    parser_link.add_argument('noraf_id', help='Noraf record ID')
    parser_link.set_defaults(func=link_action)

    args = parser.parse_args()

    if args.verbose:
        logger.setLevel(logging.DEBUG)
        for handler in logger.handlers:
            if isinstance(handler, logging.StreamHandler):
                handler.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)

    noraf = Noraf(os.getenv('BARE_KEY'), read_only_mode=args.dry_run)
    args.func(noraf, args)
