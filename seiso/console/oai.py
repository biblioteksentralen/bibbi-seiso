import argparse
import logging
import os
from pathlib import Path

from dotenv import load_dotenv
from seiso.services.oai import OaiPmh, OaiPmhSettings

from seiso.common.logging import setup_logging
from seiso.console.helpers import storage_path
from seiso.services.noraf import Noraf

logger = setup_logging()


class WritableDir(argparse.Action):

    def __call__(self, parser, namespace, values, option_string=None):
        prospective_dir = Path(values)
        if not prospective_dir.exists():
            prospective_dir.mkdir(parents=True)
        if not prospective_dir.is_dir():
            raise argparse.ArgumentTypeError('{}:{} is not a valid path'.format(self.dest, prospective_dir))
        setattr(namespace, self.dest, prospective_dir)


def harvest_action(noraf: Noraf, args: argparse.Namespace) -> None:
    noraf.oai_harvest(args.destination_dir)


def main():
    """
    Scriptet oppdaterer personposter i Bibbi (via SQL) og Noraf (via REST-API) basert på inputt
    fra Excel-filen noraf_forslag_fra_isbn_tittel_match.xlsx
    """
    load_dotenv()

    default_destination_dir = storage_path('oai-harvest', create=False)

    parser = argparse.ArgumentParser(description='OAI-PMH harvests')
    parser.add_argument('-v', '--verbose', action='store_true', help='More verbose output.')
    parser.add_argument('--dry-run', action='store_true', help='Dry run mode.')

    subparsers = parser.add_subparsers(dest='cmd')
    subparsers.required = True

    parser_harvest = subparsers.add_parser('harvest', help='Harvest records')
    parser_harvest.add_argument(
        'source',
        help='alma, noraf or bibbi'
    )
    parser_harvest.add_argument(
        'destination_dir',
        nargs='?',
        action=WritableDir,
        default=default_destination_dir,
        help='destination dir for the xml files'
    )

    parser_extract = subparsers.add_parser('extract', help='Extract data')
    parser_extract.add_argument(
        'dir',
        help='Source dir for the xml files'
    )

    args = parser.parse_args()

    if args.verbose:
        logger.setLevel(logging.DEBUG)
        for handler in logger.handlers:
            if isinstance(handler, logging.StreamHandler):
                handler.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)

    if not args.destination_dir:
        print("ERR: Destination dir not set")

    storage_dir: Path = args.destination_dir.joinpath(args.source)
    storage_dir.mkdir(exist_ok=True)

    if args.source == 'bibbi':
        if not os.getenv('BIBBI_OAI_USER') or not os.getenv('BIBBI_OAI_PASSWORD'):
            raise Exception('BIBBI_OAI_USER and/or BIBBI_OAI_PASSWORD not configured')
        settings = OaiPmhSettings(
            endpoint='https://oai.aja.bs.no/bibbi',
            metadata_prefix="marc21",
            metadata_schema="info:lc/xmlns/marcxchange-v1",
            storage_dir=storage_dir,
            request_args={'auth': (
                os.getenv('BIBBI_OAI_USER').encode('utf-8'),
                os.getenv('BIBBI_OAI_PASSWORD').encode('utf-8')
            )},
        )

    elif args.source == 'alma':
        settings = OaiPmhSettings(
            endpoint='http://eu01.alma.exlibrisgroup.com/view/oai/47BIBSYS_NETWORK/request',
            metadata_prefix="marc21",
            metadata_schema="http://www.loc.gov/MARC21/slim",
            oai_set="oai_komplett",
            storage_dir=storage_dir,
        )
    elif args.source == 'noraf':
        settings = OaiPmhSettings(
            endpoint='http://eu01.alma.exlibrisgroup.com/view/oai/47BIBSYS_NETWORK/request',
            metadata_prefix="marcxchange",
            metadata_schema="info:lc/xmlns/marcxchange-v1",
            oai_set="bibsys_authorities",
            storage_dir=storage_dir,
        )
    else:
        raise Exception('Unknown source')

    provider = OaiPmh(settings)
    provider.harvest()

