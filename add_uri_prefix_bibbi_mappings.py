import os

from dotenv import load_dotenv
from dataclasses import dataclass
from seiso.services.noraf import Noraf, NorafRecordNotFound, NorafUpdateFailed
from seiso.services.promus import Promus
from seiso.common.logging import setup_logging
from seiso.constants import bibbi_uri_namespace


@dataclass
class Mapping:
    noraf_id: str
    bibbi_id: str
    bibbi_name: str


load_dotenv()
logger = setup_logging()
noraf = Noraf(os.getenv("BARE_KEY"), read_only_mode=False)
promus = Promus(read_only_mode=True)
logger.info("Connected to Promus")


def save_noraf_record(noraf_rec, reason):
    try:
        noraf.put(noraf_rec, reason=reason)
        return True
    except NorafUpdateFailed as error:
        logger.error(error.message)
        return False


def main() -> None:
    mappings: list[Mapping] = []
    for record in promus.authorities.person.all():
        if (
            record.NB_ID is not None
            and record.NB_ID != ""
            and record.MainPerson is True
        ):
            mappings.append(
                Mapping(
                    noraf_id=str(record.NB_ID),
                    bibbi_id=str(record.Bibsent_ID),
                    bibbi_name=str(record._DisplayValue),
                )
            )

    logger.info("Read %d Bibbi-Noraf mappings from Promus", len(mappings))

    n_added = 0
    n_changed = 0
    skipped = []

    for mapping in mappings:
        try:
            noraf_rec = noraf.get(mapping.noraf_id)
        except NorafRecordNotFound:
            logger.warning('[%s] Noraf record does not exist, mapping from Bibbi:%s "%s" must be checked manually',
                           mapping.noraf_id, mapping.bibbi_id, mapping.bibbi_name)
            continue
        if noraf_rec.deleted:
            logger.warning('[%s] Noraf record is marked as deleted, mapping from Bibbi:%s "%s" must be checked manually',
                           mapping.noraf_id, mapping.bibbi_id, mapping.bibbi_name)
            continue

        bibbi_id = mapping.bibbi_id
        bibbi_uri = bibbi_uri_namespace + mapping.bibbi_id
        noraf_bibbi_ids = noraf_rec.identifiers('bibbi')
        if len(noraf_bibbi_ids) == 0:
            noraf_rec.set_identifiers('bibbi', [bibbi_uri])
            if save_noraf_record(noraf_rec, 'Added Bibbi URI'):
                n_added += 1
        elif len(noraf_bibbi_ids) == 1 and noraf_bibbi_ids[0] == bibbi_id:
            noraf_rec.set_identifiers('bibbi', [bibbi_uri])
            if save_noraf_record(noraf_rec, 'Replaced Bibbi ID with URI'):
                n_changed += 1
        elif len(noraf_bibbi_ids) == 1 and noraf_bibbi_ids[0] == bibbi_uri:
            pass  # Ok, already updated
        elif len(noraf_bibbi_ids) == 1:
            logger.warning('[%s] Skipped because: not mapped to the expected Bibbi record (%s %s)',
                           noraf_rec.id, mapping.bibbi_id, mapping.bibbi_name)
            skipped.append(mapping.noraf_id)
        elif len(noraf_bibbi_ids) > 1:
            logger.warning('[%s] Skipped because: mapped to more than one Bibbi record', noraf_rec.id)
            skipped.append(mapping.noraf_id)

    logger.info('Added: %d Changed: %d Skipped: %d', n_added, n_changed, len(skipped))
    print(skipped[:10])


main()
