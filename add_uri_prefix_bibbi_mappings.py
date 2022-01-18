import os
from dotenv import load_dotenv
from seiso.services.noraf import Noraf
from seiso.services.promus import Promus
from seiso.common.logging import setup_logging

load_dotenv()
logger = setup_logging()

noraf = Noraf(os.getenv('BARE_KEY'), read_only_mode=False)
promus = Promus(read_only_mode=True)
logger.info('Connected to Promus')
mappings = []
for record in promus.authorities.person.all():
    if record.NB_ID is not None and record.NB_ID != '' and record.MainPerson == True:
        mappings.append({'noraf': str(record.NB_ID), 'bibbi': str(record.Bibsent_ID)})

logger.info('Read %d Bibbi-Noraf mappings from Promus', len(mappings))

bibbi_uri_prefix = 'https://id.bs.no/bibbi/'
n_added = 0
n_changed = 0
skipped = []

for mapping in mappings:
    noraf_rec = noraf.get(mapping['noraf'])
    bibbi_ids = noraf_rec.identifiers('bibbi')
    if len(bibbi_ids) == 0:
        noraf_rec.set_identifiers('bibbi', [bibbi_uri_prefix + mapping['bibbi']])
        noraf.put(noraf_rec, reason='Manuell lenking til Bibbi')
        logger.info('Added Bibbi URI to Noraf record %s' % noraf_rec.id)
        n_added += 1
    elif len(bibbi_ids) == 1 and mapping['bibbi'] == bibbi_ids[0]:
        noraf_rec.set_identifiers('bibbi', [bibbi_uri_prefix + mapping['bibbi']])
        noraf.put(noraf_rec, reason='Manuell lenking til Bibbi')
        logger.info('Replaced Bibbi ID with URI in Noraf record %s' % noraf_rec.id)
        n_changed += 1
    elif len(bibbi_ids) > 1:
        skipped.append(mapping['noraf'])

logger.info('Added:', n_added, 'Changed:', n_changed, 'Mappings:', len(mappings), 'Skipped:', len(skipped), skipped[:5])
