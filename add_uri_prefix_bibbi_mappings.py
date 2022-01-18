import os
from dotenv import load_dotenv
from seiso.services.noraf import Noraf
from seiso.services.promus import Promus

load_dotenv()

noraf = Noraf(os.getenv('BARE_KEY'), read_only_mode=False)
promus = Promus(read_only_mode=True)
print('Connected to Promus')
mappings = []
for record in promus.authorities.person.all():
    if record.NB_ID is not None and record.NB_ID != '' and record.MainPerson == True:
        mappings.append({'noraf': str(record.NB_ID), 'bibbi': str(record.Bibsent_ID)})


print(len(mappings))


bibbi_uri_prefix = 'https://id.bs.no/bibbi/'
added = 0
changed = 0
skipped = []

for mapping in mappings:
    noraf_rec = noraf.get(mapping['noraf'])
    bibbi_ids = noraf_rec.identifiers('bibbi')
    if len(bibbi_ids) == 0:
        noraf_rec.set_identifiers('bibbi', [bibbi_uri_prefix + mapping['bibbi']])
        noraf.put(noraf_rec, reason='Manuell lenking til Bibbi')
        print('Added Bibbi URI to Noraf record %s' % noraf_rec.id)
        added += 1
    elif len(bibbi_ids) == 1 and mapping['bibbi'] == bibbi_ids[0]:
        noraf_rec.set_identifiers('bibbi', [bibbi_uri_prefix + mapping['bibbi']])
        noraf.put(noraf_rec, reason='Manuell lenking til Bibbi')
        print('Replaced Bibbi ID with URI in Noraf record %s' % noraf_rec.id)
        changed += 1
    elif len(bibbi_ids) > 1:
        skipped.append(mapping['noraf'])

print('Added:', added, 'Changed:', changed, 'Mappings:', len(mappings), 'Skipped:', len(skipped), skipped[:5])

# bibbi_rec = SimpleBibbiRecord.create(promus.authorities.first(Bibsent_ID=args.bibbi_id))