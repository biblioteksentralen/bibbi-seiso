# Script som går gjennom avgodkjente 651-strenger og finner erstatningsstrenger,
# f.eks. `651 Norden - Historie - 800-1050 - Essay` -> `651 Norden - Historie - 800-1050` + `655 Essay`
from typing import List, Dict, Optional
from dotenv import load_dotenv
from seiso.services.promus import Promus
from seiso.common.logging import setup_logging
import pandas as pd
from dataclasses import dataclass

load_dotenv()
logger = setup_logging()

promus = Promus(read_only_mode=True)


@dataclass
class Item:
    item_id: int
    varenr: str
    title: str


@dataclass
class ItemField:
    item_id: int
    sort_order: int
    field_code: str
    authority_id: int


def clean_label(label):
    # Remove qualifier and &
    return label.split(':')[0].split('&')[0].strip()


def list_items(field_code) -> Dict[str, List[Item]]:
    items_map = {}

    for row in promus.connection().select(
        """
        SELECT 
            item.Item_ID AS item_id, 
            item.Varenr AS varenr,
            item.Title AS title,
            field.Authority_ID AS authority_id
        FROM ItemField AS field
        JOIN Item as item ON field.Item_ID = item.Item_ID
        WHERE field.FieldCode = ?        
        """,
        [field_code]
    ):
        items_map[row['authority_id']] = items_map.get(row['authority_id'], []) + [Item(
            item_id=row['item_id'],
            varenr=row['varenr'],
            title=row['title']
        )]
    return items_map

    
def try_find_replacement(authority, all_authorities):

    def endswith(term):
        for k, v in all_authorities.items():
            if k.endswith(term):
                return v

    def try_match_part(parts):
        broader_label = ' - '.join(parts)
        broader_aut = all_authorities.get(broader_label)
        if not broader_aut or broader_aut.primary_key == authority.primary_key:
            broader_aut = endswith(broader_label)
        if broader_aut and broader_aut.primary_key != authority.primary_key:
            return {
                'old_key': str(authority.primary_key),
                'old_DisplayValue': authority._DisplayValue,
                'new_key': str(broader_aut.primary_key),
                'new_DisplayValue': broader_aut._DisplayValue,
            }

    parts = clean_label(authority._DisplayValue).split(' - ')
    # I første omgang prøver vi å fjerne opptil 1 ledd (0 og 1).
    # I neste sveip kan vi evt. prøve oss på flere.
    for n in range(2):
        if len(parts) == 0:
            break
        match = try_match_part(parts)
        if match:
            return match
        parts.pop()


def add_itemfield_entry_if_not_exists(item_id, field_id, field_code, ind1, ind2, authority_id, subfield_code) -> Optional[str]:
    def process_row(row):
        return ItemField(item_id=row["Item_ID"], sort_order=row["SortOrder"], field_code=row["FieldCode"], authority_id=row["Authority_ID"])

    rows = [process_row(row) for row in promus.connection().select(
        """
        SELECT Item_ID, SortOrder, FieldCode, Authority_ID FROM ItemField WHERE Item_ID = ? ORDER BY SortOrder
        """,
        [int(item_id)]
    )]
    match = next((row for row in rows if row.authority_id == authority_id and row.field_code == field_code), None)
    if not match:
        below = [row.sort_order for row in rows if row.sort_order is not None and int(row.field_code) < int(field_code)]
        sort_order = max(below) + 1
        return f"INSERT INTO ItemField (Item_ID, Indicator1, Indicator2, SortOrder, Field_ID, FieldCode, Authority_ID, SubFieldCode) VALUES ({item_id}, '{ind1}', '{ind2}', {sort_order}, {field_id}, '{field_code}', {authority_id}, '{subfield_code}')"


def update_authority(match, docs):
    # 1) For en forhåndsdefinert liste
    last_part = clean_label(match['old_DisplayValue']).split(' - ')[-1].lower()
    genre_labels = {
        674: 'Essay',
        360: 'Animasjonsfilmer',
        402: 'Dokumentarfilmer',
        373: 'Opplysningsfilmer',
        1218: 'Spillefilmer',
        415: 'Fjernsynsserier',
    }
    genre_map = {
        'essay': 674,
        'essays': 674,
        'animasjonsfilmer': 360, 
        'dokumentarfilmer': 402, 
        'opplysningsfilmer': 373, 
        'spillefilmer': 1218,
        'fjernsynsserier': 415,
    }
    genre_key = genre_map.get(last_part)

    print(f'-- "{match["old_DisplayValue"]}" -> "{match["new_DisplayValue"]}"')
    if genre_key is not None:
        for doc in docs:
            query = add_itemfield_entry_if_not_exists(
                item_id=doc.item_id,
                field_id='48',
                field_code='655',
                authority_id=genre_key,
                ind1=' ',
                ind2='2',
                subfield_code='a'
            )
            if query:
                print(query)
    print(f"UPDATE ItemField SET Authority_ID={match['new_key']} WHERE Authority_ID={match['old_key']} AND field.FieldCode = '651'")

    return {
        'old_key': match['old_key'],
        'old_DisplayValue': match['old_DisplayValue'],
        'new_key': match['new_key'],
        'new_DisplayValue': match['new_DisplayValue'],
        'new_genre_key': genre_key or '',
        'new_genre_DisplayValue': genre_labels[genre_key] if genre_key else '',

        'Dokumenter': len(docs),
        'Eksempel-vare': str(docs[0].varenr) if len(docs) > 0 else '',
    }


def main():

    def within_scope(x):
        # 1) Kun avgodkjente strenger
        if x.Approved:
            return False
        # 2) Kun strenger med underinndelinger
        if not x.UnderTopic:
            return False
        # 3) Kun strenger som slutter på noe som ser ut som en formterm
        undertopic = x.UnderTopic.lower()
        if undertopic.endswith('er'):
            return True
        for q in ['dikt', 'grafi', 'film', 'spill', 'essay', 'kart']:
            if q in undertopic:
                return True
        return False

    c = 0
    d = 0

    matches = []
    non_matches = []

    to_delete = 0

    items_map = list_items('651')
    all_geo = list(promus.authorities.geographic.list())
    approved_geo = {clean_label(x._DisplayValue): x for x in all_geo if x.Approved}

    for authority in all_geo:
        if not within_scope(authority):
            continue

        docs = [item for item in items_map.get(authority.primary_key, [])]
        if len(docs) == 0:
            # print(f"DELETE FROM AuthorityGeographic WHERE TopicID = {authority.primary_key}")
            to_delete += 1
            continue

        match = try_find_replacement(authority, approved_geo)
        if match:
            matches.append(
                update_authority(match, docs)
            )
            c += 1
        else:
            non_matches.append({
                'ID': str(authority.primary_key),
                '_DisplayValue': authority._DisplayValue,
                'Dokumenter': len(docs),
                'Eksempel-vare': str(docs[0].varenr) if len(docs) > 0 else ''
            })
            d += 1

    print(f"Matches: {c} Non-matches: {d} To delete: {to_delete}")

    matches_df = pd.DataFrame.from_records(matches)
    non_matches_df = pd.DataFrame.from_records(non_matches)

    matches_df.to_excel("bibbi_651_funnet_v6.xlsx")
    non_matches_df.to_excel("bibbi_651_ikke_funnet_v6.xlsx")


main()
