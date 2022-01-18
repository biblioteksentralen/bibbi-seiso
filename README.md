# bibbi-seiso 🧰

Python-basert verktøykasse for å jobbe med Bibbi via Promus og Noraf via Bibsys-API-et.

## Oppsett

Du trenger

* [Poetry](python-poetry.org/)
* [ODBC Driver for SQL Server](https://docs.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server?view=sql-server-ver15)

I `bibbi-seiso`-mappa, kjør:

    poetry install

Kommandoen henter inn avhengighetene som trengs, men kan feile hvis du mangler grunnleggende kompilatorer og programmeringsbibliotek på maskinen.
På Windows trengs f.eks. "C++ build tools". For oppdaterte instruksjoner om akkurat hvilke pakker som trengs, se https://wiki.python.org/moin/WindowsCompilers

### Konfigurasjon

Før du kan bruke bibbi-seiso, må du opprette en `.env`-fil fra `.env.dist`

    cp .env.dist .env
    
og legge inn påloggingsinformasjon for Promus og en API-nøkkel for Bibsys-API-et i denne.

## Utvikling

Verktøykassen har ikke full testdekning, men kommer med automatiske tester for spesielt viktig funksjonalitet.
Disse kan kjøres med pytest:

    poetry run pytest

Merk at noen av testene henter data fra Promus og Bibsys-API-et.
De kan feile hvis testpostene har blitt endret.
For å ikke kjøre disse:

    poetry run pytest -m "not integration"

## Innhold i verktøykassen

### `match_persons`

Script for å matche personer i Bibbi mot personer i Noraf basert på felles litteraturbelegg:

    poetry run match_persons -h

Dette produserer rapporten `bibbi-persons-match-alma.xlsx`.

### `update_persons`

Script for å oppdatere Promus basert på rader i `bibbi-persons-match-alma.xlsx` markert med "ok":

    poetry run update_persons -h

### `noraf`: Manuelle operasjoner på Noraf-poster

Noen ganger er det praktisk å manuelt kunne gjøre oppdatering på Noraf-poster. Et eksempel:
Når en oppdaterer en post i Noraf, kan en oppleve at API-et gir 400-respons på grunn av feil i felt en ikke har endret.
Det validerer nemlig posten ved oppdatering, men garanterer ikke at eksisterende poster er gyldige.
Heldigvis inkluderer responsen en lettlest feilmelding som dette:

    "Illegal value in second indicator of field 672a"

Da kan det være praktisk å kunne hente ned posten til en lokal JSON-fil, gjøre nødvendige
rettinger lokalt, og så laste den opp igjen.

#### `noraf get` : hente en post

En kan hente ned en post slik:

    poetry run noraf get 1507616672055 > 1507616672055.json

#### `noraf put` : oppdatere en eksisterende post

En kan oppdatere en post slik:

    poetry run noraf put 1507616672055.json

Filnavnet har ingen signifikans. ID-en hentes fra "systemControlNumber"-feltet.

Hvis posten er ugyldig, skrives feilmeldingen ut.

#### `noraf post` : opprette en ny post

Denne kommandoen finnes ikke enda. Kan legges til i fremtiden ved behov.


#### `noraf link` : lenke Bibbi-post til Noraf-post manuelt

For å manuelt lenke en Bibbi-post til en Noraf-post:

    poetry run noraf link {bibbi_id} {noraf_id}

OBS: Bruk Bibbi-ID som argument, den konverteres til URI automatisk.

Dette vil 

1. Oppdatere Promus med NORAF-ID-en, samt annen informasjon fra Noraf (status, kjønn, landskode, etc.)
2. Oppdatere Noraf med Bibbi-URI-en, hvis den ikke allerede er lagt inn.

### Verifisering og fiksing av mappinger

#### Bibbi → Noraf

For å verifisere alle mappinger fra Bibbi til Noraf:

    poetry run verify_bibbi_noraf_mappings

OBS: Scriptet vil automatisk fikse følgende trivielle feil:

1. Hvis Bibbi-posten A peker til en Noraf-post B, som har blitt markert som erstattet av Noraf-posten C,
   vil scriptet oppdatere Bibbi-posten A til å peke til C.

2. Hvis Bibbi-posten A peker til en Noraf-post B, som har blitt slettet,
   men et søk i Noraf etter Bibbi-ID-en til post A returnerer én Noraf-post B,
   vil scriptet oppdatere A til å peke til C.
   Dette er for å håndtere sammenslåinger i Noraf som ikke er utført med merge-funksjonaliteten, men
   der Bibbi-lenken i stedet er manuelt overført.

3. Symmetri: Hvis Bibbi-posten A peker til en Noraf-post B, men Noraf-posten ikke peker tilbake,
   vil scriptet oppdatere Noraf-posten B med en lenke tilbake til A.

Videre produserer scriptet to rapporter:

1. `bibbi-noraf-overgang - feil.xlsx`: Feil som ikke lot seg fikse automatisk.
1. `bibbi-noraf-overgang - oversikt personer.xlsx`: Oversikt over alle mappingene.

#### Noraf → Bibbi

Dette scriptet trenger en oppdatert dump fra OAI-PMH:

    poetry run noraf harvest ../oai_harvest

(Dumpen oppdateres inkrementelt hvis det eksisterer en fullstendig dump fra før)

    poetry run verify_noraf_bibbi_mappings ../oai_harvest

OBS: Scriptet vil automatisk fikse følgende trivielle feil:

1. Hvis Noraf-posten A peker til flere Bibbi-poster B og C, og én av dem, B, har blitt slettet,
   vil scriptet fjerne lenken til B fra Noraf-posten A.
   Slike tilfeller stammer typisk fra dubletter som har blitt slått sammen først i Noraf,
   og deretter i Bibbi.

2. Symmetri: Hvis Noraf-posten A lenker til Bibbi-posten B, men Bibbi-posten B ikke lenker til noe,
   vil scriptet oppdatere Bibbi-posten med lenke tilbake til Noraf-posten A.   

3. Symmetri: Hvis Noraf-posten A lenker til Bibbi-posten B, men Bibbi-posten B lenker til en annen, slettet Noraf-post C,
   vil scriptet oppdatere Bibbi-posten med en lenke tilbake til Noraf-posten A.

Videre produserer scriptet tre rapporter:

1. `noraf-bibbi-overgang - døde lenkemål.xlsx`: Noraf-poster som lenker til slettede Bibbi-poster
2. `noraf-bibbi-overgang - symmetri-feil.xlsx`: Tilfeller der en Noraf-post A lenker til en Bibbi-post B, som lenker til en *annen* Noraf-post C, fremfor å lenke tilbake til A.
3. `noraf-bibbi-overgang - en-til-flere-mappinger.xlsx`: Tilfeller der en Noraf-post A lenker til to eller flere Bibbi-poster. Noen av disse kan skyldes at vi har ulike definisjoner av bibliografisk identitet (i teori eller praksis?), men mange er nok enten dubletter i Bibbi, eller poster i Noraf som burde vært delt opp.

