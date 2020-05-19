Script for å matche personer i _Bibbi autoriteter_ med _Felles autoritetsregister_ (BARE eller NORAF) basert på litteraturbelegg.

Scriptet henter først ut fra _Promus_ en liste over (godkjente) personer i Bibbi som ikke har en kobling til BARE, og alle ISBN-numre knyttet til dem.

For hver person, gjør det så et søk i _Alma_ (network zone for Bibsys-konsortiet) etter ISBN-numrene knyttet til denne personen. Hvis det får treff på et ISBN-nummer, sjekker det om MARC-posten har et felt for ansvarshavende som matcher personen vi leter:
- Vi regner det som en match hvis verdiene i `$a` og `$d` er like. Også hvis `$d` mangler i begge.
- Hvis `$a` er like, men `$d` ikke er det, må det sjekkes manuelt, siden det kan være feil i enten Bibbi-autoriteten eller BARE-autoriteten.


Bruk:

    cp .env.dist .env  # og legg inn påloggingsinformasjon
    poetry install
    poetry run match_persons

Testing:

    poetry run pytest
