# IMAP naar Gmail import

Python project om e-mails uit een IMAP map (bijv. one.com) te verplaatsen en/of te importeren naar Gmail via de Gmail API.

## Installatie

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

## Configuratie

1. Maak `.env` aan op basis van `.env.example`.
2. Zet Google OAuth client bestand als `credentials.json` (of pas `GMAIL_CREDENTIALS_FILE` aan).
3. Optioneel: stel `IMAP_DATE_FROM` en/of `IMAP_DATE_TO` in (`YYYY-MM-DD`) om alleen een datumbereik te verwerken.

## Stap 1: IMAP map verplaatsen

Verplaatst berichten van `IMAP_SOURCE_FOLDER` naar `IMAP_MOVE_TO_FOLDER`.
Als `STEP1_ENABLE_MOVE=false`, wordt deze stap overgeslagen.
Als `IMAP_CREATE_TARGET_FOLDER=true`, maakt het script de doelmap aan wanneer nodig.
Als je datumfilters zet, worden alleen die berichten verplaatst.

```powershell
python main.py step1
```

## Stap 2: Importeren naar Gmail

Leest berichten uit `GMAIL_IMPORT_SOURCE_FOLDER` op IMAP en importeert die via `users.messages.import`.
Als `GMAIL_ENABLE_IMPORT=false`, wordt deze stap overgeslagen.

Label-opties:

- `GMAIL_LABEL_STRATEGY=env`: labels uit `GMAIL_LABELS` (comma separated), bijvoorbeeld `INBOX,Imported`.
- `GMAIL_LABEL_STRATEGY=folder_mapping`: labels uit `label_mapping.json`.

```powershell
python main.py step2
```

Tip: je kunt direct importeren vanuit `IMAP_SOURCE_FOLDER` zonder eerst te verplaatsen door `python main.py step2` te draaien en `GMAIL_IMPORT_SOURCE_FOLDER` gelijk te zetten aan die bronmap. `step1` is optioneel en wordt alleen gebruikt als je expliciet wilt verplaatsen naar `IMAP_MOVE_TO_FOLDER`.


## Beide stappen achter elkaar

```powershell
python main.py all
```

## Logging

Alle output gaat naar console en logbestand (`LOG_FILE`, standaard `logs/imaptogmail.log`).
