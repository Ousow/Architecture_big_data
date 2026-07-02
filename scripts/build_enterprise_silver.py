"""
Lit `enterprises_rich` (Bronze, intacte, jamais modifiée) et écrit une
nouvelle collection `enterprise_silver` avec :

  1. StartDate normalisée DD-MM-YYYY -> YYYY-MM-DD
  2. Activités dédupliquées (même NaceCode EXACT + même Classification
     EXACT -> 1 seul exemplaire ; codes différents ou MAIN/SECO distincts
     conservés)
  3. Une seule adresse conservée : TypeOfAddress == "REGO"
  4. Dénominations réordonnées : TypeOfDenomination == "1" en premier
  5. Labels FR ajoutés à côté des codes bruts (via code.csv KBO) :
       - JuridicalForm      -> JuridicalFormLabel
       - Status             -> StatusLabel
       - activities[].NaceCode -> activities[].NaceLabel
         (recherché dans la catégorie Nace2003/2008/2025 selon NaceVersion)

Les codes originaux sont conservés (utiles pour filtrer/indexer) ; on
AJOUTE les labels, on ne remplace rien.

Usage :
    python build_enterprise_silver.py --code-csv /path/to/code.csv
    python build_enterprise_silver.py --code-csv /path/to/code.csv --limit 1000
"""

import argparse
import csv
import logging
import os
from datetime import datetime
from pathlib import Path

from pymongo import MongoClient, UpdateOne

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-8s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

MONGO_URI    = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
DB_NAME      = os.environ.get("MONGO_DB", "kbo_db")
SOURCE_COLL  = "enterprises_rich"
TARGET_COLL  = "enterprise_silver"
MONGO_BATCH  = 1000


# ════════════════════════════════════════════════════════════════
# Chargement des labels FR depuis code.csv
# ════════════════════════════════════════════════════════════════

def load_code_labels(code_csv_path: Path) -> dict[tuple[str, str], str]:
    """
    Charge code.csv (Category, Code, Language, Description) et retourne
    un dict {(Category, Code): Description} filtré sur la langue FR.
    """
    labels: dict[tuple[str, str], str] = {}
    with open(code_csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get("Language", "").strip().upper() != "FR":
                continue
            category = row.get("Category", "").strip()
            code     = row.get("Code", "").strip()
            desc     = row.get("Description", "").strip()
            labels[(category, code)] = desc
    log.info(f"code.csv chargé : {len(labels)} libellés FR indexés")
    return labels


def lookup_label(labels: dict, category: str, code: str) -> str | None:
    return labels.get((category, code))


# ════════════════════════════════════════════════════════════════
# Transformations Silver
# ════════════════════════════════════════════════════════════════

def normalize_date(date_str: str) -> str:
    """DD-MM-YYYY -> YYYY-MM-DD. Retourne la valeur d'origine si non parsable."""
    date_str = (date_str or "").strip()
    if not date_str:
        return date_str
    try:
        return datetime.strptime(date_str, "%d-%m-%Y").strftime("%Y-%m-%d")
    except ValueError:
        return date_str  # déjà normalisée, ou format inattendu -> on ne casse rien


def dedupe_activities(activities: list[dict]) -> list[dict]:
    """
    Déduplique strictement sur (NaceCode, Classification) identiques.
    Des versions Nace différentes (2008 vs 2025) avec des codes différents
    sont conservées telles quelles, même si elles décrivent la même activité.
    """
    seen = set()
    result = []
    for act in activities:
        key = (act.get("NaceCode", "").strip(), act.get("Classification", "").strip())
        if key in seen:
            continue
        seen.add(key)
        result.append(act)
    return result


def filter_main_address(addresses: list[dict]) -> list[dict]:
    """Ne garde que l'adresse de siège social enregistré (TypeOfAddress == REGO)."""
    return [a for a in addresses if a.get("TypeOfAddress", "").strip().upper() == "REGO"]


def reorder_denominations(denominations: list[dict]) -> list[dict]:
    """Place la dénomination officielle (TypeOfDenomination == '1' / '001') en premier."""
    def is_principal(d: dict) -> bool:
        t = d.get("TypeOfDenomination", "").strip().lstrip("0")
        return t == "1"

    principal   = [d for d in denominations if is_principal(d)]
    secondaries = [d for d in denominations if not is_principal(d)]
    return principal + secondaries


NACE_VERSION_TO_CATEGORY = {
    "2003": "Nace2003",
    "2008": "Nace2008",
    "2025": "Nace2025",
}


def decode_labels(doc: dict, labels: dict) -> None:
    """Ajoute les champs *Label en place, sans toucher aux codes bruts."""
    jf = doc.get("juridical_form", "").strip()
    if jf:
        doc["juridical_form_label"] = lookup_label(labels, "JuridicalForm", jf)

    status = doc.get("status", "").strip()
    if status:
        doc["status_label"] = lookup_label(labels, "Status", status)

    for act in doc.get("activities", []):
        nace_version = act.get("NaceVersion", "").strip()
        nace_code    = act.get("NaceCode", "").strip()
        category     = NACE_VERSION_TO_CATEGORY.get(nace_version)
        act["NaceLabel"] = lookup_label(labels, category, nace_code) if category else None


def build_silver_document(doc: dict, labels: dict) -> dict:
    """Applique toutes les transformations Silver sur un document Bronze."""
    silver = dict(doc)  # copie ; on ne modifie jamais l'original en RAM

    silver["start_date"]     = normalize_date(silver.get("start_date", ""))
    silver["activities"]     = dedupe_activities(silver.get("activities", []))
    silver["addresses"]      = filter_main_address(silver.get("addresses", []))
    silver["denominations"]  = reorder_denominations(silver.get("denominations", []))

    decode_labels(silver, labels)

    return silver


# ════════════════════════════════════════════════════════════════
# Pipeline principal
# ════════════════════════════════════════════════════════════════

def run(code_csv_path: Path, limit: int | None) -> None:
    labels = load_code_labels(code_csv_path)

    client = MongoClient(MONGO_URI)
    db     = client[DB_NAME]
    source = db[SOURCE_COLL]
    target = db[TARGET_COLL]

    target.create_index("enterprise_number", unique=True)
    target.create_index("status")

    total = source.count_documents({})
    log.info(f"Transformation Silver : {total} documents source dans '{SOURCE_COLL}'")

    operations = []
    processed  = 0

    cursor = source.find({})
    for doc in cursor:
        silver_doc = build_silver_document(doc, labels)
        operations.append(
            UpdateOne({"_id": silver_doc["_id"]}, {"$set": silver_doc}, upsert=True)
        )
        processed += 1

        if len(operations) >= MONGO_BATCH:
            target.bulk_write(operations, ordered=False)
            operations = []
            if processed % 50000 == 0:
                log.info(f"Progression : {processed}/{total} documents transformés...")

        if limit is not None and processed >= limit:
            break

    if operations:
        target.bulk_write(operations, ordered=False)

    log.info(f"Terminé. {processed} documents écrits dans '{TARGET_COLL}'.")
    log.info(f"'{SOURCE_COLL}' (Bronze) n'a pas été modifiée.")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--code-csv", type=Path, required=True,
                         help="Chemin vers code.csv (labels FR KBO)")
    parser.add_argument("--limit", type=int, default=None,
                         help="Limite optionnelle du nombre de documents transformés")
    args = parser.parse_args()

    run(args.code_csv, args.limit)


if __name__ == "__main__":
    main()
