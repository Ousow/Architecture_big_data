"""
import_kbo_enriched.py
────────────────────────
Charge le dump KBO Open Data complet dans MongoDB, en joignant
toutes les tables associées (activity, address, branch, contact,
establishment, denomination) autour de chaque entreprise active.

VERSION STAGING SQLITE — contrairement à une première version qui
indexait tous les fichiers annexes en RAM (defaultdict), celle-ci
charge chaque CSV dans une table SQLite sur disque, indexée par
numéro d'entreprise. La RAM utilisée reste minimale et constante
quel que soit le volume du dump KBO (testé pour fonctionner même
avec peu de mémoire allouée à Docker).

Étapes :
  1. Chaque fichier annexe (activity.csv, address.csv, ...) est
     streamé ligne par ligne et inséré en SQLite, avec un index sur
     la colonne de jointure (EnterpriseNumber ou EntityNumber).
  2. enterprise.csv est streamé à son tour ; pour chaque entreprise
     active, une requête SQL récupère ses lignes annexes associées
     (SELECT ... WHERE numero = ?), assemble le document enrichi,
     puis l'envoie vers MongoDB par batch (bulk write).
  3. Le fichier SQLite est un fichier temporaire sur disque,
     supprimable après l'import (conservé par défaut pour debug).

Usage :
    python import_kbo_enriched.py --kbo-dir /path/to/KBO
    python import_kbo_enriched.py --kbo-dir /path/to/KBO --limit 1000
"""

import argparse
import csv
import logging
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from pymongo import MongoClient, UpdateOne

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-8s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

MONGO_URI       = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
DB_NAME         = os.environ.get("MONGO_DB", "kbo_db")
COLLECTION_NAME = "enterprises_rich"
MONGO_BATCH     = 1000
SQLITE_BATCH    = 5000
SQLITE_PATH     = "/tmp/staging_kbo.db"

# (nom_table, fichier_csv, colonne_jointure)
ANNEX_TABLES = [
    ("activity",      "activity.csv",      "EntityNumber"),
    ("address",       "address.csv",       "EntityNumber"),
    ("branch",        "branch.csv",        "EnterpriseNumber"),
    ("contact",       "contact.csv",       "EntityNumber"),
    ("establishment", "establishment.csv", "EnterpriseNumber"),
    ("denomination",  "denomination.csv",  "EntityNumber"),
]


# ════════════════════════════════════════════════════════════════
# ÉTAPE 1 — Staging des fichiers annexes dans SQLite
# ════════════════════════════════════════════════════════════════

def stage_annex_file(conn: sqlite3.Connection, kbo_dir: Path, table: str, filename: str, join_col: str) -> bool:
    """
    Charge un fichier CSV annexe dans une table SQLite, ligne par
    ligne en streaming (jamais tout le fichier en RAM). Crée un
    index sur la colonne de jointure pour des lookups rapides.
    Retourne False si le fichier est introuvable (table ignorée).
    """
    filepath = kbo_dir / filename
    if not filepath.exists():
        log.warning(f"Fichier introuvable, ignoré : {filepath}")
        return False

    with open(filepath, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        if not fieldnames:
            log.warning(f"Fichier vide, ignoré : {filepath}")
            return False

        # Créer la table avec une colonne par champ CSV + jointure normalisée
        columns_sql = ", ".join(f'"{col}" TEXT' for col in fieldnames)
        conn.execute(f'DROP TABLE IF EXISTS "{table}"')
        conn.execute(f'CREATE TABLE "{table}" (numero TEXT, {columns_sql})')

        placeholders   = ", ".join("?" for _ in fieldnames)
        quoted_columns = ", ".join('"{}"'.format(c) for c in fieldnames)
        insert_sql     = f'INSERT INTO "{table}" (numero, {quoted_columns}) VALUES (?, {placeholders})'

        rows  = []
        count = 0
        for row in reader:
            numero = row.get(join_col, "").strip().replace(".", "")
            if not numero:
                continue
            rows.append((numero, *[row.get(col, "") for col in fieldnames]))
            count += 1

            if len(rows) >= SQLITE_BATCH:
                conn.executemany(insert_sql, rows)
                rows = []

        if rows:
            conn.executemany(insert_sql, rows)

        conn.execute(f'CREATE INDEX "idx_{table}_numero" ON "{table}" (numero)')
        conn.commit()

    log.info(f"  {table:14} → {count} lignes chargées en SQLite ({filename})")
    return True


def stage_all_annexes(conn: sqlite3.Connection, kbo_dir: Path) -> dict[str, list[str]]:
    """
    Charge tous les fichiers annexes en SQLite. Retourne, pour chaque
    table chargée avec succès, la liste de ses colonnes CSV d'origine
    (nécessaire ensuite pour reconstruire des dicts lisibles).
    """
    log.info("Staging des fichiers annexes dans SQLite...")
    columns_by_table: dict[str, list[str]] = {}

    for table, filename, join_col in ANNEX_TABLES:
        filepath = kbo_dir / filename
        if not filepath.exists():
            log.warning(f"Fichier introuvable, ignoré : {filepath}")
            continue

        with open(filepath, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            fieldnames = reader.fieldnames or []

        ok = stage_annex_file(conn, kbo_dir, table, filename, join_col)
        if ok:
            columns_by_table[table] = fieldnames

    return columns_by_table


# ════════════════════════════════════════════════════════════════
# ÉTAPE 2 — Lecture jointe SQLite → assemblage document Mongo
# ════════════════════════════════════════════════════════════════

def fetch_annex_rows(conn: sqlite3.Connection, table: str, columns: list[str], numero: str) -> list[dict]:
    """Récupère les lignes annexes d'une entreprise depuis SQLite, sous forme de liste de dicts."""
    quoted_columns = ", ".join('"{}"'.format(c) for c in columns)
    cursor = conn.execute(f'SELECT {quoted_columns} FROM "{table}" WHERE numero = ?', (numero,))
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def build_enterprise_document(row: dict, conn: sqlite3.Connection, columns_by_table: dict[str, list[str]]) -> dict:
    """Assemble un document enrichi pour une ligne de enterprise.csv."""
    numero = row["EnterpriseNumber"].strip().replace(".", "")

    doc = {
        "_id":                 numero,
        "enterprise_number":   numero,
        "status":              row.get("Status", "").strip(),
        "juridical_situation": row.get("JuridicalSituation", "").strip(),
        "type_of_enterprise":  row.get("TypeOfEnterprise", "").strip(),
        "juridical_form":      row.get("JuridicalForm", "").strip(),
        "juridical_form_cac":  row.get("JuridicalFormCAC", "").strip(),
        "start_date":          row.get("StartDate", "").strip(),
        "imported_at":         datetime.now(timezone.utc),
    }

    PLURAL_KEYS = {
        "activity":      "activities",
        "address":       "addresses",
        "branch":        "branches",
        "contact":       "contacts",
        "establishment": "establishments",
        "denomination":  "denominations",
    }

    for table, key in PLURAL_KEYS.items():
        if table in columns_by_table:
            doc[key] = fetch_annex_rows(conn, table, columns_by_table[table], numero)
        else:
            doc[key] = []

    return doc


# ════════════════════════════════════════════════════════════════
# ÉTAPE 3 — Import principal : enterprise.csv → MongoDB
# ════════════════════════════════════════════════════════════════

def import_enterprises(kbo_dir: Path, limit: int | None) -> None:
    client = MongoClient(MONGO_URI)
    db     = client[DB_NAME]
    coll   = db[COLLECTION_NAME]

    coll.create_index("enterprise_number", unique=True)
    coll.create_index("status")

    # SQLite frais à chaque run (on supprime l'éventuel fichier précédent)
    if os.path.exists(SQLITE_PATH):
        os.remove(SQLITE_PATH)
    sqlite_conn = sqlite3.connect(SQLITE_PATH)

    columns_by_table = stage_all_annexes(sqlite_conn, kbo_dir)

    enterprise_csv = kbo_dir / "enterprise.csv"
    if not enterprise_csv.exists():
        raise FileNotFoundError(f"Introuvable : {enterprise_csv}")

    log.info(f"Import de {enterprise_csv.name} → collection '{COLLECTION_NAME}'...")

    operations = []
    imported   = 0
    skipped    = 0

    with open(enterprise_csv, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            status = row.get("Status", "").strip().upper()
            if status != "AC":
                skipped += 1
                continue

            doc = build_enterprise_document(row, sqlite_conn, columns_by_table)
            operations.append(
                UpdateOne({"_id": doc["_id"]}, {"$set": doc}, upsert=True)
            )
            imported += 1

            if len(operations) >= MONGO_BATCH:
                coll.bulk_write(operations, ordered=False)
                operations = []
                if imported % 50000 == 0:
                    log.info(f"Progression : {imported} entreprises importées...")

            if limit is not None and imported >= limit:
                break

    if operations:
        coll.bulk_write(operations, ordered=False)

    sqlite_conn.close()

    log.info(f"Import terminé. {imported} entreprises importées, {skipped} ignorées (inactives).")
    log.info(f"Staging SQLite conservé pour debug : {SQLITE_PATH}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--kbo-dir", type=Path, required=True,
                         help="Dossier contenant les CSV KBO Open Data")
    parser.add_argument("--limit", type=int, default=None,
                         help="Limite optionnelle du nombre d'entreprises importées")
    args = parser.parse_args()

    import_enterprises(args.kbo_dir, args.limit)


if __name__ == "__main__":
    main()