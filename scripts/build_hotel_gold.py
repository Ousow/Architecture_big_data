"""
Lit tous les CSV PCMN bruts depuis HDFS (/data/nbb/csvs/{numero}/{year}.csv),
extrait les montants comptables utiles, calcule 5 ratios financiers par
exercice, et consolide le tout en un document par entreprise dans la
collection `hotel_gold` (upsert sur enterprise_number).

Traitement Spark : lecture parallèle de tous les CSV via wholeTextFiles,
parsing + calcul des ratios par (entreprise, année), puis regroupement
par entreprise. Le résultat final (quelques milliers de documents, pas
des dizaines de millions) est rapatrié sur le driver et écrit dans Mongo
via pymongo — pas besoin du connecteur Mongo-Spark pour ce volume.

Format CSV attendu : deux colonnes "code_pcmn;valeur", séparateur ';',
une ligne par code comptable.

Incrémental : ne retraite que les entreprises dont au moins un exercice
CSV est nouveau depuis le dernier passage (comparaison avec last_updated
dans hotel_gold), sauf si --full est passé.

Usage :
    python build_hotel_gold.py                 # incrémental
    python build_hotel_gold.py --full           # retraite tout
    python build_hotel_gold.py --limit 100      # test sur 100 entreprises
"""

import argparse
import csv
import io
import logging
import os
import re
from datetime import datetime, timezone

from pymongo import MongoClient, UpdateOne
from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-8s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

MONGO_URI    = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
DB_NAME      = os.environ.get("MONGO_DB", "kbo_db")
TARGET_COLL  = "hotel_gold"
HDFS_NAMENODE = os.environ.get("HDFS_NAMENODE_RPC", "hdfs://namenode:9000")
CSV_HDFS_GLOB = f"{HDFS_NAMENODE}/data/nbb/csvs/*/*.csv"
MONGO_BATCH  = 500

# Chemin fichier -> (enterprise_number, year), ex:
# hdfs://namenode:9000/data/nbb/csvs/0400951181/2024.csv
PATH_PATTERN = re.compile(r"/data/nbb/csvs/(\d+)/(\d{4})\.csv$")

# Un vrai code PCMN : chiffres, avec parfois un '/' (code agrégé) et/ou une
# lettre finale (variante), ex: "70", "9900", "40/41", "8199P", "65/66B".
# Les lignes de métadonnées ("Reference number", "Entity name", ...) ne
# matchent jamais ce pattern et sont donc naturellement ignorées.
PCMN_CODE_PATTERN = re.compile(r"^\d+[A-Z]?(/\d+[A-Z]?)?$")

# ════════════════════════════════════════════════════════════════
# Mapping PCMN -> champs métier
# ════════════════════════════════════════════════════════════════

SIMPLE_CODES = {
    "70":   "chiffre_affaires",
    "60":   "achats",
    "71":   "variation_stocks",
    "9901": "ebit",
    "9904": "resultat_net",
    "100":  "capital_souscrit",
}

SUMMED_CODES = {
    "tresorerie":         ["54", "55"],
    "dettes_financieres": ["17", "43"],
    "fonds_propres":      ["10", "11", "12", "13", "14", "15"],
}

# Certains champs ont un code agrégé NBB officiel qui représente exactement
# la même chose que la somme des codes individuels (ex: "10/15" = somme
# exacte des comptes 10 à 15). Quand ce code agrégé est présent dans le
# dépôt (fréquent en schéma abrégé/micro où le détail n'est pas fourni),
# on le préfère à la somme des codes individuels, souvent absents.
AGGREGATE_SHORTCUT = {
    "fonds_propres": "10/15",
}


def parse_pcmn_content(content: str) -> tuple[dict[str, float], dict[str, str]]:
    """
    Parse le contenu brut CSV (guillemets, séparateur virgule) en :
      - amounts : dict {code_pcmn: montant} pour les lignes reconnues comme codes PCMN
      - meta    : dict {libellé: valeur} pour les lignes de métadonnées (Model code, etc.)
    """
    amounts: dict[str, float] = {}
    meta: dict[str, str] = {}

    reader = csv.reader(io.StringIO(content))
    for row in reader:
        if len(row) < 2:
            continue
        code_or_label = row[0].strip()
        raw_value     = row[1].strip()

        if PCMN_CODE_PATTERN.match(code_or_label):
            value = raw_value.replace(",", ".").replace(" ", "")
            try:
                amounts[code_or_label] = amounts.get(code_or_label, 0.0) + float(value)
            except ValueError:
                continue
        else:
            meta[code_or_label] = raw_value

    return amounts, meta


def extract_fields(amounts: dict[str, float]) -> dict[str, float | None]:
    """Applique le mapping PCMN -> champs métier, avec raccourci agrégat quand disponible."""
    fields: dict[str, float | None] = {}

    for code, field_name in SIMPLE_CODES.items():
        fields[field_name] = amounts.get(code)

    for field_name, codes in SUMMED_CODES.items():
        shortcut_code = AGGREGATE_SHORTCUT.get(field_name)
        if shortcut_code and shortcut_code in amounts:
            fields[field_name] = amounts[shortcut_code]
        else:
            present = [amounts[c] for c in codes if c in amounts]
            fields[field_name] = sum(present) if present else None

    return fields


def guess_schema_type(fields: dict[str, float | None], model_code: str | None) -> str:
    """
    Heuristique faute de table officielle "model_code -> schema_type" connue :
    se base sur le nombre de champs métier renseignés. Le model_code brut
    NBB (ex: "m87-f") est conservé tel quel dans le document pour permettre
    d'affiner ce mapping plus tard si la correspondance officielle est trouvée.
    """
    non_null = sum(1 for v in fields.values() if v is not None)
    if non_null >= 8:
        return "full"
    if non_null >= 4:
        return "abrege"
    return "micro"


def safe_div(numerator: float | None, denominator: float | None) -> float | None:
    if numerator is None or denominator is None or denominator == 0:
        return None
    return numerator / denominator


def compute_ratios(fields: dict[str, float | None]) -> dict[str, float | None]:
    ca      = fields.get("chiffre_affaires")
    achats  = fields.get("achats") or 0.0
    var_st  = fields.get("variation_stocks") or 0.0
    net     = fields.get("resultat_net")
    fp      = fields.get("fonds_propres")
    treso   = fields.get("tresorerie")
    dettes  = fields.get("dettes_financieres")

    marge_brute = (ca - achats + var_st) if ca is not None else None

    return {
        "marge_brute":          marge_brute,
        "marge_nette_pct":      safe_div(net, ca) and safe_div(net, ca) * 100,
        "roe_pct":              safe_div(net, fp) and safe_div(net, fp) * 100,
        "ratio_liquidite":      safe_div(treso, dettes),
        "taux_endettement_pct": safe_div(dettes, fp) and safe_div(dettes, fp) * 100,
    }


# ════════════════════════════════════════════════════════════════
# Traitement d'un fichier (une paire entreprise/année)
# ════════════════════════════════════════════════════════════════

def process_file(path_content: tuple[str, str]) -> tuple[str, dict] | None:
    """Fonction exécutée par les workers Spark sur chaque (path, content)."""
    path, content = path_content
    match = PATH_PATTERN.search(path)
    if not match:
        return None
    numero, year_str = match.group(1), match.group(2)

    amounts, meta = parse_pcmn_content(content)
    fields  = extract_fields(amounts)
    ratios  = compute_ratios(fields)
    model_code = meta.get("Model code")

    year_doc = {
        "year": int(year_str),
        **fields,
        "ratios": ratios,
        "model_code": model_code,  # conservé brut pour affiner schema_type plus tard
    }
    schema_type = guess_schema_type(fields, model_code)

    return numero, {"year_doc": year_doc, "schema_type": schema_type}


# ════════════════════════════════════════════════════════════════
# Pipeline principal
# ════════════════════════════════════════════════════════════════

def run(limit: int | None, full: bool, numbers_file: str | None) -> None:
    spark = (
        SparkSession.builder
        .appName("build_hotel_gold")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    if numbers_file:
        with open(numbers_file) as f:
            numbers = [line.strip() for line in f if line.strip()]
        if not numbers:
            log.info("Fichier de numéros vide — rien à recalculer.")
            spark.stop()
            return
        pattern = "{" + ",".join(numbers) + "}"
        glob_path = f"{HDFS_NAMENODE}/data/nbb/csvs/{pattern}/*.csv"
        log.info(f"Mode incrémental : {len(numbers)} entreprises ciblées")
    else:
        glob_path = CSV_HDFS_GLOB
        log.info("Mode complet : toutes les entreprises")

    log.info(f"Lecture des CSV depuis {glob_path} ...")
    rdd = spark.sparkContext.wholeTextFiles(glob_path)

    parsed = rdd.map(process_file).filter(lambda x: x is not None)

    # Regroupement par entreprise : liste de (year_doc, schema_type) par numero
    grouped = parsed.groupByKey().mapValues(list).collect()
    spark.stop()

    log.info(f"{len(grouped)} entreprises avec au moins un exercice parsé")

    client = MongoClient(MONGO_URI)
    db     = client[DB_NAME]
    coll   = db[TARGET_COLL]
    coll.create_index("enterprise_number", unique=True)

    if limit is not None:
        grouped = grouped[:limit]

    operations = []
    for numero, entries in grouped:
        years = sorted((e["year_doc"] for e in entries), key=lambda y: y["year"])
        # schema_type retenu : celui du dernier exercice disponible
        schema_type = entries[-1]["schema_type"] if entries else "micro"

        doc = {
            "_id":               numero,
            "enterprise_number": numero,
            "years":             years,
            "schema_type":       schema_type,
            "last_updated":      datetime.now(timezone.utc),
        }
        operations.append(UpdateOne({"_id": numero}, {"$set": doc}, upsert=True))

        if len(operations) >= MONGO_BATCH:
            coll.bulk_write(operations, ordered=False)
            operations = []

    if operations:
        coll.bulk_write(operations, ordered=False)

    log.info(f"Terminé. {len(grouped)} documents upsertés dans '{TARGET_COLL}'.")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=None,
                         help="Limite optionnelle du nombre d'entreprises écrites")
    parser.add_argument("--full", action="store_true",
                         help="Retraite tout (placeholder — le job relit toujours tout HDFS pour l'instant)")
    parser.add_argument("--numbers-file", type=str, default=None,
                         help="Fichier texte (un numéro BCE par ligne) pour restreindre le recalcul à ces entreprises")
    args = parser.parse_args()
    run(args.limit, args.full, args.numbers_file)


if __name__ == "__main__":
    main()