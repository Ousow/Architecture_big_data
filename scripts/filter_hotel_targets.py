"""
filter_hotel_targets.py

Usage :
    python filter_hotel_targets.py
    python filter_hotel_targets.py --dry-run   # affiche juste le compte, n'écrit rien
"""

import argparse
import logging
import os
from datetime import datetime, timezone

from pymongo import MongoClient, UpdateOne

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-8s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

MONGO_URI   = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
DB_NAME     = os.environ.get("MONGO_DB", "kbo_db")
SOURCE_COLL = "enterprise_silver"
BATCH       = 1000

HOTEL_NACE_CODES = [
    "55100",  # Hôtels et hébergement similaire
    "55201",  # Auberges de jeunesse
    "55202",  # Centres et villages de vacances
    "55203",  # Gîtes de vacances, appartements et meublés de vacances
    "55204",  # Chambres d'hôtes
    "55209",  # Autres hébergements de courte durée n.c.a.
    "55300",  # Terrains de camping et parcs pour caravanes
    "55400",  # Intermédiation pour l'hébergement (type Airbnb/Booking)
    "55900",  # Autres hébergements
]

EXCLUDED_JURIDICAL_FORMS = [
    "110", "114", "116", "117",                          # entités publiques
    "301", "302", "303",                                  # services fédéraux
    "310", "320", "330", "340", "350",                     # autorités régionales
    "400", "411", "412", "413", "414", "415", "416",       # communes, CPAS,
    "417", "418", "419", "420",                            # intercommunales
]


def build_filter_query() -> dict:
    return {
        "status": "AC",
        "type_of_enterprise": "2",
        "juridical_form": {"$nin": EXCLUDED_JURIDICAL_FORMS},
        "activities": {
            "$elemMatch": {
                "Classification": "MAIN",
                "NaceCode": {"$in": HOTEL_NACE_CODES},
            }
        },
    }


def load_targets_into_state_db(db, enterprise_numbers: list[str]) -> None:
    """
    Upsert chaque entreprise ciblée dans state_db, source='nbb', pending,
    sans écraser le statut d'une entreprise déjà in_progress/done.
    """
    operations = []
    for numero in enterprise_numbers:
        operations.append(
            UpdateOne(
                {
                    "enterprise_number": numero,
                    "source":            "nbb",
                    "year":              None,
                    "doc_type":          "target",
                },
                {
                    "$setOnInsert": {
                        "status":        "pending",
                        "first_seen_at": datetime.now(timezone.utc),
                    },
                },
                upsert=True,
            )
        )
        if len(operations) >= BATCH:
            db.state_db.bulk_write(operations, ordered=False)
            operations = []

    if operations:
        db.state_db.bulk_write(operations, ordered=False)


def run(dry_run: bool) -> None:
    client = MongoClient(MONGO_URI)
    db     = client[DB_NAME]

    query = build_filter_query()
    total_matched = db[SOURCE_COLL].count_documents(query)
    log.info(f"Entreprises hôtelières identifiées dans '{SOURCE_COLL}' : {total_matched}")

    if dry_run:
        log.info("Mode --dry-run : rien n'est écrit dans state_db.")
        return

    cursor = db[SOURCE_COLL].find(query, {"enterprise_number": 1})
    enterprise_numbers = [doc["enterprise_number"] for doc in cursor]

    load_targets_into_state_db(db, enterprise_numbers)

    pending_count = db.state_db.count_documents({
        "source": "nbb", "doc_type": "target", "status": "pending"
    })
    log.info(f"StateDB mise à jour. {pending_count} entreprises en status='pending' (source=nbb, doc_type=target).")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true",
                         help="Affiche uniquement le nombre de résultats, n'écrit rien en base")
    args = parser.parse_args()
    run(args.dry_run)


if __name__ == "__main__":
    main()
