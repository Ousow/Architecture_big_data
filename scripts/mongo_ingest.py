"""
mongo_ingest.py 
"""

import logging
import os
from datetime import datetime, timezone

from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError

log = logging.getLogger(__name__)

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB  = os.environ.get("MONGO_DB", "kbo_db")


def get_client() -> MongoClient:
    return MongoClient(MONGO_URI)


def get_db(client: MongoClient | None = None):
    client = client or get_client()
    return client[MONGO_DB]


def ensure_indexes(db) -> None:
    db.enterprises_rich.create_index("enterprise_number", unique=True)
    db.enterprises_rich.create_index("status")

    db.state_db.create_index(
        [("enterprise_number", 1), ("source", 1), ("year", 1), ("doc_type", 1)],
        unique=True,
        name="uniq_state_enterprise_source_year_type",
    )
    db.state_db.create_index("enterprise_number")
    db.state_db.create_index("status")

    db.batch_state.create_index("batch_id", unique=True)


# ════════════════════════════════════════════════════════════════
# Lecture des entreprises depuis Mongo (remplace companies.json)
# ════════════════════════════════════════════════════════════════

def get_enterprise_numbers(db, limit: int | None = None, offset: int = 0, status: str = "AC") -> list[str]:
    """
    Lit les numéros BCE depuis `enterprises_rich`, filtrés par statut,
    avec pagination (offset/limit). C'est la source officielle des
    entreprises à traiter — remplace l'ancien fichier companies.json.

    Le tri par _id est nécessaire pour garantir un ordre stable et
    déterministe à chaque appel, condition indispensable pour que
    offset/limit retournent toujours la même tranche d'entreprises
    d'un appel à l'autre (sinon le découpage en batches serait instable).
    """
    query = {"status": status} if status else {}
    cursor = db.enterprises_rich.find(query, {"enterprise_number": 1}).sort("_id", 1)
    if offset:
        cursor = cursor.skip(offset)
    if limit is not None:
        cursor = cursor.limit(limit)
    return [doc["enterprise_number"] for doc in cursor]


def get_enterprise_count(db, status: str = "AC") -> int:
    query = {"status": status} if status else {}
    return db.enterprises_rich.count_documents(query)


# ════════════════════════════════════════════════════════════════
# StateDB — tracking de chaque fichier téléchargé (toutes sources)
# ════════════════════════════════════════════════════════════════

def update_state(
    db,
    numero: str,
    source: str,         # "nbb" | "stapor" | "ejustice"
    year: int | None,
    doc_type: str,        # "pdf" | "csv"
    status: str,           # "pending" | "done" | "error"
    hdfs_path: str | None = None,
    deposit_id: str | None = None,
    error: str | None = None,
) -> None:
    """Upsert l'état d'un document précis dans la State DB unifiée."""
    db.state_db.update_one(
        {
            "enterprise_number": numero,
            "source":            source,
            "year":              year,
            "doc_type":          doc_type,
        },
        {
            "$set": {
                "status":      status,
                "hdfs_path":   hdfs_path,
                "deposit_id":  deposit_id,
                "error":       error,
                "updated_at":  datetime.now(timezone.utc),
            },
            "$setOnInsert": {
                "first_seen_at": datetime.now(timezone.utc),
            },
        },
        upsert=True,
    )


def is_already_done(db, numero: str, source: str, year: int | None, doc_type: str) -> bool:
    """Delta detection : True si ce document a déjà été téléchargé avec succès."""
    record = db.state_db.find_one({
        "enterprise_number": numero,
        "source":            source,
        "year":              year,
        "doc_type":          doc_type,
        "status":            "done",
    })
    return record is not None


def log_company_run(db, numero: str, source: str, status: str, error: str | None = None) -> None:
    """Journalise le résultat global du scraping d'une entreprise pour une source."""
    db.run_log.insert_one({
        "enterprise_number": numero,
        "source":             source,
        "status":             status,   # "ok" | "no_data" | "error"
        "error":              error,
        "run_at":             datetime.now(timezone.utc),
    })


# ════════════════════════════════════════════════════════════════
# Checkpoint par BATCH — reprise robuste sans rejouer les batches finis
# ════════════════════════════════════════════════════════════════

def is_batch_completed(db, batch_id: str) -> bool:
    record = db.batch_state.find_one({"batch_id": batch_id, "status": "completed"})
    return record is not None


def mark_batch_started(db, batch_id: str) -> None:
    db.batch_state.update_one(
        {"batch_id": batch_id},
        {"$set": {"status": "in_progress", "started_at": datetime.now(timezone.utc)}},
        upsert=True,
    )


def mark_batch_completed(db, batch_id: str, summary: dict) -> None:
    db.batch_state.update_one(
        {"batch_id": batch_id},
        {
            "$set": {
                "status":       "completed",
                "summary":      summary,
                "completed_at": datetime.now(timezone.utc),
            }
        },
        upsert=True,
    )
