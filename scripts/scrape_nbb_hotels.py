"""
scrape_nbb_hotels.py

  1. Liste ses dépôts CBSO (endpoint validé /rs-consult/published-deposits)
  2. Sélectionne un dépôt par année (comptes non consolidés, préférence FR)
  3. Télécharge le PDF (dès 2000) et le CSV (dès 2021)
  4. Upload vers HDFS : /data/nbb/pdfs/<numero>/<year>.pdf
                          /data/nbb/csvs/<numero>/<year>.csv
  5. Trace chaque fichier dans state_db (source="nbb", year, doc_type=pdf/csv)
  6. Marque l'entreprise "target" comme done une fois tous ses dépôts traités

"""

import argparse
import itertools
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
from hdfs import InsecureClient
from pymongo import MongoClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-8s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
DB_NAME   = os.environ.get("MONGO_DB", "kbo_db")
HDFS_URL  = os.environ.get("HDFS_URL", "http://namenode:9870")
HDFS_USER = os.environ.get("HDFS_USER", "root")

TMP_PDF = Path("tmp/pdfs")
TMP_CSV = Path("tmp/csvs")
TMP_PDF.mkdir(parents=True, exist_ok=True)
TMP_CSV.mkdir(parents=True, exist_ok=True)

BASE = "https://consult.cbso.nbb.be/api"
BASE_HEADERS = {
    "User-Agent":     "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/149.0.0.0 Safari/537.36",
    "Accept":         "application/json, text/plain, */*",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
}
CSV_URL_TEMPLATES = [
    f"{BASE}/external/broker/public/deposits/consult/csv/{{id}}",
    f"{BASE}/rs-consult/published-deposits/{{id}}/file/csv",
]

MIN_CSV_YEAR    = 2021
MAX_RETRIES_429 = 3
BACKOFF_SECONDS = 30


class RateLimited(Exception):
    """Levée quand le 429 persiste après toutes les tentatives — signal d'arrêt propre."""


# ════════════════════════════════════════════════════════════════
# Rotation des proxies Tor (utilisée uniquement en réaction à un 429)
# ════════════════════════════════════════════════════════════════

_tor_proxies = [p.strip() for p in os.environ.get("TOR_PROXIES", "").split(",") if p.strip()]
_proxy_cycle = itertools.cycle(_tor_proxies) if _tor_proxies else None


def next_proxy() -> dict | None:
    if not _proxy_cycle:
        return None
    proxy_url = next(_proxy_cycle)
    return {"http": proxy_url, "https": proxy_url}


def get_with_retry(session: requests.Session, url: str, **kwargs) -> requests.Response:
    """GET avec backoff + rotation Tor sur 429. Lève RateLimited si ça persiste."""
    for attempt in range(1, MAX_RETRIES_429 + 1):
        try:
            resp = session.get(url, timeout=30, **kwargs)
        except requests.RequestException as e:
            log.warning(f"    Erreur réseau (tentative {attempt}/{MAX_RETRIES_429}) : {e}")
            time.sleep(BACKOFF_SECONDS)
            continue

        if resp.status_code == 429:
            wait = BACKOFF_SECONDS * attempt
            log.warning(f"    429 rate limit (tentative {attempt}/{MAX_RETRIES_429}) — attente {wait}s + rotation proxy")
            proxy = next_proxy()
            if proxy:
                session.proxies.update(proxy)
            time.sleep(wait)
            continue

        return resp

    raise RateLimited(f"429 persistant sur {url}")


# ════════════════════════════════════════════════════════════════
# HDFS — upload via WebHDFS (HTTP direct, aucun accès au socket Docker
# nécessaire — fonctionne depuis n'importe quel container du réseau)
# ════════════════════════════════════════════════════════════════

_hdfs_client = InsecureClient(HDFS_URL, user=HDFS_USER)


def upload_to_hdfs(local_path: Path, hdfs_path: str) -> bool:
    try:
        _hdfs_client.upload(hdfs_path, str(local_path), overwrite=True)
        log.info(f"    HDFS OK : {hdfs_path}")
        return True
    except Exception as e:
        log.error(f"    HDFS ERROR : {e}")
        return False


# ════════════════════════════════════════════════════════════════
# API CBSO — liste + sélection des dépôts (méthode validée, inchangée)
# ════════════════════════════════════════════════════════════════

def get_deposits(numero: str, session: requests.Session) -> list:
    resp = get_with_retry(
        session,
        f"{BASE}/rs-consult/published-deposits"
        f"?page=0&size=50&enterpriseNumber={numero}&sort=periodEndDate,desc",
    )
    return resp.json().get("content", [])


def select_deposits(deposits: list) -> dict:
    selected = {}
    for dep in deposits:
        year     = dep.get("periodEndDateYear")
        model_id = dep.get("modelId", "")
        lang     = dep.get("language", "")
        if not year or model_id.lower().startswith("mc"):
            continue
        if year not in selected or lang == "FR":
            selected[year] = dep
    return selected


def download_pdf(numero: str, deposit: dict, session: requests.Session) -> Path | None:
    year = deposit["periodEndDateYear"]
    dest = TMP_PDF / numero / f"{year}.pdf"
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.exists():
        return dest

    resp = get_with_retry(session, f"{BASE}/external/broker/public/deposits/pdf/{deposit['id']}")
    if resp.status_code == 200 and resp.content:
        dest.write_bytes(resp.content)
        log.info(f"  PDF OK  : {dest.name} ({len(resp.content)//1024} KB)")
        return dest

    log.warning(f"  PDF ERROR ({resp.status_code}) : année {year}")
    return None


def download_csv(numero: str, deposit: dict, session: requests.Session) -> Path | None:
    year = deposit["periodEndDateYear"]
    if year < MIN_CSV_YEAR:
        return None

    dest = TMP_CSV / numero / f"{year}.csv"
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.exists():
        return dest

    for tmpl in CSV_URL_TEMPLATES:
        resp = get_with_retry(session, tmpl.format(id=deposit["id"]))
        if resp.status_code == 200 and resp.content:
            dest.write_bytes(resp.content)
            log.info(f"  CSV OK  : {dest.name} ({len(resp.content)//1024} KB)")
            return dest

    log.warning(f"  CSV ERROR : année {year}")
    return None


# ════════════════════════════════════════════════════════════════
# StateDB — mêmes conventions que mongo_ingest.py
# ════════════════════════════════════════════════════════════════

def is_already_done(db, numero: str, source: str, year, doc_type: str) -> bool:
    return db.state_db.find_one({
        "enterprise_number": numero, "source": source, "year": year,
        "doc_type": doc_type, "status": "done",
    }) is not None


def update_state(db, numero: str, source: str, year, doc_type: str, status: str,
                  hdfs_path: str | None = None, error: str | None = None) -> None:
    db.state_db.update_one(
        {"enterprise_number": numero, "source": source, "year": year, "doc_type": doc_type},
        {
            "$set": {"status": status, "hdfs_path": hdfs_path, "error": error,
                      "updated_at": datetime.now(timezone.utc)},
            "$setOnInsert": {"first_seen_at": datetime.now(timezone.utc)},
        },
        upsert=True,
    )


def get_pending_targets(db, limit: int | None) -> list[str]:
    query  = {"source": "nbb", "doc_type": "target", "status": "pending"}
    cursor = db.state_db.find(query, {"enterprise_number": 1})
    if limit is not None:
        cursor = cursor.limit(limit)
    return [doc["enterprise_number"] for doc in cursor]


def mark_target(db, numero: str, status: str, filings_count: int | None = None) -> None:
    update = {"status": status, "updated_at": datetime.now(timezone.utc)}
    if filings_count is not None:
        update["filings_count"] = filings_count
    db.state_db.update_one(
        {"enterprise_number": numero, "source": "nbb", "year": None, "doc_type": "target"},
        {"$set": update},
    )


# ════════════════════════════════════════════════════════════════
# Traitement d'une entreprise
# ════════════════════════════════════════════════════════════════

def process_enterprise(db, numero: str) -> None:
    session = requests.Session()
    session.headers.update(BASE_HEADERS)
    session.headers["Referer"] = f"https://consult.cbso.nbb.be/consult-enterprise/{numero}"

    mark_target(db, numero, "in_progress")

    deposits = get_deposits(numero, session)
    selected = select_deposits(deposits)
    filings_done = 0

    for year, dep in sorted(selected.items(), reverse=True):
        if year >= 2000 and not is_already_done(db, numero, "nbb", year, "pdf"):
            pdf_path = download_pdf(numero, dep, session)
            if pdf_path:
                hdfs_path = f"/data/nbb/pdfs/{numero}/{year}.pdf"
                if upload_to_hdfs(pdf_path, hdfs_path):
                    update_state(db, numero, "nbb", year, "pdf", "done", hdfs_path=hdfs_path)
                    filings_done += 1
                else:
                    update_state(db, numero, "nbb", year, "pdf", "error", error="hdfs_upload_failed")

        if year >= MIN_CSV_YEAR and not is_already_done(db, numero, "nbb", year, "csv"):
            csv_path = download_csv(numero, dep, session)
            if csv_path:
                hdfs_path = f"/data/nbb/csvs/{numero}/{year}.csv"
                if upload_to_hdfs(csv_path, hdfs_path):
                    update_state(db, numero, "nbb", year, "csv", "done", hdfs_path=hdfs_path)
                    filings_done += 1
                else:
                    update_state(db, numero, "nbb", year, "csv", "error", error="hdfs_upload_failed")

        time.sleep(0.3)

    if filings_done > 0:
        mark_target(db, numero, "done", filings_count=filings_done)
    else:
        # Aucun fichier n'a pu être uploadé (ex: HDFS down) — on remet en
        # pending plutôt que de marquer "done" à tort, pour reprise ultérieure.
        mark_target(db, numero, "pending", filings_count=0)
        log.warning(f"  Aucun fichier uploadé pour {numero} — remis en pending.")


# ════════════════════════════════════════════════════════════════
# Pipeline principal
# ════════════════════════════════════════════════════════════════

def run(limit: int | None) -> None:
    client = MongoClient(MONGO_URI)
    db     = client[DB_NAME]

    targets = get_pending_targets(db, limit)
    log.info(f"{len(targets)} entreprises hôtelières à scraper (status=pending)")

    processed = 0
    for numero in targets:
        log.info(f"\n{'='*60}\n{numero}  ({processed + 1}/{len(targets)})\n{'='*60}")
        try:
            process_enterprise(db, numero)
            processed += 1
        except RateLimited as e:
            log.error(f"Arrêt du run : {e}")
            log.error(f"{processed} entreprises traitées, {len(targets) - processed} restent en pending.")
            mark_target(db, numero, "pending")  # on remet celle en cours en pending, pas de perte
            break
        except Exception as e:
            log.error(f"Erreur inattendue sur {numero} : {e}")
            mark_target(db, numero, "pending")
            continue

    log.info(f"\nRun terminé. {processed}/{len(targets)} entreprises traitées dans cette exécution.")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=None,
                         help="Limite optionnelle du nombre d'entreprises à traiter dans ce run")
    args = parser.parse_args()
    run(args.limit)


if __name__ == "__main__":
    main()