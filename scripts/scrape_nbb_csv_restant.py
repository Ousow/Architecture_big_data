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

TMP_CSV = Path("tmp/csvs")
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

TARGET_YEARS    = {2021, 2022, 2023, 2024, 2025}
MAX_RETRIES_429 = 6
BACKOFF_SECONDS = 45


class RateLimited(Exception):
    pass


_tor_proxies = [p.strip() for p in os.environ.get("TOR_PROXIES", "").split(",") if p.strip()]
_proxy_cycle = itertools.cycle(_tor_proxies) if _tor_proxies else None


def next_proxy() -> dict | None:
    if not _proxy_cycle:
        return None
    proxy_url = next(_proxy_cycle)
    return {"http": proxy_url, "https": proxy_url}


def get_with_retry(session: requests.Session, url: str, **kwargs) -> requests.Response:
    for attempt in range(1, MAX_RETRIES_429 + 1):
        try:
            resp = session.get(url, timeout=30, **kwargs)
        except requests.RequestException as e:
            log.warning(f"    Erreur réseau (tentative {attempt}/{MAX_RETRIES_429}) : {e}")
            time.sleep(BACKOFF_SECONDS)
            continue

        if resp.status_code == 429:
            wait = BACKOFF_SECONDS * attempt
            log.warning(f"    429 (tentative {attempt}/{MAX_RETRIES_429}) — attente {wait}s + rotation proxy")
            proxy = next_proxy()
            if proxy:
                session.proxies.update(proxy)
            time.sleep(wait)
            continue

        return resp

    raise RateLimited(f"429 persistant sur {url}")


_hdfs_client = InsecureClient(HDFS_URL, user=HDFS_USER)


def upload_to_hdfs(local_path: Path, hdfs_path: str) -> bool:
    try:
        _hdfs_client.upload(hdfs_path, str(local_path), overwrite=True)
        log.info(f"    HDFS OK : {hdfs_path}")
        return True
    except Exception as e:
        log.error(f"    HDFS ERROR : {e}")
        return False


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
        if not year or year not in TARGET_YEARS or model_id.lower().startswith("mc"):
            continue
        if year not in selected or lang == "FR":
            selected[year] = dep
    return selected


def download_csv(numero: str, deposit: dict, session: requests.Session) -> Path | None:
    year = deposit["periodEndDateYear"]
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


def is_already_done(db, numero: str, year: int) -> bool:
    return db.state_db.find_one({
        "enterprise_number": numero, "source": "nbb", "year": year,
        "doc_type": "csv", "status": "done",
    }) is not None


def update_state(db, numero: str, year: int, status: str, hdfs_path: str | None = None, error: str | None = None) -> None:
    db.state_db.update_one(
        {"enterprise_number": numero, "source": "nbb", "year": year, "doc_type": "csv"},
        {
            "$set": {"status": status, "hdfs_path": hdfs_path, "error": error,
                      "updated_at": datetime.now(timezone.utc)},
            "$setOnInsert": {"first_seen_at": datetime.now(timezone.utc)},
        },
        upsert=True,
    )


def get_all_hotel_enterprise_numbers(db) -> list[str]:
    """Liste des 4908 entreprises, indépendamment du statut du tracking PDF+CSV mixte."""
    return db.state_db.distinct("enterprise_number", {"source": "nbb", "doc_type": "target"})


def is_target_csv_done(db, numero: str) -> bool:
    return db.state_db.find_one({
        "enterprise_number": numero, "source": "nbb", "year": None,
        "doc_type": "target_csv", "status": "done",
    }) is not None


def mark_target_csv(db, numero: str, status: str, csv_count: int | None = None) -> None:
    update = {"status": status, "updated_at": datetime.now(timezone.utc)}
    if csv_count is not None:
        update["csv_count"] = csv_count
    db.state_db.update_one(
        {"enterprise_number": numero, "source": "nbb", "year": None, "doc_type": "target_csv"},
        {"$set": update, "$setOnInsert": {"first_seen_at": datetime.now(timezone.utc)}},
        upsert=True,
    )


def process_enterprise_csv(db, numero: str) -> None:
    session = requests.Session()
    session.headers.update(BASE_HEADERS)
    session.headers["Referer"] = f"https://consult.cbso.nbb.be/consult-enterprise/{numero}"

    # Si tous les CSV cibles sont déjà en state_db=done, on ne fait même pas
    # d'appel API — gain de temps et de requêtes sur les entreprises déjà
    # couvertes par le run précédent (interrompu).
    if all(is_already_done(db, numero, year) for year in TARGET_YEARS):
        mark_target_csv(db, numero, "done", csv_count=len(TARGET_YEARS))
        log.info(f"  Déjà complet (5/5 CSV) — skip API.")
        return

    mark_target_csv(db, numero, "in_progress")

    deposits = get_deposits(numero, session)
    selected = select_deposits(deposits)  # déjà filtré sur TARGET_YEARS

    csv_done = 0
    for year, dep in sorted(selected.items(), reverse=True):
        if is_already_done(db, numero, year):
            csv_done += 1
            continue

        csv_path = download_csv(numero, dep, session)
        if csv_path:
            hdfs_path = f"/data/nbb/csvs/{numero}/{year}.csv"
            if upload_to_hdfs(csv_path, hdfs_path):
                update_state(db, numero, year, "done", hdfs_path=hdfs_path)
                csv_done += 1
            else:
                update_state(db, numero, year, "error", error="hdfs_upload_failed")
        time.sleep(0.5)

    mark_target_csv(db, numero, "done" if csv_done > 0 or not selected else "pending", csv_count=csv_done)


def run(limit: int | None) -> None:
    client = MongoClient(MONGO_URI)
    db     = client[DB_NAME]

    all_numbers = get_all_hotel_enterprise_numbers(db)
    todo = [n for n in all_numbers if not is_target_csv_done(db, n)]
    if limit is not None:
        todo = todo[:limit]

    log.info(f"{len(all_numbers)} entreprises au total, {len(todo)} restant à traiter pour les CSV 2021-2025")

    processed = 0
    for numero in todo:
        log.info(f"\n{'='*60}\n{numero}  ({processed + 1}/{len(todo)})\n{'='*60}")
        try:
            process_enterprise_csv(db, numero)
            processed += 1
            time.sleep(1)
        except RateLimited as e:
            log.error(f"Arrêt du run : {e}")
            log.error(f"{processed} entreprises traitées, {len(todo) - processed} restent à faire.")
            mark_target_csv(db, numero, "pending")
            break
        except Exception as e:
            log.error(f"Erreur inattendue sur {numero} : {e}")
            mark_target_csv(db, numero, "pending")
            continue

    log.info(f"\nRun terminé. {processed}/{len(todo)} entreprises traitées dans cette exécution.")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=None)
    args = parser.parse_args()
    run(args.limit)


if __name__ == "__main__":
    main()