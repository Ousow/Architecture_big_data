"""
Recalcul annuel incrémental de hotel_gold.

Logique (StateDB-driven, ne retraite que ce qui a changé) :
  1. Lister les entreprises dont le scraping CSV est déjà "done" (state_db,
     source=nbb, doc_type=target_csv, status=done)
  2. Pour chacune, interroger l'API NBB et comparer les années disponibles
     à ce qui est déjà tracé en state_db (doc_type=csv, status=done)
  3. Télécharger uniquement les CSV des années manquantes, upload HDFS,
     mise à jour state_db
  4. Relancer build_hotel_gold.py en mode incrémental (--numbers-file),
     uniquement sur les entreprises ayant reçu de nouveaux exercices
  5. build_hotel_gold.py upsert déjà hotel_gold.years par entreprise —
     rien de plus à faire ici

Déclenchement : annuel (@yearly). Comme tous les DAGs de ce projet,
il démarre en pause (AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=true) —
activation et déclenchement manuels depuis l'UI Airflow.
"""

import itertools
import logging
import os
import subprocess
import tempfile
import time
from datetime import datetime, timedelta, timezone

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from pymongo import MongoClient

log = logging.getLogger(__name__)

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://mongo:27017")
MONGO_DB  = os.environ.get("MONGO_DB", "kbo_db")

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
MIN_CSV_YEAR = 2021
MAX_RETRIES_429 = 6
BACKOFF_SECONDS = 45

_tor_proxies = [p.strip() for p in os.environ.get("TOR_PROXIES", "").split(",") if p.strip()]
_proxy_cycle = itertools.cycle(_tor_proxies) if _tor_proxies else None


def _next_proxy():
    if not _proxy_cycle:
        return None
    proxy_url = next(_proxy_cycle)
    return {"http": proxy_url, "https": proxy_url}


def _get_with_retry(session, url, **kwargs):
    for attempt in range(1, MAX_RETRIES_429 + 1):
        try:
            resp = session.get(url, timeout=30, **kwargs)
        except requests.RequestException:
            time.sleep(BACKOFF_SECONDS)
            continue
        if resp.status_code == 429:
            proxy = _next_proxy()
            if proxy:
                session.proxies.update(proxy)
            time.sleep(BACKOFF_SECONDS * attempt)
            continue
        return resp
    return None  # rate limit persistant — l'appelant doit gérer le None


def _get_deposits(numero: str, session: requests.Session) -> list:
    resp = _get_with_retry(
        session,
        f"{BASE}/rs-consult/published-deposits"
        f"?page=0&size=50&enterpriseNumber={numero}&sort=periodEndDate,desc",
    )
    if resp is None:
        return []
    return resp.json().get("content", [])


def _select_deposits(deposits: list) -> dict:
    selected = {}
    for dep in deposits:
        year     = dep.get("periodEndDateYear")
        model_id = dep.get("modelId", "")
        lang     = dep.get("language", "")
        if not year or year < MIN_CSV_YEAR or model_id.lower().startswith("mc"):
            continue
        if year not in selected or lang == "FR":
            selected[year] = dep
    return selected


# ════════════════════════════════════════════════════════════════
# Étape 1+2 : lister les entreprises "done" et détecter les nouveaux dépôts
# ════════════════════════════════════════════════════════════════

def check_new_deposits(**context):
    client = MongoClient(MONGO_URI)
    db     = client[MONGO_DB]

    done_enterprises = db.state_db.distinct(
        "enterprise_number", {"source": "nbb", "doc_type": "target_csv", "status": "done"}
    )
    log.info(f"{len(done_enterprises)} entreprises déjà scrapées à vérifier")

    to_update = {}  # {numero: {year: deposit}}
    for numero in done_enterprises:
        session = requests.Session()
        session.headers.update(BASE_HEADERS)
        session.headers["Referer"] = f"https://consult.cbso.nbb.be/consult-enterprise/{numero}"

        deposits = _get_deposits(numero, session)
        selected = _select_deposits(deposits)

        missing_years = {
            year: dep for year, dep in selected.items()
            if db.state_db.count_documents({
                "enterprise_number": numero, "source": "nbb", "year": year,
                "doc_type": "csv", "status": "done",
            }) == 0
        }
        if missing_years:
            to_update[numero] = missing_years
        time.sleep(0.5)

    log.info(f"{len(to_update)} entreprises ont de nouveaux exercices à récupérer")
    # On ne peut pas XCom des objets requests/deposit complets facilement au-delà de
    # l'essentiel : on ne pousse que {numero: [years]}, on re-fetchera le deposit
    # exact (id) à l'étape suivante pour rester simple et robuste en cas de reprise.
    context["ti"].xcom_push(key="to_update_years", value={k: list(v.keys()) for k, v in to_update.items()})


# ════════════════════════════════════════════════════════════════
# Étape 3 : téléchargement des CSV manquants + upload HDFS
# ════════════════════════════════════════════════════════════════

def download_missing_csv(**context):
    from hdfs import InsecureClient
    from pathlib import Path

    HDFS_URL  = os.environ.get("HDFS_URL", "http://namenode:9870")
    HDFS_USER = os.environ.get("HDFS_USER", "root")
    hdfs_client = InsecureClient(HDFS_URL, user=HDFS_USER)

    tmp_dir = Path(tempfile.mkdtemp(prefix="gold_recalc_csv_"))

    client = MongoClient(MONGO_URI)
    db     = client[MONGO_DB]

    to_update_years = context["ti"].xcom_pull(key="to_update_years", task_ids="check_new_deposits") or {}
    updated_enterprises = []

    for numero, years in to_update_years.items():
        session = requests.Session()
        session.headers.update(BASE_HEADERS)
        session.headers["Referer"] = f"https://consult.cbso.nbb.be/consult-enterprise/{numero}"

        deposits = _get_deposits(numero, session)
        selected = _select_deposits(deposits)

        got_at_least_one = False
        for year in years:
            dep = selected.get(year)
            if not dep:
                continue

            dest = tmp_dir / numero / f"{year}.csv"
            dest.parent.mkdir(parents=True, exist_ok=True)

            downloaded = False
            for tmpl in CSV_URL_TEMPLATES:
                resp = _get_with_retry(session, tmpl.format(id=dep["id"]))
                if resp is not None and resp.status_code == 200 and resp.content:
                    dest.write_bytes(resp.content)
                    downloaded = True
                    break

            if downloaded:
                hdfs_path = f"/data/nbb/csvs/{numero}/{year}.csv"
                try:
                    hdfs_client.upload(hdfs_path, str(dest), overwrite=True)
                    db.state_db.update_one(
                        {"enterprise_number": numero, "source": "nbb", "year": year, "doc_type": "csv"},
                        {"$set": {"status": "done", "hdfs_path": hdfs_path,
                                   "updated_at": datetime.now(timezone.utc)},
                         "$setOnInsert": {"first_seen_at": datetime.now(timezone.utc)}},
                        upsert=True,
                    )
                    got_at_least_one = True
                except Exception as e:
                    log.error(f"Upload HDFS échoué pour {numero}/{year}: {e}")

            time.sleep(0.5)

        if got_at_least_one:
            updated_enterprises.append(numero)
            db.state_db.update_one(
                {"enterprise_number": numero, "source": "nbb", "year": None, "doc_type": "target_csv"},
                {"$set": {"updated_at": datetime.now(timezone.utc)}},
            )

    log.info(f"{len(updated_enterprises)} entreprises effectivement mises à jour")
    context["ti"].xcom_push(key="updated_enterprises", value=updated_enterprises)


# ════════════════════════════════════════════════════════════════
# Étape 4+5 : recalcul Gold incrémental (Spark) + upsert Mongo
# ════════════════════════════════════════════════════════════════

def recalc_gold(**context):
    updated_enterprises = context["ti"].xcom_pull(key="updated_enterprises", task_ids="download_missing_csv") or []

    if not updated_enterprises:
        log.info("Aucune entreprise à recalculer cette année — rien à faire.")
        return

    numbers_file = tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False)
    numbers_file.write("\n".join(updated_enterprises))
    numbers_file.close()

    log.info(f"Recalcul Gold pour {len(updated_enterprises)} entreprises...")
    result = subprocess.run(
        ["python", "/opt/airflow/scripts/build_hotel_gold.py", "--numbers-file", numbers_file.name],
        capture_output=True, text=True,
    )
    log.info(result.stdout)
    if result.returncode != 0:
        log.error(result.stderr)
        raise RuntimeError("build_hotel_gold.py a échoué — voir logs ci-dessus")


# ════════════════════════════════════════════════════════════════
# Définition du DAG
# ════════════════════════════════════════════════════════════════

default_args = {
    "owner": "oumi",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="hotel_gold_recalc",
    description="Recalcul annuel incrémental de la Gold layer (nouveaux dépôts NBB uniquement)",
    default_args=default_args,
    schedule="@yearly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["kbo", "gold", "incremental"],
) as dag:

    t1 = PythonOperator(
        task_id="check_new_deposits",
        python_callable=check_new_deposits,
    )

    t2 = PythonOperator(
        task_id="download_missing_csv",
        python_callable=download_missing_csv,
    )

    t3 = PythonOperator(
        task_id="recalc_gold",
        python_callable=recalc_gold,
    )

    t1 >> t2 >> t3
