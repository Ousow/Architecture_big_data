"""
nbb_scraper.py
───────────────
Module de scraping NBB/CBSO — reprend la logique validée manuellement :
- Liste des dépôts via l'API published-deposits
- Filtre : exclusion des comptes consolidés (modelId 'mc-*'),
           1 seul dépôt par année (préférence FR > NL),
           limité aux 3 dernières années disponibles
- Téléchargement PDF via l'endpoint pdf/{id} (validé)
- Téléchargement CSV via consult/csv/{id} (validé, dispo depuis 2021)

Toutes les fonctions retournent des bytes bruts — aucun parsing,
aucune transformation. L'ingestion MongoDB stocke le contenu tel quel.
"""

import logging
import time
import requests

log = logging.getLogger(__name__)

BASE = "https://consult.cbso.nbb.be/api"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/149.0.0.0 Safari/537.36"
    ),
    "Accept":         "application/json, text/plain, */*",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
}

PAGE_SIZE     = 50
REQUEST_DELAY = 0.3   # secondes entre deux requêtes — politesse serveur


def _session(numero: str) -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    s.headers["Referer"] = f"https://consult.cbso.nbb.be/consult-enterprise/{numero}"
    return s


def get_deposits(numero: str) -> list[dict]:
    """Pagine et retourne tous les dépôts bruts pour un numéro BCE."""
    session = _session(numero)
    all_deposits = []
    page = 0

    while True:
        url = f"{BASE}/rs-consult/published-deposits"
        params = {
            "page": page,
            "size": PAGE_SIZE,
            "enterpriseNumber": numero,
            "sort": "periodEndDate,desc",
        }
        try:
            r = session.get(url, params=params, timeout=20)
            r.raise_for_status()
        except Exception as e:
            log.warning(f"[{numero}] erreur API page {page} : {e}")
            break

        data = r.json()
        content = data.get("content", [])
        all_deposits.extend(content)

        total_pages = data.get("totalPages", 1)
        page += 1
        if page >= total_pages or not content:
            break
        time.sleep(REQUEST_DELAY)

    return all_deposits


def select_deposits(deposits: list[dict], max_years: int = 3) -> dict[int, dict]:
    """
    Filtre : exclut les comptes consolidés (modelId 'mc-*'),
    garde 1 dépôt par année (préférence FR > NL > autre),
    et limite aux `max_years` années les plus récentes disponibles.
    """
    selected: dict[int, dict] = {}
    for dep in deposits:
        year = dep.get("periodEndDateYear")
        model_id = dep.get("modelId", "")
        lang = dep.get("language", "")
        if not year or model_id.lower().startswith("mc"):
            continue
        if year not in selected or lang == "FR":
            selected[year] = dep

    top_years = sorted(selected.keys(), reverse=True)[:max_years]
    return {y: selected[y] for y in top_years}


def download_pdf(numero: str, deposit: dict) -> bytes | None:
    """Télécharge le PDF brut d'un dépôt (endpoint validé pdf/{id})."""
    session = _session(numero)
    url = f"{BASE}/external/broker/public/deposits/pdf/{deposit['id']}"
    try:
        r = session.get(url, timeout=30)
        if r.status_code == 200 and r.content:
            return r.content
        log.warning(f"[{numero}] PDF {deposit.get('periodEndDateYear')} → {r.status_code}")
    except Exception as e:
        log.warning(f"[{numero}] PDF erreur réseau : {e}")
    return None


def download_csv(numero: str, deposit: dict) -> bytes | None:
    """Télécharge le CSV brut d'un dépôt (disponible depuis 2021)."""
    year = deposit.get("periodEndDateYear", 0)
    if year < 2021:
        return None

    session = _session(numero)
    urls = [
        f"{BASE}/external/broker/public/deposits/consult/csv/{deposit['id']}",
        f"{BASE}/rs-consult/published-deposits/{deposit['id']}/file/csv",
    ]
    for url in urls:
        try:
            r = session.get(url, timeout=30)
            if r.status_code == 200 and r.content:
                return r.content
        except Exception as e:
            log.warning(f"[{numero}] CSV erreur réseau : {e}")
    log.warning(f"[{numero}] CSV {year} → introuvable")
    return None


def scrape_company(numero: str, max_years: int = 3) -> list[dict]:
    """
    Pipeline complet pour UNE entreprise :
    retourne une liste de documents bruts prêts à insérer en Mongo.

    Chaque élément :
        {
            "enterprise_number": str,
            "deposit_id": str,
            "year": int,
            "language": str,
            "model_id": str,
            "reference": str,
            "deposit_date": str,
            "doc_type": "pdf" | "csv",
            "content": bytes,
        }
    """
    documents = []
    deposits = get_deposits(numero)
    if not deposits:
        log.info(f"[{numero}] aucun dépôt trouvé")
        return documents

    selected = select_deposits(deposits, max_years=max_years)

    for year, dep in selected.items():
        meta = {
            "enterprise_number": numero,
            "deposit_id":        dep["id"],
            "year":               year,
            "language":           dep.get("language", ""),
            "model_id":           dep.get("modelId", ""),
            "reference":          dep.get("reference", ""),
            "deposit_date":       dep.get("depositDate", ""),
        }

        pdf_bytes = download_pdf(numero, dep)
        if pdf_bytes:
            documents.append({**meta, "doc_type": "pdf", "content": pdf_bytes})

        csv_bytes = download_csv(numero, dep)
        if csv_bytes:
            documents.append({**meta, "doc_type": "csv", "content": csv_bytes})

        time.sleep(REQUEST_DELAY)

    return documents
