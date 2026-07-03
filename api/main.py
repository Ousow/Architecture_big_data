"""
main.py — API FastAPI, Jour 3 Partie 2 (étape 1/2)
──────────────────────────────────────────────────
Deux endpoints pour l'instant :

  GET /search?q=...        Recherche par numéro BCE ou nom d'entreprise
  GET /entreprise/{numero} Fiche complète : Silver (identité, adresse,
                            activités) + Gold (ratios financiers par année)

Lancé via docker-compose (service "api"), connexion Mongo par nom de
service ("mongo"), pas besoin de build custom (image python:3.11-slim +
volume monté + pip install au démarrage).
"""

import os

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pymongo import MongoClient

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
DB_NAME   = os.environ.get("MONGO_DB", "kbo_db")

app = FastAPI(title="KBO Hôtellerie API")

# CORS ouvert pour le futur frontend React (à restreindre en prod)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

client = MongoClient(MONGO_URI)
db     = client[DB_NAME]


def clean_doc(doc: dict | None) -> dict | None:
    """Retire les champs internes non destinés à l'API (ex: ObjectId Mongo)."""
    if doc is None:
        return None
    doc.pop("_id", None)
    return doc


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/search")
def search(q: str = Query(..., min_length=2, description="Numéro BCE ou nom d'entreprise")):
    """
    Recherche sur enterprise_silver : match exact/partiel sur le numéro BCE,
    ou recherche insensible à la casse sur les dénominations.
    Retourne une liste allégée (pas la fiche complète).
    """
    query = {
        "$or": [
            {"enterprise_number": {"$regex": q}},
            {"denominations.Denomination": {"$regex": q, "$options": "i"}},
        ]
    }
    projection = {
        "enterprise_number":   1,
        "denominations":       1,
        "addresses":           1,
        "status_label":        1,
        "juridical_form_label": 1,
    }

    results = []
    for doc in db.enterprise_silver.find(query, projection).limit(20):
        denomination = None
        if doc.get("denominations"):
            denomination = doc["denominations"][0].get("Denomination")

        commune = None
        if doc.get("addresses"):
            commune = doc["addresses"][0].get("MunicipalityFR") or doc["addresses"][0].get("MunicipalityNL")

        results.append({
            "enterprise_number":    doc["enterprise_number"],
            "denomination":         denomination,
            "commune":              commune,
            "status_label":         doc.get("status_label"),
            "juridical_form_label": doc.get("juridical_form_label"),
        })

    return {"query": q, "count": len(results), "results": results}


@app.get("/entreprise/{numero}")
def get_entreprise(numero: str):
    """
    Fiche complète d'une entreprise : identité/activités/adresse (Silver)
    fusionnées avec les ratios financiers par année (Gold, si disponible —
    toutes les entreprises Silver ne sont pas dans le périmètre hôtellier
    scrapé, donc `financials` peut être absent).
    """
    silver = clean_doc(db.enterprise_silver.find_one({"_id": numero}))
    if silver is None:
        raise HTTPException(status_code=404, detail=f"Entreprise {numero} introuvable")

    gold = clean_doc(db.hotel_gold.find_one({"_id": numero}))

    return {
        "identity": {
            "enterprise_number":    silver.get("enterprise_number"),
            "status":               silver.get("status"),
            "status_label":         silver.get("status_label"),
            "juridical_form":       silver.get("juridical_form"),
            "juridical_form_label": silver.get("juridical_form_label"),
            "start_date":           silver.get("start_date"),
            "denominations":        silver.get("denominations", []),
            "addresses":            silver.get("addresses", []),
            "activities":           silver.get("activities", []),
            "establishments":       silver.get("establishments", []),
        },
        "financials": {
            "available":   gold is not None,
            "schema_type": gold.get("schema_type") if gold else None,
            "years":       gold.get("years", []) if gold else [],
        },
    }
