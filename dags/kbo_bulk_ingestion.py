"""
DAG : kbo_bulk_ingestion 

— limite Airflow de dynamic task mapping : Airflow
     refuse tout XCom mappé contenant plus de 1024 éléments
     (UnmappableXComLengthPushed). Avec BATCH_SIZE=50 entreprises par
     micro-batch, un run de 500 000 entreprises produit 10 000
     micro-batches — bien au-delà de cette limite, peu importe la
     taille en octets de la liste retournée (testé et confirmé : la
     limite porte sur le NOMBRE d'éléments, pas sur les octets).

     Solution : un niveau de GROUPEMENT. On regroupe les micro-batches
     en "groupes" de GROUP_SIZE micro-batches chacun. Le DAG ne mappe
     dynamiquement que sur les indices de GROUPE (toujours < 1024),
     et chaque tâche de groupe boucle EN INTERNE sur ses micro-batches
     (donc le checkpoint reste aussi fin qu'avant, juste l'orchestration
     Airflow est groupée).
"""

from __future__ import annotations

import logging
import sys
from datetime import datetime

from airflow.decorators import dag, task

sys.path.insert(0, "/opt/airflow/scripts")

log = logging.getLogger(__name__)

BATCH_SIZE        = 50      # entreprises par micro-batch (granularité du checkpoint)
GROUP_SIZE        = 100     # micro-batches par groupe (granularité du dynamic mapping Airflow)
MAX_YEARS         = 3
ENTERPRISE_LIMIT  = 500_000   # taille du lot traité par ce run — ajuster pour les lots suivants


@dag(
    dag_id="kbo_bulk_ingestion",
    description="Lit les entreprises depuis MongoDB, scrape NBB+StaPor, dépose CSV+PDF en HDFS Bronze, suit l'état dans State DB",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["kbo", "nbb", "stapor", "hdfs", "mongodb"],
)
def kbo_bulk_ingestion():

    @task
    def load_groups_from_mongo() -> list[int]:
        """
        Calcule le nombre total de GROUPES (pas de micro-batches) à
        traiter, et retourne une simple liste d'index de groupe.

        Avec ENTERPRISE_LIMIT=500000, BATCH_SIZE=50, GROUP_SIZE=100 :
        500000 / 50 = 10000 micro-batches / 100 = 100 groupes.
        100 < 1024 → respecte la limite Airflow de dynamic task mapping.
        """
        from mongo_ingest import get_db, ensure_indexes, get_enterprise_count

        db = get_db()
        ensure_indexes(db)

        total = get_enterprise_count(db, status="AC")
        total = min(total, ENTERPRISE_LIMIT)
        log.info(f"{total} entreprises actives à traiter depuis MongoDB (enterprises_rich)")

        if total == 0:
            raise ValueError(
                "Aucune entreprise trouvée dans enterprises_rich — "
                "lance d'abord import_kbo_enriched.py pour peupler MongoDB."
            )

        total_micro_batches = (total + BATCH_SIZE - 1) // BATCH_SIZE
        total_groups = (total_micro_batches + GROUP_SIZE - 1) // GROUP_SIZE

        log.info(f"Découpage : {total_micro_batches} micro-batches de {BATCH_SIZE} entreprises")
        log.info(f"Regroupés en : {total_groups} groupes de {GROUP_SIZE} micro-batches")

        if total_groups > 1024:
            raise ValueError(
                f"{total_groups} groupes dépasse la limite Airflow de 1024 pour le "
                f"dynamic task mapping. Augmente GROUP_SIZE (actuellement {GROUP_SIZE})."
            )

        return list(range(total_groups))

    @task(max_active_tis_per_dag=8)
    def process_group(group_index: int) -> dict:
        """
        Traite TOUS les micro-batches d'un groupe (jusqu'à GROUP_SIZE
        micro-batches de BATCH_SIZE entreprises chacun), en bouclant
        en interne. Le checkpoint reste au niveau micro-batch — un
        micro-batch déjà terminé dans un run précédent est skippé
        individuellement, même si le groupe entier est relancé.
        """
        from nbb_scraper import scrape_company
        from mongo_ingest import (
            get_db, ensure_indexes, update_state, is_already_done,
            log_company_run, is_batch_completed, mark_batch_started,
            mark_batch_completed, get_enterprise_numbers,
        )
        from hdfs_ingest import get_hdfs_client, upload_bytes

        db = get_db()
        ensure_indexes(db)
        hdfs_client = get_hdfs_client()

        group_summary = {
            "group_index":            group_index,
            "micro_batches_done":     0,
            "micro_batches_skipped":  0,
            "companies":              0,
            "nbb_csv":                0,
            "nbb_pdf":                0,
            "skipped":                0,
            "doc_errors":             0,
            "company_errors":         0,
        }

        first_micro_index = group_index * GROUP_SIZE
        last_micro_index  = first_micro_index + GROUP_SIZE  # exclusif

        for micro_index in range(first_micro_index, last_micro_index):
            batch_id = f"run_{micro_index}"
            offset   = micro_index * BATCH_SIZE

            if is_batch_completed(db, batch_id):
                group_summary["micro_batches_skipped"] += 1
                continue

            companies = get_enterprise_numbers(db, status="AC", offset=offset, limit=BATCH_SIZE)
            if not companies:
                # offset au-delà du total réel — dernier groupe partiel
                continue

            mark_batch_started(db, batch_id)

            for numero in companies:
                try:
                    documents = scrape_company(numero, max_years=MAX_YEARS)

                    if not documents:
                        log_company_run(db, numero, source="nbb", status="no_data")
                    else:
                        for doc in documents:
                            year     = doc["year"]
                            doc_type = doc["doc_type"]

                            if is_already_done(db, numero, "nbb", year, doc_type):
                                group_summary["skipped"] += 1
                                continue

                            hdfs_path = f"/data/bronze/nbb/{doc_type}/{numero}/{year}.{doc_type}"
                            ok = upload_bytes(hdfs_client, doc["content"], hdfs_path)

                            if ok:
                                update_state(
                                    db, numero, source="nbb", year=year, doc_type=doc_type,
                                    status="done", hdfs_path=hdfs_path,
                                    deposit_id=doc.get("deposit_id"),
                                )
                                if doc_type == "csv":
                                    group_summary["nbb_csv"] += 1
                                else:
                                    group_summary["nbb_pdf"] += 1
                            else:
                                update_state(
                                    db, numero, source="nbb", year=year, doc_type=doc_type,
                                    status="error", error="hdfs upload échoué",
                                )
                                group_summary["doc_errors"] += 1

                        log_company_run(db, numero, source="nbb", status="ok")

                    group_summary["companies"] += 1

                except Exception as e:
                    log.error(f"[{numero}] erreur : {e}")
                    log_company_run(db, numero, source="nbb", status="error", error=str(e))
                    group_summary["company_errors"] += 1

            mark_batch_completed(db, batch_id, {"companies": len(companies)})
            group_summary["micro_batches_done"] += 1

        log.info(f"Groupe {group_index} terminé : {group_summary}")
        return group_summary

    @task
    def summarize(group_summaries: list[dict]) -> None:
        total = {
            "companies": 0, "nbb_csv": 0, "nbb_pdf": 0,
            "skipped": 0, "doc_errors": 0, "company_errors": 0,
            "micro_batches_done": 0, "micro_batches_skipped": 0,
        }

        for s in group_summaries:
            for k in total:
                total[k] += s.get(k, 0)

        log.info("=" * 60)
        log.info("RÉSUMÉ FINAL")
        log.info("=" * 60)
        log.info(f"  Micro-batches traités             : {total['micro_batches_done']}")
        log.info(f"  Micro-batches skippés (checkpoint) : {total['micro_batches_skipped']}")
        log.info(f"  Entreprises traitées               : {total['companies']}")
        log.info(f"  CSV NBB → HDFS                     : {total['nbb_csv']}")
        log.info(f"  PDF NBB → HDFS                     : {total['nbb_pdf']}")
        log.info(f"  Documents déjà présents             : {total['skipped']}")
        log.info(f"  Erreurs document                    : {total['doc_errors']}")
        log.info(f"  Erreurs entreprise                  : {total['company_errors']}")

    groups  = load_groups_from_mongo()
    results = process_group.expand(group_index=groups)
    summarize(results)


kbo_bulk_ingestion()
