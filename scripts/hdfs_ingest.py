"""
hdfs_ingest.py
────────────────
Upload de fichiers bruts (PDF, CSV scrapés, et tout autre fichier brut)
vers HDFS via WebHDFS REST API.

Contrairement au contournement docker-exec utilisé en manuel sous Windows,
ici Airflow tourne DANS le même réseau Docker que namenode/datanode
(défini par le docker-compose.yml du pipeline), donc le redirect WebHDFS
vers le DataNode fonctionne nativement — pas de problème de hostname.
"""

import logging
import os

from hdfs import InsecureClient

log = logging.getLogger(__name__)

HDFS_URL  = os.environ.get("HDFS_URL", "http://namenode:9870")
HDFS_USER = os.environ.get("HDFS_USER", "root")


def get_hdfs_client() -> InsecureClient:
    return InsecureClient(HDFS_URL, user=HDFS_USER)


def upload_bytes(client: InsecureClient, content: bytes, hdfs_path: str) -> bool:
    """
    Upload un contenu binaire brut vers un chemin HDFS donné.
    Crée les dossiers parents automatiquement, écrase si déjà présent.
    """
    try:
        with client.write(hdfs_path, overwrite=True) as writer:
            writer.write(content)
        log.info(f"HDFS OK : {hdfs_path} ({len(content)} octets)")
        return True
    except Exception as e:
        log.warning(f"HDFS ERROR {hdfs_path} : {e}")
        return False


def hdfs_path_exists(client: InsecureClient, hdfs_path: str) -> bool:
    try:
        return client.status(hdfs_path, strict=False) is not None
    except Exception:
        return False
