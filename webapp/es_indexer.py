# es_indexer.py — indexation HTTP (bulk) avec URL ES robuste et mapping complet
from __future__ import annotations
import os
import sys
import json
import time
import requests
from pymongo import MongoClient
from bson import ObjectId
from datetime import datetime, date

# -------------------- Helpers env --------------------
def getenv_any(keys: list[str], default: str | None = None) -> str | None:
    for k in keys:
        v = os.getenv(k)
        if v is not None:
            return v
    return default

# -------------------- Config Mongo --------------------
MONGO_URI = getenv_any(["MONGO_URI"], "mongodb://mongo:27017")
MONGO_DB = getenv_any(["MONGO_DB", "MONGO_DATABASE"], "LORA_voyage")
MONGO_COL = getenv_any(["MONGO_COLLECTION", "MONGO_COL"], "objects")

mongo_client = MongoClient(MONGO_URI)
mongo_db = mongo_client[MONGO_DB]
mongo_collection = mongo_db[MONGO_COL]

# -------------------- Config Elasticsearch --------------------
_raw_es = getenv_any(["ES_HOST", "ELASTICSEARCH_URI"], "http://elasticsearch:9200") or "http://elasticsearch:9200"
if not _raw_es.startswith(("http://", "https://")):
    _raw_es = "http://" + _raw_es
ES_BASE = _raw_es.rstrip("/")  # ex: http://elasticsearch:9200

INDEX_NAME = os.getenv("ES_INDEX", "objects")
BATCH_SIZE = int(os.getenv("ES_BULK_BATCH", "500"))
SESSION = requests.Session()

# -------------------- HTTP wrappers --------------------
def es_get(path: str, **kw):
    return SESSION.get(f"{ES_BASE}{path}", timeout=kw.pop("timeout", 10), **kw)

def es_head(path: str, **kw):
    return SESSION.head(f"{ES_BASE}{path}", timeout=kw.pop("timeout", 10), **kw)

def es_put(path: str, **kw):
    return SESSION.put(f"{ES_BASE}{path}", timeout=kw.pop("timeout", 15), **kw)

def es_post(path: str, **kw):
    return SESSION.post(f"{ES_BASE}{path}", timeout=kw.pop("timeout", 30), **kw)

# -------------------- Connexion ES --------------------
def test_es_connection() -> bool:
    try:
        r = es_get("/_cluster/health")
        r.raise_for_status()
        info = r.json()
        print(f"✅ Elasticsearch OK: {ES_BASE} | status={info.get('status')}")
        return True
    except Exception as e:
        print(f"❌ Connexion ES échouée: {e}")
        return False

# -------------------- Création index --------------------
def create_index_if_not_exists() -> bool:
    try:
        h = es_head(f"/{INDEX_NAME}")
        if h.status_code == 404:
            mapping = {
                "settings": {
                    "number_of_shards": 1,
                    "number_of_replicas": 0
                },
                "mappings": {
                    "properties": {
                        # champs génériques
                        "name": {"type": "text"},
                        "description": {"type": "text"},
                        "isLocatedAt": {"type": "object"},
                        # images enrichies
                        "image": {"type": "keyword", "ignore_above": 2048},
                        "image_attribution": {
                            "type": "object",
                            "properties": {
                                "origin": {"type": "keyword"},
                                "source": {"type": "keyword"},
                                "creator": {"type": "text"},
                                "license": {"type": "keyword"},
                                "license_url": {"type": "keyword"}
                            }
                        },
                        # quelques champs datatourisme fréquents (souples)
                        "@id": {"type": "keyword", "ignore_above": 4096},
                        "@type": {"type": "keyword"},
                        "rdfs:label": {"type": "object"},
                        "hasDescription": {"type": "object"},
                        "dc:description": {"type": "object"},
                        "schema:description": {"type": "object"},
                        "rdfs:comment": {"type": "object"},
                        "shortDescription": {"type": "object"},
                        "longDescription": {"type": "object"},
                    }
                }
            }
            r = es_put(f"/{INDEX_NAME}", json=mapping)
            if r.status_code in (200, 201):
                print(f"✅ Index '{INDEX_NAME}' créé avec mapping")
                return True
            else:
                print(f"❌ Erreur création index: {r.status_code} - {r.text}")
                return False
        elif h.status_code in (200, 201):
            print(f"✅ Index '{INDEX_NAME}' existe déjà")
            return True
        else:
            print(f"❌ HEAD index renvoie {h.status_code}: {h.text}")
            return False
    except Exception as e:
        print(f"❌ Erreur index: {e}")
        return False

# -------------------- Serialization --------------------
def make_serializable(obj):
    if isinstance(obj, dict):
        out = {}
        for k, v in obj.items():
            if k == "_id":
                # on évite de garder _id Mongo dans _source (l'_id ES est déjà utilisé)
                continue
            out[k] = make_serializable(v)
        return out
    if isinstance(obj, list):
        return [make_serializable(v) for v in obj]
    if isinstance(obj, ObjectId):
        return str(obj)
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    return obj

# -------------------- Bulk index --------------------
def bulk_index(pairs: list[tuple[str, dict]]) -> bool:
    """
    pairs: liste de (doc_id, doc_dict)
    """
    try:
        lines = []
        for _id, doc in pairs:
            meta = {"index": {"_index": INDEX_NAME, "_id": _id}}
            lines.append(json.dumps(meta, ensure_ascii=False))
            lines.append(json.dumps(doc, ensure_ascii=False))
        ndjson = "\n".join(lines) + "\n"
        r = es_post("/_bulk", data=ndjson.encode("utf-8"),
                    headers={"Content-Type": "application/x-ndjson"})
        if r.status_code not in (200, 201):
            print(f"❌ Erreur bulk HTTP {r.status_code}: {r.text[:500]}")
            return False
        payload = r.json()
        if payload.get("errors"):
            # Afficher quelques erreurs pour debug sans spammer
            errs = [it for it in payload.get("items", []) if it.get("index", {}).get("error")]
            print(f"⚠️ Bulk avec erreurs: {len(errs)} / {len(payload.get('items', []))}")
            if errs:
                sample = errs[0]["index"]["error"]
                print(f"  ex: {sample}")
        return True
    except Exception as e:
        print(f"❌ Exception bulk: {e}")
        return False

# -------------------- Main --------------------
if __name__ == "__main__":
    t0 = time.time()

    if not test_es_connection():
        print("❌ Impossible de se connecter à Elasticsearch")
        sys.exit(1)

    if not create_index_if_not_exists():
        print("❌ Impossible de créer/vérifier l'index")
        sys.exit(1)

    total = mongo_collection.count_documents({})
    print(f"📊 Mongo: {MONGO_DB}.{MONGO_COL} | docs={total}")
    if total == 0:
        print("⚠️ Aucun document dans Mongo, rien à indexer.")
        sys.exit(0)

    success = 0
    errors = 0
    batch: list[tuple[str, dict]] = []

    # Astuce: ne projeter que ce qui est utile si besoin; ici on prend tout
    cursor = mongo_collection.find({}, no_cursor_timeout=True)
    try:
        for doc in cursor:
            # _id ES préféré: @id si présent (URL datatourisme), sinon _id Mongo
            doc_id = doc.get("@id") or str(doc.get("_id"))
            payload = make_serializable(doc)
            batch.append((doc_id, payload))

            if len(batch) >= BATCH_SIZE:
                ok = bulk_index(batch)
                if ok:
                    success += len(batch)
                    print(f"📝 {success}/{total} indexés...")
                else:
                    errors += len(batch)
                batch.clear()
        # flush final
        if batch:
            ok = bulk_index(batch)
            if ok:
                success += len(batch)
            else:
                errors += len(batch)
    finally:
        cursor.close()

    dt = time.time() - t0
    print(f"✅ Fini: {success} succès, {errors} erreurs | {dt:.1f}s")
    sys.exit(0 if errors == 0 else 2)
