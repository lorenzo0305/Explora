#!/usr/bin/env sh
set -eu

# --- Vars (avec défauts) ---
: "${MONGO_URI:=mongodb://mongo:27017/LORA_voyage}"
: "${MONGO_DB:=LORA_voyage}"
: "${MONGO_COL:=objects}"
: "${MONGO_MIN_DOCS:=1}"
: "${MONGO_WAIT_TIMEOUT:=900}"
: "${SLEEP_BETWEEN:=5}"
: "${ENRICH_PREFER:=og,openverse,wikimedia}"
: "${ENRICH_LIMIT:=0}"
: "${ES_HOST:=http://elasticsearch:9200}"

# ✅ Exporter pour que Python les voie
export MONGO_URI MONGO_DB MONGO_COL MONGO_MIN_DOCS MONGO_WAIT_TIMEOUT SLEEP_BETWEEN ENRICH_PREFER ENRICH_LIMIT ES_HOST

echo "🔎 Enrichisseur démarré
  MONGO_URI=$MONGO_URI
  MONGO_DB=$MONGO_DB
  MONGO_COL=$MONGO_COL
  MONGO_MIN_DOCS=$MONGO_MIN_DOCS
  ENRICH_PREFER=$ENRICH_PREFER
  ENRICH_LIMIT=$ENRICH_LIMIT
  ES_HOST=$ES_HOST"

# --- 1) Attendre Mongo + données seedées ---
python - <<'PY'
import os, sys, time
from pymongo import MongoClient

uri      = os.environ.get("MONGO_URI", "mongodb://mongo:27017/LORA_voyage")
dbn      = os.environ.get("MONGO_DB", "LORA_voyage")
col_name = os.environ.get("MONGO_COL", "objects")
min_docs = int(os.environ.get("MONGO_MIN_DOCS", "1"))
timeout  = int(os.environ.get("MONGO_WAIT_TIMEOUT", "900"))
sleep    = int(os.environ.get("SLEEP_BETWEEN", "5"))

t0 = time.time()
print(f"⏳ Attente Mongo et {min_docs}+ docs dans {dbn}.{col_name}…", flush=True)
while True:
    try:
        c = MongoClient(uri, serverSelectionTimeoutMS=2000)
        c.admin.command("ping")
        n = c[dbn][col_name].count_documents({})
        if n >= min_docs:
            print(f"✅ Mongo OK, docs={n}", flush=True)
            sys.exit(0)
        if time.time() - t0 > timeout:
            print(f"❌ Timeout: docs={n} (< {min_docs})", flush=True); sys.exit(1)
        print(f"…docs={n}, on attend", flush=True); time.sleep(sleep)
    except Exception as e:
        if time.time() - t0 > timeout:
            print(f"❌ Timeout Mongo: {e}", flush=True); sys.exit(1)
        print(f"…Mongo pas prêt: {e}", flush=True); time.sleep(sleep)
PY

# --- 2) Enrichissement images ---
echo "🖼️  Lancement image_enricher.py…"
python /app/webapp/image_enricher.py

# --- 3) Attendre ES puis réindexer (boucle curl, pas besoin de requests) ---
: "${ES_WAIT_TIMEOUT:=600}"  # 10 min par défaut
python - <<'PY'
import os, time, sys
import requests
es = os.environ.get("ES_HOST","http://elasticsearch:9200").rstrip("/")
timeout = int(os.environ.get("ES_WAIT_TIMEOUT","600"))
t0 = time.time()
print(f"⏳ Attente Elasticsearch {es}… (timeout {timeout}s)", flush=True)
while True:
    try:
        r = requests.get(es + "/_cluster/health", timeout=3)
        if r.ok:
            print("✅ Elasticsearch OK", flush=True)
            sys.exit(0)
    except Exception:
        pass
    if time.time()-t0 > timeout:
        print(f"❌ Timeout ES ({timeout}s)", flush=True); sys.exit(1)
    time.sleep(3)
PY

echo "📦 Réindexation ES via es_indexer.py…"
python /app/webapp/es_indexer.py