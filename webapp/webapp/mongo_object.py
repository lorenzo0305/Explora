# webapp/mongo_object.py
from pathlib import Path, PurePosixPath
from pymongo import MongoClient
import os
import json
import sys
import unicodedata
from typing import Iterable, Optional

# -------------------- Config --------------------
DATA_ROOT = Path(os.environ.get("DATA_ROOT", "/app/data")).resolve()
OBJECTS_BASE = DATA_ROOT / "full_france_object" / "objects"

REGION_FILES_ENV = os.environ.get("REGION_FILES", "Hauts-de-France.json")
REGION_FILES = [x.strip() for x in REGION_FILES_ENV.split(",") if x.strip()]

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://mongo:27017/LORA_voyage")
MONGO_DB = os.environ.get("MONGO_DB", "LORA_voyage")
MONGO_COL = os.environ.get("MONGO_COLLECTION", "objects")

print(f"DATA_ROOT = {DATA_ROOT}")
print(f"OBJECTS_BASE = {OBJECTS_BASE}")
print(f"REGION_FILES = {REGION_FILES}")
print(f"MONGO_URI = {MONGO_URI} DB={MONGO_DB} COL={MONGO_COL}")

# -------------------- Helpers --------------------
def _slugify(s: str) -> str:
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode()
    s = s.replace("_", "-").replace(" ", "-")
    while "--" in s:
        s = s.replace("--", "-")
    return s.lower()

def _find_region_file(name_or_filename: str) -> Optional[Path]:
    candidates = [
        DATA_ROOT / name_or_filename,
        DATA_ROOT / f"{name_or_filename}.json",
        DATA_ROOT / f"{_slugify(name_or_filename)}.json",
        DATA_ROOT / "regions" / name_or_filename,
        DATA_ROOT / "regions" / f"{name_or_filename}.json",
        DATA_ROOT / "regions" / f"{_slugify(name_or_filename)}.json",
    ]
    for p in candidates:
        if p.exists():
            return p
    target = _slugify(name_or_filename.replace(".json", ""))
    for p in DATA_ROOT.rglob("*.json"):
        try:
            if len(p.relative_to(DATA_ROOT).parts) > 3:
                continue
        except Exception:
            pass
        if _slugify(p.stem) == target:
            return p
    return None

def _safe_join_objects(rel_path: str) -> Optional[Path]:
    parts = [seg for seg in PurePosixPath(rel_path).parts if seg not in (".", "..", "")]
    candidate = (OBJECTS_BASE.joinpath(*parts)).resolve()
    try:
        candidate.relative_to(OBJECTS_BASE)
    except Exception:
        return None
    return candidate

def _load_json(path: Path):
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)

def _iter_region_entries(region_json_path: Path) -> Iterable[dict]:
    data = _load_json(region_json_path)
    if isinstance(data, list):
        for e in data:
            if isinstance(e, dict):
                yield e
    elif isinstance(data, dict) and isinstance(data.get("items"), list):
        for e in data["items"]:
            if isinstance(e, dict):
                yield e
    else:
        print(f"⚠️ Format inattendu pour {region_json_path.name} — ignoré.")

# -------------------- Insertion Mongo (upsert ciblé) --------------------
def upsert_needed_objects():
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    col = db[MONGO_COL]
    print("✅ Connecté à MongoDB, insertion ciblée…")

    total_wanted = 0
    total_found_files = 0
    total_upserts = 0
    total_missing = 0

    for rf in REGION_FILES:
        region_file = _find_region_file(rf)
        if not region_file:
            print(f"❌ Région '{rf}' introuvable sous {DATA_ROOT}.")
            continue

        print(f"📄 Région: {region_file}")
        region_name_from_file = region_file.stem  # ex: "Auvergne-Rhône-Alpes"

        for e in _iter_region_entries(region_file):
            total_wanted += 1

            rel_file = e.get("file") or e.get("path")
            identifier = e.get("identifier") or e.get("id") or e.get("dc:identifier")

            if not rel_file:
                total_missing += 1
                continue

            obj_path = _safe_join_objects(rel_file)
            if not obj_path or not obj_path.exists():
                total_missing += 1
                continue

            total_found_files += 1

            try:
                obj = _load_json(obj_path)
            except Exception as ex:
                print(f"⚠️ JSON invalide: {obj_path} ({ex})")
                total_missing += 1
                continue

            # --- Normalisation des identifiants / champs utiles ---
            doc_id = obj.get("@id") or identifier
            if not doc_id:
                total_missing += 1
                continue
            obj["@id"] = doc_id  # garantit @id dans le document
            if "identifier" not in obj and identifier:
                obj["identifier"] = identifier

            # Champ plat 'region' pour les filtres simples (attach photos, etc.)
            region_flat = obj.get("region") or e.get("region") or region_name_from_file
            obj["region"] = region_flat

            # Tag multi-régions (historique des sources)
            tags = set(obj.get("_regions", []))
            tags.add(region_name_from_file)
            obj["_regions"] = sorted(tags)

            # Upsert par @id
            res = col.update_one(
                {"@id": doc_id},
                {"$set": obj, "$setOnInsert": {"_ingestedFrom": "region_filter"}},
                upsert=True
            )
            if res.upserted_id is not None or res.modified_count > 0:
                total_upserts += 1

    print(f"--- Résumé ---")
    print(f"Demandés (entrées région): {total_wanted}")
    print(f"Fichiers trouvés:         {total_found_files}")
    print(f"Upserts effectués:         {total_upserts}")
    print(f"Manquants/ignorés:         {total_missing}")

if __name__ == "__main__":
    try:
        upsert_needed_objects()
        print("✅ Import ciblé terminé.")
        sys.exit(0)
    except Exception as e:
        print(f"❌ Échec import ciblé: {e}")
        sys.exit(1)
