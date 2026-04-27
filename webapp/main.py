from __future__ import annotations

import os
import re
import time
import urllib.parse
from datetime import datetime
from pathlib import Path
from typing import Any

import certifi
from bson import ObjectId
from fastapi import Body, FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pymongo import MongoClient

# =============================================================
# CONFIG APP
# =============================================================
BASE_DIR = Path(__file__).resolve().parent

app = FastAPI(title="Explora API")
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

# =============================================================
# MONGODB ATLAS (Fusion de la connexion distante)
# =============================================================
user = "equipe_explora"
password = "2BqXsiNi8nCCE@W"
safe_password = urllib.parse.quote_plus(password)
uri = f"mongodb+srv://{user}:{safe_password}@datas.xc1dpyu.mongodb.net/?appName=datas"

mongo_client = MongoClient(uri, tlsCAFile=certifi.where())
# On utilise la base de données "explora" (issue de votre Snippet 1)
db = mongo_client["explora"]

# Si vos données sont dispersées par région, la recherche globale tape ici par défaut.
# J'ai mis "Auvergne" comme collection par défaut comme indiqué dans votre Snippet 1.
DEFAULT_COLLECTION = os.getenv("MONGO_COLLECTION", "Auvergne")
objects_col = db[DEFAULT_COLLECTION]
journeys_col = db["journeys"]

# =============================================================
# UTILS (Les versions robustes de votre site complet)
# =============================================================
def _json_safe(value: Any) -> Any:
    """Convert Mongo/NumPy/pandas/datetime values into JSON-safe primitives."""
    if value is None or isinstance(value, (str, bool, int, float)):
        if isinstance(value, float):
            try:
                import math
                if math.isnan(value) or math.isinf(value):
                    return None
            except Exception:
                pass
        return value
    if isinstance(value, ObjectId):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items() if k != "_id"}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(v) for v in value]
    
    # Try parsing NumPy/Pandas if they exist
    try:
        import numpy as np
        if isinstance(value, (np.integer,)): return int(value)
        if isinstance(value, (np.floating,)):
            f = float(value)
            import math
            return None if (math.isnan(f) or math.isinf(f)) else f
        if isinstance(value, (np.bool_,)): return bool(value)
        if isinstance(value, (np.ndarray,)): return [_json_safe(v) for v in value.tolist()]
    except Exception:
        pass
    try:
        import pandas as pd
        if isinstance(value, pd.Timestamp): return value.isoformat()
        if value is pd.NaT: return None
    except Exception:
        pass
    
    return str(value)

def _clean_mongo(value: Any) -> Any:
    if isinstance(value, dict):
        return {k: _clean_mongo(v) for k, v in value.items() if k != "_id"}
    if isinstance(value, list):
        return [_clean_mongo(v) for v in value]
    if isinstance(value, datetime):
        return value.isoformat()
    return value

def _normalize_region(value: str) -> str:
    s = value.lower()
    key = re.sub(r"[^a-z0-9]+", "-", s).strip("-")
    if key in ("hauts-de-france", "haut-de-france", "hdf"): return "hauts-de-france"
    if key in ("auvergne-rhone-alpes", "auvergne", "ara"): return "auvergne-rhone-alpes"
    return key

def _region_variants(region: str) -> list[str]:
    key = _normalize_region(region)
    if key == "hauts-de-france": return ["Hauts-de-France", "hauts-de-france", "Hauts de France"]
    if key == "auvergne-rhone-alpes": return ["Auvergne-Rhone-Alpes", "Auvergne-Rhône-Alpes", "auvergne-rhone-alpes"]
    return [region]

def _pick_name(doc: dict[str, Any]) -> str:
    label = doc.get("rdfs:label")
    if isinstance(label, dict):
        fr = label.get("fr")
        if isinstance(fr, list) and fr: return str(fr[0])
        if isinstance(fr, str): return fr
    for key in ("label", "nom", "name"):
        value = doc.get(key)
        if isinstance(value, str) and value.strip(): return value
    return "Sans nom"

def _pick_image(doc: dict[str, Any]) -> str:
    for key in ("image", "photo", "picture", "thumbnail", "cover"):
        value = doc.get(key)
        if isinstance(value, str) and value.strip(): return value
    reps = doc.get("https://www.datatourisme.fr/ontology/core#hasMainRepresentation")
    if isinstance(reps, list):
        for rep in reps:
            if not isinstance(rep, dict): continue
            resources = rep.get("ebucore:hasRelatedResource")
            if not isinstance(resources, list): continue
            for r in resources:
                if not isinstance(r, dict): continue
                locators = r.get("ebucore:locator")
                if isinstance(locators, list):
                    for loc in locators:
                        if isinstance(loc, str) and loc.strip(): return loc
    return "/static/img/no-image.jpg"

def _pick_locality(doc: dict[str, Any]) -> str:
    is_located = doc.get("isLocatedAt")
    if not isinstance(is_located, list) or not is_located: return ""
    first = is_located[0]
    if not isinstance(first, dict): return ""
    addresses = first.get("schema:address")
    if not isinstance(addresses, list) or not addresses: return ""
    addr = addresses[0]
    if not isinstance(addr, dict): return ""
    locality = addr.get("schema:addressLocality")
    return locality if isinstance(locality, str) else ""

def _pick_types(doc: dict[str, Any]) -> list[str]:
    types = doc.get("@type", [])
    if isinstance(types, list): return [str(t) for t in types]
    if isinstance(types, str): return [types]
    return []

def _public_object(doc: dict[str, Any]) -> dict[str, Any]:
    object_id = str(doc.get("@id") or doc.get("identifier") or doc.get("_id") or "")
    return {
        "id": object_id,
        "name": _pick_name(doc),
        "image": _pick_image(doc),
        "locality": _pick_locality(doc),
        "region": doc.get("region", ""),
        "types": _pick_types(doc),
        "categories": doc.get("categories", doc.get("category", [])),
    }

def _get_object_by_id(object_id: str) -> dict[str, Any] | None:
    doc = objects_col.find_one({"@id": object_id})
    if doc: return doc
    doc = objects_col.find_one({"identifier": object_id})
    if doc: return doc
    return None

def _get_descriptions(doc: dict[str, Any]) -> list[str]:
    descs: list[str] = []
    comments = doc.get("rdfs:comment")
    if isinstance(comments, dict):
        fr = comments.get("fr")
        if isinstance(fr, list): descs.extend([str(x) for x in fr if isinstance(x, str)])
        elif isinstance(fr, str): descs.append(fr)

    has_desc = doc.get("hasDescription")
    if isinstance(has_desc, list):
        for item in has_desc:
            if not isinstance(item, dict): continue
            short_desc = item.get("shortDescription")
            if isinstance(short_desc, dict):
                fr = short_desc.get("fr")
                if isinstance(fr, list): descs.extend([str(x) for x in fr if isinstance(x, str)])
                elif isinstance(fr, str): descs.append(fr)

    clean = []
    seen = set()
    for d in descs:
        text = d.strip()
        if text and text not in seen:
            seen.add(text)
            clean.append(text)
    return clean

def _journey_id(payload: dict[str, Any]) -> str:
    given = payload.get("id")
    if isinstance(given, str) and given.strip(): return given.strip()
    return f"j_{int(time.time() * 1000)}"

def _sanitize_mongo_keys(value: Any) -> Any:
    if isinstance(value, dict):
        clean = {}
        for k, v in value.items():
            key = str(k).replace('.', '_')
            if key.startswith('$'): key = '_' + key[1:]
            clean[key] = _sanitize_mongo_keys(v)
        return clean
    if isinstance(value, list):
        return [_sanitize_mongo_keys(v) for v in value]
    return value

def _journey_doc(payload: dict[str, Any], journey_id: str) -> dict[str, Any]:
    doc = _sanitize_mongo_keys(_json_safe(payload))
    if not isinstance(doc, dict): doc = {}
    doc["id"] = journey_id
    doc["updatedAt"] = int(time.time() * 1000)
    return doc

# =============================================================
# ROUTES FRONT (Toutes les pages de Snippet 2)
# =============================================================
@app.get("/", response_class=HTMLResponse)
@app.get("/accueil", response_class=HTMLResponse)
def home(request: Request):
    return templates.TemplateResponse(request=request, name="Accueil.html")

@app.get("/exploration", response_class=HTMLResponse)
@app.get("/Destinations", response_class=HTMLResponse)
def exploration(request: Request):
    return templates.TemplateResponse(request=request, name="Catalogue.html")

@app.get("/region/{region_slug}", response_class=HTMLResponse)
def region_page(request: Request, region_slug: str):
    return templates.TemplateResponse(request=request, name="Region.html", context={"region": region_slug})

@app.get("/carnet", response_class=HTMLResponse)
@app.get("/topics", response_class=HTMLResponse)
def carnet(request: Request):
    return templates.TemplateResponse(request=request, name="MesVoyages.html")

@app.get("/editeur", response_class=HTMLResponse)
def editeur_page(request: Request):
    return templates.TemplateResponse(request=request, name="Editeur.html")

@app.get("/creation", response_class=HTMLResponse)
@app.get("/makejourney", response_class=HTMLResponse)
@app.get("/makejourney/new", response_class=HTMLResponse)
@app.get("/makejourney/{journey_id}", response_class=HTMLResponse)
def creation(request: Request, journey_id: str | None = None):
    return templates.TemplateResponse(request=request, name="Creation.html", context={"journey_id": journey_id or ""})

@app.get("/voyage", response_class=HTMLResponse)
@app.get("/Voyage.html", response_class=HTMLResponse)
def voyage_page(request: Request):
    return templates.TemplateResponse(request=request, name="Voyage.html")

@app.get("/journeys/view/{journey_id}", response_class=HTMLResponse)
def journey_view(request: Request, journey_id: str):
    return templates.TemplateResponse(request=request, name="ViewJourney.html", context={"journey_id": journey_id})

@app.get("/journeys/view/{journey_id}/day/{day_index}", response_class=HTMLResponse)
def journey_day_view(request: Request, journey_id: str, day_index: int):
    return templates.TemplateResponse(request=request, name="ViewJourneyDay.html", context={"journey_id": journey_id, "day": day_index})

@app.get("/object/{object_id:path}", response_class=HTMLResponse)
@app.get("/detail-act-perso/{object_id:path}", response_class=HTMLResponse)
def object_page(request: Request, object_id: str):
    doc = _get_object_by_id(object_id)
    if not doc:
        raise HTTPException(status_code=404, detail="Objet introuvable")
    context = {
        "request": request,
        "name": _pick_name(doc),
        "image": _pick_image(doc),
        "descriptions": _get_descriptions(doc),
    }
    return templates.TemplateResponse(request=request, name="DetailActPerso.html", context=context)

# =============================================================
# API ACTIVITÉS (DYNAMIQUE PAR COLLECTION - Snippet 1)
# =============================================================
@app.get("/api/activites/{region}/{categorie}")
def get_activites(region: str, categorie: str):
    TRADUCTIONS_CATEGORIES = {
        "patrimoine": "culture",
        "shopping": "boutique",
        "détente": "détente",
        "sport": "sport",
        "nature": "nature",
        "gastronomie": "gastronomie"
    }

    cat_base = TRADUCTIONS_CATEGORIES.get(categorie.lower(), categorie.lower())
    target_collection = db[region]
    query_cat = re.compile(f"^{cat_base}$", re.IGNORECASE)

    print(f"--- DEBUG RECHERCHE ---")
    print(f"Collection : {region} | Catégorie : {cat_base}")

    cursor = target_collection.find({"categories": query_cat}).limit(50)
    return [_public_object(doc) for doc in cursor]

# =============================================================
# RECHERCHES & API GLOBALES (Snippet 2)
# =============================================================
@app.get("/objects/{object_id:path}")
@app.get("/api/objects/{object_id:path}")
@app.get("/api/object/{object_id:path}")
def object_api(object_id: str):
    doc = _get_object_by_id(object_id)
    if not doc: raise HTTPException(status_code=404, detail="Objet introuvable")
    return JSONResponse(_clean_mongo(doc))

@app.get("/search")
def search(query: str = Query("", min_length=0), offset: int = Query(0, ge=0), limit: int = Query(30, ge=1, le=100)):
    q = query.strip()
    if len(q) < 1: return []
    regex = re.compile(re.escape(q), re.IGNORECASE)
    mongo_query = {
        "$or": [
            {"label": regex},
            {"nom": regex},
            {"name": regex},
            {"rdfs:label.fr": regex},
        ]
    }
    # Note : Cherche dans la collection par défaut ("Auvergne")
    cursor = objects_col.find(mongo_query).skip(offset).limit(limit)
    return [_public_object(doc) for doc in cursor]

@app.get("/regions/{region_slug}/cards")
def region_cards(region_slug: str, type: str | None = None, q: str | None = None, limit: int = Query(24, ge=1, le=120)):
    variants = _region_variants(region_slug)
    # Dans une architecture multi-collections, on tape directement dans la bonne collection :
    target_collection = db[region_slug.capitalize()] if region_slug.capitalize() in db.list_collection_names() else objects_col
    
    mongo_query: dict[str, Any] = {"$or": [{"region": {"$in": variants}}, {"_regions": {"$in": variants}}]}
    and_terms: list[dict[str, Any]] = [mongo_query]

    if type:
        regex = re.compile(r"^" + re.escape(type.strip()) + r"$", re.IGNORECASE)
        and_terms.append({"categories": regex})

    if q and q.strip():
        regex = re.compile(re.escape(q.strip()), re.IGNORECASE)
        and_terms.append({"$or": [{"label": regex}, {"nom": regex}, {"name": regex}, {"rdfs:label.fr": regex}]})

    final_query = and_terms[0] if len(and_terms) == 1 else {"$and": and_terms}
    cursor = target_collection.find(final_query).limit(limit)
    return [_public_object(doc) for doc in cursor]

# =============================================================
# JOURNEYS (CRUD)
# =============================================================
@app.get("/journeys")
@app.get("/journeys/list")
def list_journeys():
    cursor = journeys_col.find({}).sort("updatedAt", -1)
    return [_clean_mongo(doc) for doc in cursor]

@app.get("/journeys/{journey_id}")
def get_journey(journey_id: str):
    doc = journeys_col.find_one({"id": journey_id})
    if not doc: raise HTTPException(status_code=404, detail="Voyage introuvable")
    return JSONResponse(_clean_mongo(doc))

@app.post("/journeys")
@app.post("/journeys/save")
def create_journey(payload: dict[str, Any] = Body(...)):
    try:
        jid = _journey_id(payload)
        doc = _journey_doc(payload, jid)
        journeys_col.update_one({"id": jid}, {"$set": doc}, upsert=True)
        return {"status": "ok", "id": jid}
    except Exception as exc:
        import traceback
        traceback.print_exc()
        return JSONResponse(status_code=500, content={"status": "error", "message": str(exc)})

@app.put("/journeys/{journey_id}")
def update_journey(journey_id: str, payload: dict[str, Any] = Body(...)):
    doc = _journey_doc(payload, journey_id)
    res = journeys_col.update_one({"id": journey_id}, {"$set": doc}, upsert=True)
    return {"status": "updated" if res.matched_count > 0 else "ok", "id": journey_id}

@app.delete("/journeys/{journey_id}")
def delete_journey(journey_id: str):
    res = journeys_col.delete_one({"id": journey_id})
    if res.deleted_count == 0: raise HTTPException(status_code=404, detail="Voyage introuvable")
    return {"status": "deleted", "id": journey_id}

@app.post("/journeys/delete")
def delete_journey_alias(payload: dict[str, Any] = Body(...)):
    jid = str(payload.get("id", "")).strip()
    if not jid: raise HTTPException(status_code=400, detail="Champ 'id' requis")
    return delete_journey(jid)

# =============================================================
# ALGORITHME & AUTRES
# =============================================================
@app.get("/health")
def health():
    try:
        mongo_client.admin.command("ping")
        return {"ok": True, "mongo": "up"}
    except Exception as exc:
        return JSONResponse(status_code=500, content={"ok": False, "error": str(exc)})

@app.post("/algorithm")
async def run_algorithm(request: Request):
    try:
        data = await request.json()
        try:
            from .recommendation import lancer_explora
        except ImportError:
            from recommendation import lancer_explora

        ville = str(data.get("ville", "")).strip()
        if not ville: raise HTTPException(status_code=400, detail="Le champ 'ville' est requis")

        planning = lancer_explora(
            ville=ville,
            rayon=int(data.get("rayon", 30)),
            jours=int(data.get("jours", 3)),
            nature=int(data.get("nature", 0)),
            gastronomie=int(data.get("gastronomie", 0)),
            sport=int(data.get("sport", 0)),
            culture=int(data.get("culture", 0)),
            detente=int(data.get("détente", 0)),
            boutique=int(data.get("boutique", 0)),
        )

        if not planning:
            return JSONResponse(status_code=404, content={"status": "empty", "message": "Aucun itinéraire trouvé", "data": []})

        return JSONResponse({"status": "success", "message": "Itinéraire généré avec succès", "data": _json_safe(planning)})

    except Exception as e:
        print("[BACKEND] Erreur : ", e)
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)