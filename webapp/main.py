from pathlib import Path, PurePosixPath
from typing import Optional, List
from fastapi import FastAPI, Request, Query, Body, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from pymongo import MongoClient
from bson import ObjectId
from pydantic import BaseModel
from elasticsearch import Elasticsearch
import os
import time
import json as _json
import unicodedata
import httpx
from urllib.parse import urlparse, unquote
from itertools import chain
import re
from threading import RLock

app = FastAPI()

#  Panier en mémoire (dev) 
basket: List[dict] = []

class BasketItem(BaseModel):
    id: str
    name: str
    image: Optional[str] = None

#  MongoDB 
mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017/LORA_voyage")
client = MongoClient(mongo_uri)
db = client["LORA_voyage"]
objects_collection = db["objects"]
journeys_collection = db["journeys"]
print(f"✅ MongoDB: db='{db.name}', collection='{objects_collection.name}'")

#  Elasticsearch 
es_host = os.getenv("ELASTICSEARCH_URI", "http://elasticsearch:9200")
es = Elasticsearch(es_host)
print(f"✅ Elasticsearch: host='{es_host}'")

#  Static & Templates
BASE_DIR = Path(__file__).resolve().parent
app.mount("/static", StaticFiles(directory=BASE_DIR / "static"), name="static")
templates = Jinja2Templates(directory=BASE_DIR / "templates")

# data exposé en statique
DATA_ROOT = Path(os.getenv("DATA_ROOT", r"D:\LORA_Voyage\LORA_bis\data"))
app.mount("/data", StaticFiles(directory=DATA_ROOT, check_dir=False), name="data")
print(f"✅ /data → {DATA_ROOT}")

#  Helpers communs 

def get_first_image(doc: dict) -> str:
    # Structures Datatourisme / ebucore
    for key in ("hasMainRepresentation", "hasRepresentation"):
        reps = doc.get(key)
        if isinstance(reps, list):
            for rep in reps:
                rels = (rep.get("ebucore:hasRelatedResource") or []) if isinstance(rep, dict) else []
                for res in rels:
                    locator = res.get("ebucore:locator")
                    if isinstance(locator, list) and locator:
                        url = locator[0]
                        if isinstance(url, str) and any(url.lower().endswith(ext) for ext in (".jpg", ".jpeg", ".png", ".gif", ".webp")):
                            return url
    # Fallbacks simples
    for k in ("image", "thumbnail", "depiction"):
        v = doc.get(k)
        if isinstance(v, str) and v:
            return v
    imgs = doc.get("images")
    if isinstance(imgs, list) and imgs:
        u = imgs[0].get("url") if isinstance(imgs[0], dict) else None
        if isinstance(u, str) and u:
            return u
    return "/static/img/no-image.jpg"

def _pick_locality(obj: dict) -> str:
    try:
        return obj["isLocatedAt"][0]["schema:address"][0]["schema:addressLocality"]
    except Exception:
        return ""

#  Helpers Voyages 
def normalize_days(payload: dict) -> List[dict]:
    days = payload.get("days")
    if isinstance(days, list) and days:
        return days
    slots = payload.get("slots", {}) or {}
    return [{
        "day": 1,
        "slots": {
            "morning": slots.get("morning", []),
            "noon": slots.get("noon", []),
            "afternoon": slots.get("afternoon", []),
            "evening": slots.get("evening", []),
        }
    }]

def pick_cover_from_journey_doc(j: dict) -> str:
    for day in j.get("days", []):
        for acts in (day.get("slots") or {}).values():
            for a in acts or []:
                if a.get("image"):
                    return a["image"]
    for acts in (j.get("slots") or {}).values():
        for a in acts or []:
            if a.get("image"):
                return a["image"]
    for a in j.get("basket", []):
        if a.get("image"):
            return a["image"]
    return "/static/img/no-image.jpg"

#  Pages HTML 
@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    return templates.TemplateResponse("Accueil.html", {"request": request})

@app.get("/accueil", response_class=HTMLResponse)
async def accueil(request: Request):
    return templates.TemplateResponse("Accueil.html", {"request": request})

@app.get("/exploration", response_class=HTMLResponse)
async def exploration(request: Request):
    return templates.TemplateResponse("Exploration.html", {"request": request})

@app.get("/carnet", response_class=HTMLResponse)
async def carnet(request: Request):
    return templates.TemplateResponse("Carnet.html", {"request": request})

@app.get("/creation", response_class=HTMLResponse)
async def creation_editor(request: Request, id: str | None = None):
    """
    Ouvre l'éditeur Creation.html.
    - Si 'id' est fourni en query (?id=...), charge l'édition de ce voyage.
    - Sinon, démarre un nouveau voyage (id=None).
    """
    return templates.TemplateResponse("Creation.html", {"request": request, "id": id})

#  Détail d’un voyage (vue générale) 
@app.get("/journeys/view/{id}", response_class=HTMLResponse)
async def view_journey(request: Request, id: str):
    """
    Affiche la page de détail d’un voyage (ViewJourney.html).
    URL attendue par le front : /journeys/view/{id}
    """
    return templates.TemplateResponse("ViewJourney.html", {"request": request, "id": id})

#  Détail d’une journée spécifique d’un voyage 
@app.get("/journeys/view/{id}/day/{day}", response_class=HTMLResponse)
async def view_journey_day(request: Request, id: str, day: int):
    """
    Affiche la page d’une journée précise d’un voyage (ViewJourneyDay.html).
    URL attendue par le front : /journeys/view/{id}/day/{day}
    """
    return templates.TemplateResponse("ViewJourneyDay.html", {"request": request, "id": id, "day": day})

# Détail d’une activité/personnalisation 
@app.get("/detail-act-perso/{id:path}")
async def detail_act_perso_redirect(id: str):
    return RedirectResponse(url=f"/object/{id}", status_code=307)

# Edition via /creation/{journey_id}
@app.get("/creation/{journey_id}", response_class=HTMLResponse)
async def creation_edit(request: Request, journey_id: str):
    return templates.TemplateResponse("Creation.html", {"request": request, "id": journey_id})

# Page Région
@app.get("/region/{slug}", response_class=HTMLResponse)
async def region_page(request: Request, slug: str):
    region_slug = unquote(slug)
    return templates.TemplateResponse("Region.html", {"request": request, "region_slug": region_slug})

# ---------- Redirections anciennes routes ----------
@app.get("/Destinations")
async def old_destinations():
    return RedirectResponse(url="/exploration", status_code=308)

@app.get("/topics")
async def old_topics():
    return RedirectResponse(url="/carnet", status_code=308)

@app.get("/makejourney")
async def old_makejourney():
    return RedirectResponse(url="/creation", status_code=308)

@app.get("/makejourney/new")
async def old_makejourney_new():
    return RedirectResponse(url="/creation/nouveau", status_code=308)

@app.get("/makejourney/{journey_id}")
async def old_makejourney_edit(journey_id: str):
    return RedirectResponse(url=f"/creation/{journey_id}", status_code=308)

# Panier (API)
@app.post("/basket/add")
async def add_to_basket(item: BasketItem):
    basket.append(item.dict())
    return {"status": "ok", "basket": basket}

@app.get("/basket/get")
async def get_basket():
    return {"basket": basket}

# cache en mémoire pour la recherche
_SEARCH_CACHE: dict = {}
_SEARCH_CACHE_TTL = 30  # secondes
_SEARCH_CACHE_LOCK = RLock()

def _cache_get(key):
    now = time.time()
    with _SEARCH_CACHE_LOCK:
        v = _SEARCH_CACHE.get(key)
        if v and v["exp"] > now:
            return v["data"]
        if v:
            _SEARCH_CACHE.pop(key, None)
    return None

def _cache_set(key, data, ttl=_SEARCH_CACHE_TTL):
    with _SEARCH_CACHE_LOCK:
        _SEARCH_CACHE[key] = {"data": data, "exp": time.time() + ttl}

#  Recherche paginée (optimisée)
@app.get("/search")
async def search_destinations(
    query: str = Query(..., min_length=2),
    offset: int = Query(0, ge=0),
    limit: int = Query(30, ge=1, le=100),
    fast: bool = Query(False, description="Mode suggester rapide")
):
    """
    Recherche optimisée :
     - ES: multi_match + match_bool_prefix (rapide pour autocomplétion)
     - timeout court, payload réduit, track_total_hits désactivé
     - fallback Mongo avec regex préfixé (^query)
     - cache mémoire 30s
    """
    q = (query or "").strip()
    if len(q) < 2:
        return JSONResponse(content=[])

    cache_key = (q.lower(), offset, limit, bool(fast))
    cached = _cache_get(cache_key)
    if cached is not None:
        return JSONResponse(content=cached)

    # champs utilisés (pondération)
    fields = [
        "rdfs:label.fr^5",
        "isLocatedAt.schema:address.schema:addressLocality^3",
        "@type^1",
    ]

    # 1) ESSAI ELASTICSEARCH
    try:
        # Requête: un "best_fields" + un "bool_prefix" (auto-complétion sur le dernier terme)
        es_query = {
            "bool": {
                "should": [
                    {"multi_match": {
                        "query": q,
                        "fields": fields,
                        "type": "best_fields",
                        "operator": "and"
                    }},
                    {"multi_match": {
                        "query": q,
                        "fields": fields,
                        "type": "bool_prefix"
                    }},
                ],
                "minimum_should_match": 1
            }
        }

        source_includes = [
            "@id", "@type", "rdfs:label",
            "isLocatedAt.schema:address.schema:addressLocality",
            "hasMainRepresentation", "hasRepresentation",
            "image", "images", "thumbnail", "depiction"
        ]

        es_res = es.search(
            index="objects",
            query=es_query,
            from_=offset,
            size=limit,
            _source={"includes": source_includes},
            track_total_hits=False,      # on ne calcule pas le total (gagne du temps)
            timeout="2s",                # timeout côté cluster
            request_timeout=5,           # timeout côté client
            preference="_local"
        )

        hits = (es_res.get("hits", {}) or {}).get("hits", []) or []
        results = [h.get("_source", {}) for h in hits]

        # formatage identique à avant
        formatted_results = []
        seen_ids = set()
        for doc in results:
            item_id = doc.get("@id") or doc.get("_id")
            if isinstance(item_id, ObjectId):
                item_id = str(item_id)
            item_id = str(item_id) if item_id is not None else None
            if not item_id or item_id in seen_ids:
                continue
            seen_ids.add(item_id)

            # Nom
            name = "Sans nom"
            lab = doc.get("rdfs:label")
            if isinstance(lab, dict):
                name = (lab.get("fr") or lab.get("fr-FR") or lab.get("en") or ["Sans nom"])
                name = name[0] if isinstance(name, list) else name

            # Types
            types = doc.get("@type") or []
            if isinstance(types, str):
                types = [types]

            # Localité
            locality = _pick_locality(doc)

            formatted_results.append({
                "id": item_id,
                "name": name or "Sans nom",
                "image": get_first_image(doc),
                "types": types,
                "locality": locality,
            })

        _cache_set(cache_key, formatted_results)
        return JSONResponse(content=formatted_results)

    except Exception as e:
        print("⚠️ ES indisponible/timeout, fallback Mongo:", e)

    # 2) FALLBACK MONGO
    try:
        # Regex préfixée (plus rapide et comporte mieux pour autocomplétion)
        rx = {"$regex": f"^{re.escape(q)}", "$options": "i"}
        mongo_filter = {
            "$or": [
                {"rdfs:label.fr": rx},
                {"isLocatedAt.schema:address.schema:addressLocality": rx},
                {"@type": rx},
            ]
        }
        cursor = objects_collection.find(
            mongo_filter,
            projection={
                "@id": 1, "@type": 1, "rdfs:label": 1,
                "isLocatedAt.schema:address.schema:addressLocality": 1,
                "hasMainRepresentation": 1, "hasRepresentation": 1,
                "image": 1, "images": 1, "thumbnail": 1, "depiction": 1,
            }
        ).skip(offset).limit(limit)
        results = list(cursor)

        formatted_results = []
        seen_ids = set()
        for doc in results:
            item_id = doc.get("@id") or doc.get("_id")
            if isinstance(item_id, ObjectId):
                item_id = str(item_id)
            item_id = str(item_id) if item_id is not None else None
            if not item_id or item_id in seen_ids:
                continue
            seen_ids.add(item_id)

            name = "Sans nom"
            lab = doc.get("rdfs:label")
            if isinstance(lab, dict):
                name = (lab.get("fr") or lab.get("fr-FR") or lab.get("en") or ["Sans nom"])
                name = name[0] if isinstance(name, list) else name

            types = doc.get("@type") or []
            if isinstance(types, str):
                types = [types]

            locality = _pick_locality(doc)

            formatted_results.append({
                "id": item_id,
                "name": name or "Sans nom",
                "image": get_first_image(doc),
                "types": types,
                "locality": locality,
            })

        _cache_set(cache_key, formatted_results)
        return JSONResponse(content=formatted_results)

    except Exception as e2:
        print("❌ Fallback Mongo error:", e2)
        return JSONResponse(content=[], status_code=200)

#  API Voyages 
@app.post("/journeys")
async def create_journey(payload: dict = Body(...)):
    now = int(time.time() * 1000)
    doc = {
        "name": payload.get("name", "Sans nom"),
        "location": payload.get("location"),
        "image": payload.get("image"),
        "basket": payload.get("basket", []),
        "slots": payload.get("slots", {}),
        "days": normalize_days(payload),
        "updatedAt": payload.get("updatedAt", now)
    }
    result = journeys_collection.insert_one(doc)
    return {"status": "ok", "id": str(result.inserted_id)}

@app.post("/journeys/save")
async def save_journey(payload: dict = Body(...)):
    now = int(time.time() * 1000)
    jid = payload.get("id")
    if jid and ObjectId.is_valid(jid):
        journeys_collection.update_one(
            {"_id": ObjectId(jid)},
            {"$set": {
                "name": payload.get("name", "Sans nom"),
                "location": payload.get("location"),
                "image": payload.get("image"),
                "basket": payload.get("basket", []),
                "slots": payload.get("slots", {}),
                "days": normalize_days(payload),
                "updatedAt": payload.get("updatedAt", now)
            }}
        )
        return {"status": "updated", "id": jid}
    else:
        doc = {
            "name": payload.get("name", "Sans nom"),
            "location": payload.get("location"),
            "image": payload.get("image"),
            "basket": payload.get("basket", []),
            "slots": payload.get("slots", {}),
            "days": normalize_days(payload),
            "updatedAt": payload.get("updatedAt", now)
        }
        result = journeys_collection.insert_one(doc)
        return {"status": "ok", "id": str(result.inserted_id)}

@app.put("/journeys/{journey_id}")
async def update_journey(journey_id: str, payload: dict = Body(...)):
    if not ObjectId.is_valid(journey_id):
        return {"status": "not_found"}
    now = int(time.time() * 1000)
    journeys_collection.update_one(
        {"_id": ObjectId(journey_id)},
        {"$set": {
            "name": payload.get("name", "Sans nom"),
            "location": payload.get("location"),
            "image": payload.get("image"),
            "basket": payload.get("basket", []),
            "slots": payload.get("slots", {}),
            "days": normalize_days(payload),
            "updatedAt": payload.get("updatedAt", now)
        }}
    )
    return {"status": "updated", "id": journey_id}

@app.get("/journeys/{journey_id}")
async def get_journey(journey_id: str):
    if not ObjectId.is_valid(journey_id):
        return JSONResponse(status_code=404, content={"error": "not_found"})
    j = journeys_collection.find_one({"_id": ObjectId(journey_id)})
    if not j:
        return JSONResponse(status_code=404, content={"error": "not_found"})
    return {
        "id": str(j["_id"]),
        "name": j.get("name", "Sans nom"),
        "location": j.get("location"),
        "image": j.get("image"),
        "basket": j.get("basket", []),
        "slots": j.get("slots", {}),
        "days": j.get("days", []),
        "updatedAt": j.get("updatedAt")
    }

@app.get("/journeys/list")
async def list_journeys_alias():
    journeys = list(journeys_collection.find())
    formatted = []
    for j in journeys:
        image = pick_cover_from_journey_doc(j)
        formatted.append({
            "id": str(j["_id"]),
            "name": j.get("name", "Sans nom"),
            "location": j.get("location"),
            "image": image,
            "updatedAt": j.get("updatedAt", None),
            "daysCount": len(j.get("days", [])) if isinstance(j.get("days"), list) else 0
        })
    return JSONResponse(content=formatted)

@app.delete("/journeys/{journey_id}")
async def delete_journey(journey_id: str):
    if ObjectId.is_valid(journey_id):
        result = journeys_collection.delete_one({"_id": ObjectId(journey_id)})
        if result.deleted_count > 0:
            return {"status": "deleted"}
    return {"status": "not_found"}

@app.post("/journeys/delete")
async def delete_journey_legacy(payload: dict = Body(...)):
    jid = payload.get("id")
    if jid and ObjectId.is_valid(jid):
        result = journeys_collection.delete_one({"_id": ObjectId(jid)})
        if result.deleted_count > 0:
            return {"status": "deleted"}
    return {"status": "not_found"}

@app.get("/region/ara", response_class=HTMLResponse)
async def region_ara_alias(request: Request):
    return templates.TemplateResponse("Region.html", {"request": request, "region_slug": "Auvergne-Rhône-Alpes"})

#  Régions avec Cards 

REGION_DIRS = [
    DATA_ROOT,                                          # /app/data/Hauts-de-France.json
    DATA_ROOT / "regions",
    DATA_ROOT / "full_france_object" / "regions",
]
OBJECTS_BASE = DATA_ROOT / "full_france_object" / "objects"

def _slugify(s: str) -> str:
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode()
    s = s.replace("_", "-").replace(" ", "-")
    while "--" in s:
        s = s.replace("--", "-")
    return s.lower()

def _find_region_file(name: str) -> Optional[Path]:
    candidates = [f"{name}.json", f"{_slugify(name)}.json"]
    for d in REGION_DIRS:
        p = d / candidates[0]
        if p.exists():
            return p
        p2 = d / candidates[1]
        if p2.exists():
            return p2
    slug = _slugify(name)
    for d in REGION_DIRS:
        for p in d.glob("*.json"):
            if _slugify(p.stem) == slug:
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

def _pick_label(obj: dict, fallback: Optional[str] = None) -> str:
    lab = obj.get("rdfs:label")
    if isinstance(lab, dict):
        for k in ("fr", "fr-FR", "en"):
            v = lab.get(k)
            if isinstance(v, list) and v:
                return v[0]
            if isinstance(v, str) and v.strip():
                return v
    return obj.get("name") or obj.get("title") or (fallback or "Sans nom")

@app.get("/regions/{slug}/cards")
async def region_cards(
    slug: str,
    limit: int = Query(72, ge=1, le=200),
    offset: int = Query(0, ge=0),
    q: str | None = None,
    typ: str | None = Query(default=None, alias="type"),
    debug: bool = False,
):
    def build_es_query(slug: str, typ: str | None, q: str | None):
        slug_space = slug.replace("-", " ")
        variants = [slug, slug_space, slug.lower(), slug_space.lower()]

        region_fields = [
            "hasBeenCreatedBy.schema:address.hasAddressCity.isPartOfDepartment.isPartOfRegion.rdfs:label.fr",
            "isLocatedAt.schema:address.isPartOfDepartment.isPartOfRegion.rdfs:label.fr",
            "hasBeenCreatedBy.schema:address.hasAddressCity.isPartOfDepartment.isPartOfRegion.@id",
            "isLocatedAt.schema:address.isPartOfDepartment.isPartOfRegion.@id",
            "isLocatedAt.schema:address.schema:addressRegion",
            "isLocatedAt.schema:address.addressRegion",
        ]

        region_should = []
        for f in region_fields:
            for v in variants:
                region_should.append({"match": {f: v}})
            region_should.append({"wildcard": {f: f"*{variants[-1]}*"}})

        region_should.append({
            "query_string": {
                "query": f'("{slug}" OR "{slug_space}")',
                "fields": ["*"],
                "analyze_wildcard": True,
                "default_operator": "AND"
            }
        })

        must_clauses = [{"bool": {"should": region_should, "minimum_should_match": 1}}]

        if typ:
            raw_types = [t.strip() for t in typ.split(",") if t.strip()]
            expanded = []
            for t in raw_types:
                expanded.append(t)
                if not t.startswith("http"):
                    expanded.append(f"https://www.datatourisme.fr/ontology/core#{t}")
                    expanded.append(f"https://schema.org/{t}")
            expanded = list(dict.fromkeys(expanded))

            typ_should = [
                {"terms": {"@type.keyword": expanded}},
                {"terms": {"@type": expanded}},
            ] + list(chain.from_iterable(
                ({"match": {"@type": t}}, {"wildcard": {"@type": f"*{t.lower()}*"}}) for t in raw_types
            ))
            must_clauses.append({"bool": {"should": typ_should, "minimum_should_match": 1}})

        should_clauses = []
        if q:
            ql = q.lower()
            should_clauses += [
                {"match": {"@type": q}},
                {"match": {"rdfs:label.fr": q}},
                {"match": {"isLocatedAt.schema:address.schema:addressLocality": q}},
                {"wildcard": {"@type": f"*{ql}*"}},
                {"wildcard": {"rdfs:label.fr": f"*{ql}*"}},
                {"wildcard": {"isLocatedAt.schema:address.schema:addressLocality": f"*{ql}*"}},
                {"fuzzy": {"rdfs:label.fr": {"value": q, "fuzziness": "AUTO"}}},
            ]

        es_query = {"bool": {"must": must_clauses}}
        if should_clauses:
            es_query["bool"]["should"] = should_clauses
        return es_query

    def card_from_source(src: dict) -> dict:
        name = _pick_label(src)
        return {
            "id": src.get("@id") or name or "",
            "name": name,
            "image": get_first_image(src),
            "types": src.get("@type") or [],
            "locality": _pick_locality(src)
        }

    def dedupe_cards(cards: list[dict], limit: int) -> tuple[list[dict], int, int]:
        seen = set()
        uniq = []
        for c in cards:
            key = c.get("id") or f"{c.get('name','')}|{c.get('locality','')}"
            if not key:
                key = _json.dumps({"n": c.get("name",""), "l": c.get("locality","")}, ensure_ascii=False)
            if key in seen:
                continue
            seen.add(key)
            uniq.append(c)
            if len(uniq) >= limit:
                break
        return uniq, len(cards), len(seen)

    try:
        es_query = build_es_query(slug, typ, q)

        source_includes = [
            "@id", "@type", "rdfs:label",
            "isLocatedAt.schema:address.schema:addressLocality",
            "hasMainRepresentation", "hasRepresentation",
            "image", "images", "thumbnail", "depiction",
        ]

        search_size = min(1000, max(limit * 3, limit))
        es_res = es.search(
            index="objects",
            query=es_query,
            from_=offset,
            size=search_size,
            _source={"includes": source_includes},
            track_total_hits=True
        )
        hits = es_res.get("hits", {}).get("hits", [])
        cards = [card_from_source(h.get("_source", {})) for h in hits]
        uniq, before_cnt, after_cnt = dedupe_cards(cards, limit)

        retried = False
        if not uniq and typ:
            retried = True
            es_query2 = build_es_query(slug, None, q)
            es_res = es.search(
                index="objects",
                query=es_query2,
                from_=offset,
                size=search_size,
                _source={"includes": source_includes},
                track_total_hits=True
            )
            hits = es_res.get("hits", {}).get("hits", [])
            cards = [card_from_source(h.get("_source", {})) for h in hits]
            uniq, before_cnt, after_cnt = dedupe_cards(cards, limit)

        if debug:
            return JSONResponse(content={
                "took": es_res.get("took"),
                "total": es_res.get("hits", {}).get("total"),
                "returned": len(uniq),
                "deduped_from": before_cnt,
                "unique_after_dedupe_pool": after_cnt,
                "retried_without_type": retried,
                "query": es_query if not retried else {"first": es_query, "second": build_es_query(slug, None, q)},
                "sample_ids": [c.get("id") for c in uniq[:8]],
            })

        return JSONResponse(content=uniq)

    except Exception as e:
        print("⚠️ ES indisponible, fallback filesystem:", e)

    # ------- Fallback fichier -------
    try:
        region_file = _find_region_file(slug)
        if not region_file:
            searched = " | ".join(str(d) for d in REGION_DIRS)
            raise HTTPException(status_code=404, detail=f"Région '{slug}' introuvable. Cherché dans: {searched}")

        data = _json.loads(region_file.read_text(encoding="utf-8"))
        entries = data if isinstance(data, list) else data.get("items", [])
        if not isinstance(entries, list):
            raise HTTPException(status_code=400, detail="Format région non supporté (attendu une liste).")

        wanted_types = {t.strip().lower() for t in typ.split(",")} if typ else None
        q_norm = q.lower() if q else None

        cards = []
        for e in entries:
            if not isinstance(e, dict):
                continue
            rel_file = e.get("file") or e.get("path")
            identifier = e.get("identifier") or e.get("id") or e.get("dc:identifier")
            label_hint = e.get("label") or e.get("name")

            obj = {}
            if rel_file:
                p = _safe_join_objects(rel_file)
                if p and p.exists():
                    try:
                        obj = _json.loads(p.read_text(encoding="utf-8"))
                    except Exception:
                        obj = {}

            types = obj.get("@type") or []
            if isinstance(types, str):
                types = [types]
            types_l = [str(t).lower() for t in types] if isinstance(types, list) else []

            name = _pick_label(obj, fallback=label_hint) if obj else (label_hint or "Sans nom")
            locality = _pick_locality(obj) if obj else ""

            if wanted_types and not any(t in types_l for t in wanted_types):
                continue
            if q_norm:
                hay = " ".join([name or "", locality or "", (label_hint or "")]).lower()
                if q_norm not in hay and all(q_norm not in str(t).lower() for t in types_l):
                    continue

            cards.append({
                "id": (obj.get("@id") if obj else None) or identifier or name or "",
                "name": name or "Sans nom",
                "image": get_first_image(obj) if obj else None,
                "types": types or [],
                "locality": locality
            })

        uniq, _, _ = dedupe_cards(cards, limit)
        return JSONResponse(content=uniq)

    except HTTPException:
        raise
    except Exception as e2:
        raise HTTPException(status_code=500, detail=f"Erreur fallback région: {e2}")

# ----- /api/object & /proxy : fetch tolérant -----
@app.get("/api/object")
async def api_object(url: str):
    """
    Proxy JSON/JSON-LD tolérant.
    - Essaye l'URL telle quelle, puis la variante ?format=jsonld
    - Tolère un 'content-type' mal déclaré (parse via .text)
    - Donne un diagnostic clair (status, content-type, extrait du body si HTML)
    """
    # 0) support fichier local
    p = urlparse(url)
    if p.scheme in ("", "file"):
        fp = Path(p.path)
        if fp.exists():
            try:
                return JSONResponse(_json.loads(fp.read_text(encoding="utf-8")))
            except Exception as e:
                return JSONResponse({"error":"local_file_not_json", "detail":str(e)}, status_code=415)

    # 1) HTTP(S)
    headers = {
        "Accept": "application/ld+json, application/json;q=0.9, */*;q=0.1",
        "User-Agent": "Mozilla/5.0 (compatible; LoraVoyage/1.0; +http://localhost:8080)"
    }
    candidates = [url]
    if "format=" not in url:
        candidates.append(url + ("&" if "?" in url else "?") + "format=jsonld")

    async with httpx.AsyncClient(follow_redirects=True, timeout=20) as client:
        last_detail = None
        for u in candidates:
            try:
                r = await client.get(u, headers=headers)
                ct = r.headers.get("content-type", "")
                if r.status_code != 200:
                    last_detail = f"HTTP {r.status_code} ({ct})"
                    continue

                txt = r.text
                try:
                    data = _json.loads(txt)
                    return JSONResponse(data)
                except Exception as e:
                    if "<html" in txt[:200].lower():
                        last_detail = f"Upstream returned HTML (content-type: {ct})"
                    else:
                        last_detail = f"JSON parse failed (content-type: {ct}): {e}"
            except Exception as e:
                last_detail = f"request error: {e!r}"

    return JSONResponse(
        {"error": "upstream_fetch_failed", "detail": last_detail, "tried": candidates},
        status_code=502
    )

@app.get("/proxy")
async def proxy(url: str):
    return await api_object(url)

#  Check Health 
@app.get("/health")
def health():
    ok_es = False
    try:
        ok_es = bool(es.ping())
    except Exception:
        ok_es = False
    try:
        client.admin.command("ping")
        ok_mongo = True
    except Exception:
        ok_mongo = False
    return {
        "mongo": ok_mongo,
        "elasticsearch": ok_es,
        "data_root_exists": DATA_ROOT.exists()
    }

def _pick_lang_text(langmap, langs=('fr','fr-FR','en','es','it','de','nl','pt')):
    if isinstance(langmap, dict):
        for L in langs:
            v = langmap.get(L)
            if isinstance(v, list) and v:
                return v[0]
            if isinstance(v, str) and v.strip():
                return v
        for v in langmap.values():
            if isinstance(v, list) and v:
                return v[0]
            if isinstance(v, str) and v.strip():
                return v
    elif isinstance(langmap, list):
        for x in langmap:
            if isinstance(x, str) and x.strip():
                return x
    elif isinstance(langmap, str):
        return langmap
    return None

def extract_descriptions(doc: dict) -> list[str]:
    KEYS = (
        "dc:description",
        "schema:description",
        "rdfs:comment",
        "http://www.w3.org/2000/01/rdf-schema#comment",
        "shortDescription",
        "longDescription",
        "https://www.datatourisme.fr/ontology/core#shortDescription",
        "https://www.datatourisme.fr/ontology/core#longDescription",
    )
    out: list[str] = []

    def pick(v):
        t = _pick_lang_text(v)
        return t.strip() if isinstance(t, str) else None

    for hd_key in ("hasDescription", "https://www.datatourisme.fr/ontology/core#hasDescription"):
        hd = doc.get(hd_key)
        if isinstance(hd, list):
            for d in hd:
                if isinstance(d, dict):
                    for k in KEYS:
                        if k in d:
                            t = pick(d.get(k))
                            if t:
                                out.append(t)

    for k in KEYS:
        if k in doc:
            t = pick(doc.get(k))
            if t:
                out.append(t)

    seen, uniq = set(), []
    for s in out:
        if s and s not in seen:
            seen.add(s)
            uniq.append(s)
    return uniq

@app.get("/object/{item_id:path}", response_class=HTMLResponse)
async def object_detail(request: Request, item_id: str):
    decoded_id = unquote(item_id)
    doc = None

    # 1) ES par _id
    try:
        es_res = es.get(index="objects", id=decoded_id, ignore=[404])
        if es_res and "_source" in es_res:
            doc = es_res["_source"]
    except Exception as e:
        print(f"⚠️ ES get error: {e}")

    # 2) ES par @id
    if not doc:
        try:
            q = {"term": {"@id.keyword": decoded_id}}
            es_s = es.search(index="objects", query=q, size=1, _source_includes=["*"])
            hits = es_s.get("hits", {}).get("hits", [])
            if hits:
                doc = hits[0].get("_source", {})
        except Exception as e:
            print(f"⚠️ ES search by @id error: {e}")

    # 3) Mongo par @id
    if not doc:
        for coll_name in ("objects", "index"):
            coll = db[coll_name]
            m = coll.find_one({"@id": decoded_id})
            if m:
                doc = m
                break

    # 4) Mongo par _id
    if not doc and ObjectId.is_valid(decoded_id):
        for coll_name in ("objects", "index"):
            coll = db[coll_name]
            m = coll.find_one({"_id": ObjectId(decoded_id)})
            if m:
                doc = m
                break

    if not doc:
        return templates.TemplateResponse("DetailActPerso.html", {
            "request": request,
            "name": "Objet introuvable",
            "image": "/static/img/no-image.jpg",
            "contacts": [],
            "descriptions": []
        })

    name = (doc.get("rdfs:label", {}) or {}).get("fr", ["Sans nom"])[0]
    image = get_first_image(doc)
    descriptions = extract_descriptions(doc)
    contacts = []
    if "hasContact" in doc and isinstance(doc["hasContact"], list):
        for contact in doc["hasContact"]:
            contacts.append({
                "email": (contact.get("schema:email", [None]) or [None])[0],
                "telephone": (contact.get("schema:telephone", [None]) or [None])[0],
                "homepage": (contact.get("foaf:homepage", [None]) or [None])[0]
            })

    return templates.TemplateResponse("DetailActPerso.html", {
        "request": request,
        "id": decoded_id,
        "name": name,
        "image": image,
        "descriptions": descriptions,
        "contacts": contacts,
        "types": doc.get("@type", []),
        "imageAttribution": doc.get("image_attribution")
    })
