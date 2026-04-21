from __future__ import annotations

import os
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Any

from fastapi import Body, FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pymongo import MongoClient


BASE_DIR = Path(__file__).resolve().parent
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "LORA_voyage")
MONGO_OBJECTS_COL = os.getenv("MONGO_COLLECTION", "objects")
MONGO_JOURNEYS_COL = os.getenv("MONGO_JOURNEYS_COLLECTION", "journeys")


app = FastAPI(title="Explora Mongo API")
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))


mongo_client = MongoClient(MONGO_URI)
db = mongo_client[MONGO_DB]
objects_col = db[MONGO_OBJECTS_COL]
journeys_col = db[MONGO_JOURNEYS_COL]


def _normalize_region(value: str) -> str:
    s = value.lower()
    mapping = {
        "hauts-de-france": "hauts-de-france",
        "hdf": "hauts-de-france",
        "auvergne-rhone-alpes": "auvergne-rhone-alpes",
        "ara": "auvergne-rhone-alpes",
    }
    key = re.sub(r"[^a-z0-9]+", "-", s).strip("-")
    return mapping.get(key, key)


def _region_variants(region: str) -> list[str]:
    key = _normalize_region(region)
    if key == "hauts-de-france":
        return ["Hauts-de-France", "hauts-de-france", "Hauts de France"]
    if key == "auvergne-rhone-alpes":
        return [
            "Auvergne-Rhone-Alpes",
            "Auvergne-Rhône-Alpes",
            "auvergne-rhone-alpes",
        ]
    return [region]


def _pick_name(doc: dict[str, Any]) -> str:
    label = doc.get("rdfs:label")
    if isinstance(label, dict):
        fr = label.get("fr")
        if isinstance(fr, list) and fr:
            return str(fr[0])
        if isinstance(fr, str):
            return fr
    for key in ("label", "nom", "name"):
        value = doc.get(key)
        if isinstance(value, str) and value.strip():
            return value
    return "Sans nom"


def _pick_image(doc: dict[str, Any]) -> str:
    for key in ("image", "photo", "picture", "thumbnail", "cover"):
        value = doc.get(key)
        if isinstance(value, str) and value.strip():
            return value
    reps = doc.get("https://www.datatourisme.fr/ontology/core#hasMainRepresentation")
    if isinstance(reps, list):
        for rep in reps:
            if not isinstance(rep, dict):
                continue
            resources = rep.get("ebucore:hasRelatedResource")
            if not isinstance(resources, list):
                continue
            for r in resources:
                if not isinstance(r, dict):
                    continue
                locators = r.get("ebucore:locator")
                if isinstance(locators, list):
                    for loc in locators:
                        if isinstance(loc, str) and loc.strip():
                            return loc
    return "/static/img/no-image.jpg"


def _pick_locality(doc: dict[str, Any]) -> str:
    is_located = doc.get("isLocatedAt")
    if not isinstance(is_located, list) or not is_located:
        return ""
    first = is_located[0]
    if not isinstance(first, dict):
        return ""
    addresses = first.get("schema:address")
    if not isinstance(addresses, list) or not addresses:
        return ""
    addr = addresses[0]
    if not isinstance(addr, dict):
        return ""
    locality = addr.get("schema:addressLocality")
    return locality if isinstance(locality, str) else ""


def _pick_types(doc: dict[str, Any]) -> list[str]:
    types = doc.get("@type", [])
    if isinstance(types, list):
        return [str(t) for t in types]
    if isinstance(types, str):
        return [types]
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
        "category": doc.get("category", {}),
    }


def _clean_mongo(value: Any) -> Any:
    if isinstance(value, dict):
        return {k: _clean_mongo(v) for k, v in value.items() if k != "_id"}
    if isinstance(value, list):
        return [_clean_mongo(v) for v in value]
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def _get_object_by_id(object_id: str) -> dict[str, Any] | None:
    doc = objects_col.find_one({"@id": object_id})
    if doc:
        return doc
    doc = objects_col.find_one({"identifier": object_id})
    if doc:
        return doc
    return None


def _get_descriptions(doc: dict[str, Any]) -> list[str]:
    descs: list[str] = []
    comments = doc.get("rdfs:comment")
    if isinstance(comments, dict):
        fr = comments.get("fr")
        if isinstance(fr, list):
            descs.extend([str(x) for x in fr if isinstance(x, str)])
        elif isinstance(fr, str):
            descs.append(fr)

    has_desc = doc.get("hasDescription")
    if isinstance(has_desc, list):
        for item in has_desc:
            if not isinstance(item, dict):
                continue
            short_desc = item.get("shortDescription")
            if isinstance(short_desc, dict):
                fr = short_desc.get("fr")
                if isinstance(fr, list):
                    descs.extend([str(x) for x in fr if isinstance(x, str)])
                elif isinstance(fr, str):
                    descs.append(fr)

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
    if isinstance(given, str) and given.strip():
        return given.strip()
    return f"j_{int(time.time() * 1000)}"


def _journey_doc(payload: dict[str, Any], journey_id: str) -> dict[str, Any]:
    doc = dict(payload)
    doc["id"] = journey_id
    doc["updatedAt"] = int(time.time() * 1000)
    return doc


@app.get("/")
def home(request: Request):
    return templates.TemplateResponse(request=request, name="Accueil.html")


@app.get("/accueil")
def accueil(request: Request):
    return templates.TemplateResponse(request=request, name="Accueil.html")


@app.get("/exploration")
@app.get("/Destinations")
def exploration(request: Request):
    return templates.TemplateResponse(request=request, name="Catalogue.html")


@app.get("/region/{region_slug}")
def region_page(request: Request, region_slug: str):
    return templates.TemplateResponse(request=request, name="Region.html", context={"region": region_slug})


@app.get("/carnet")
@app.get("/topics")
def carnet(request: Request):
    return templates.TemplateResponse(request=request, name="MesVoyages.html")

@app.get("/editeur")
def editeur_page(request: Request):
    return templates.TemplateResponse(request=request, name="Editeur.html")

@app.get("/creation")
@app.get("/makejourney")
@app.get("/makejourney/new")
@app.get("/makejourney/{journey_id}")
def creation(request: Request, journey_id: str | None = None):
    return templates.TemplateResponse(
        request=request,
        name="Creation.html",
        context={"journey_id": journey_id or ""},
    )


@app.get("/voyage")
@app.get("/Voyage.html")
def voyage_page(request: Request):
    return templates.TemplateResponse(request=request, name="Voyage.html")

@app.get("/journeys/view/{journey_id}")
def journey_view(request: Request, journey_id: str):
    return templates.TemplateResponse(
        request=request,
        name="ViewJourney.html",
        context={"journey_id": journey_id},
    )

@app.get("/journeys/view/{journey_id}/day/{day_index}")
def journey_day_view(request: Request, journey_id: str, day_index: int):
    return templates.TemplateResponse(
        request=request,
        name="ViewJourneyDay.html",
        context={"journey_id": journey_id, "day": day_index},
    )

@app.get("/object/{object_id:path}")
@app.get("/detail-act-perso/{object_id:path}")
def object_page(request: Request, object_id: str):
    decoded = object_id
    doc = _get_object_by_id(decoded)
    if not doc:
        raise HTTPException(status_code=404, detail="Objet introuvable")

    context = {
        "request": request,
        "name": _pick_name(doc),
        "image": _pick_image(doc),
        "descriptions": _get_descriptions(doc),
    }
    return templates.TemplateResponse(request=request, name="DetailActPerso.html", context=context)


@app.get("/objects/{object_id:path}")
@app.get("/api/objects/{object_id:path}")
@app.get("/api/object/{object_id:path}")
def object_api(object_id: str):
    doc = _get_object_by_id(object_id)
    if not doc:
        raise HTTPException(status_code=404, detail="Objet introuvable")
    return JSONResponse(_clean_mongo(doc))


@app.get("/search")
def search(
    query: str = Query("", min_length=0),
    offset: int = Query(0, ge=0),
    limit: int = Query(30, ge=1, le=100),
):
    q = query.strip()
    if len(q) < 1:
        return []

    regex = re.compile(re.escape(q), re.IGNORECASE)
    mongo_query = {
        "$or": [
            {"label": regex},
            {"nom": regex},
            {"name": regex},
            {"rdfs:label.fr": regex},
        ]
    }

    cursor = objects_col.find(mongo_query).skip(offset).limit(limit)
    return [_public_object(doc) for doc in cursor]


@app.get("/regions/{region_slug}/cards")
def region_cards(
    region_slug: str,
    type: str | None = None,
    q: str | None = None,
    limit: int = Query(24, ge=1, le=120),
):
    variants = _region_variants(region_slug)
    mongo_query: dict[str, Any] = {
        "$or": [
            {"region": {"$in": variants}},
            {"_regions": {"$in": variants}},
        ]
    }

    and_terms: list[dict[str, Any]] = [mongo_query]

    if type:
        and_terms.append({"@type": type})

    if q and q.strip():
        regex = re.compile(re.escape(q.strip()), re.IGNORECASE)
        and_terms.append(
            {
                "$or": [
                    {"label": regex},
                    {"nom": regex},
                    {"name": regex},
                    {"rdfs:label.fr": regex},
                ]
            }
        )

    final_query: dict[str, Any]
    if len(and_terms) == 1:
        final_query = and_terms[0]
    else:
        final_query = {"$and": and_terms}

    cursor = objects_col.find(final_query).limit(limit)
    return [_public_object(doc) for doc in cursor]


@app.get("/journeys")
def list_journeys():
    cursor = journeys_col.find({}).sort("updatedAt", -1)
    return [_clean_mongo(doc) for doc in cursor]


@app.get("/journeys/list")
def list_journeys_alias():
    return list_journeys()


@app.get("/journeys/{journey_id}")
def get_journey(journey_id: str):
    doc = journeys_col.find_one({"id": journey_id})
    if not doc:
        raise HTTPException(status_code=404, detail="Voyage introuvable")
    return JSONResponse(_clean_mongo(doc))


@app.post("/journeys")
def create_journey(payload: dict[str, Any] = Body(...)):
    jid = _journey_id(payload)
    doc = _journey_doc(payload, jid)
    journeys_col.update_one({"id": jid}, {"$set": doc}, upsert=True)
    return {"status": "ok", "id": jid}


@app.post("/journeys/save")
def save_journey(payload: dict[str, Any] = Body(...)):
    return create_journey(payload)


@app.put("/journeys/{journey_id}")
def update_journey(journey_id: str, payload: dict[str, Any] = Body(...)):
    doc = _journey_doc(payload, journey_id)
    res = journeys_col.update_one({"id": journey_id}, {"$set": doc}, upsert=True)
    status = "updated" if res.matched_count > 0 else "ok"
    return {"status": status, "id": journey_id}


@app.delete("/journeys/{journey_id}")
def delete_journey(journey_id: str):
    res = journeys_col.delete_one({"id": journey_id})
    if res.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Voyage introuvable")
    return {"status": "deleted", "id": journey_id}


@app.post("/journeys/delete")
def delete_journey_alias(payload: dict[str, Any] = Body(...)):
    jid = str(payload.get("id", "")).strip()
    if not jid:
        raise HTTPException(status_code=400, detail="Champ 'id' requis")
    return delete_journey(jid)


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
        print("[BACKEND] Données reçues depuis le formulaire : ", data)

        # Import local pour éviter de faire tomber l'app au démarrage si recommendation.py a un souci.
        try:
            from .recommendation import lancer_explora
        except ImportError:
            from recommendation import lancer_explora

        ville = str(data.get("ville", "")).strip()
        if not ville:
            raise HTTPException(status_code=400, detail="Le champ 'ville' est requis")

        planning = lancer_explora(
            ville=ville,
            rayon=int(data.get("rayon", 30)),
            jours=int(data.get("jours", 3)),
            nature=int(data.get("nature", 0)),
            gastronomie=int(data.get("gastronomie", 0)),
            sport=int(data.get("sport", 0)),
            culture=int(data.get("culture", 0)),
            detente=int(data.get("detente", 0)),
            boutique=int(data.get("boutique", 0)),
        )

        if not planning:
            return JSONResponse(
                status_code=404,
                content={
                    "status": "empty",
                    "message": "Aucun itinéraire trouvé avec les paramètres fournis",
                    "data": [],
                },
            )

        return JSONResponse(
            {
                "status": "success",
                "message": "Itinéraire généré avec succès",
                "data": planning,
            }
        )

    except Exception as e:
        print("[BACKEND] Erreur : ", e)
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)
