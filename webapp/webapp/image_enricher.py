# image_enricher.py
"""
Ajoute une image aux objets sans image en essayant, dans l'ordre :
  1) Open Graph (depuis la homepage officielle si dispo)
  2) Openverse (images CC)
  3) Wikimedia Commons (images libres, CC/PD)

Dépendances:
  pip install requests pymongo

ENV attendues (avec valeurs par défaut):
  MONGO_URI=mongodb://localhost:27017/LORA_voyage
  MONGO_DB=LORA_voyage
  MONGO_COL=objects
  ENRICH_LIMIT=200               # nombre d'objets à tenter (0 = illimité)
  ENRICH_PREFER="og,openverse,wikimedia"  # ordre des fournisseurs
"""
from __future__ import annotations
import os, re, time, json, html
import requests
from urllib.parse import urlparse, urljoin, quote
from pymongo import MongoClient

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/LORA_voyage")
MONGO_DB  = os.getenv("MONGO_DB", "LORA_voyage")
MONGO_COL = os.getenv("MONGO_COL", "objects")
ENRICH_LIMIT = int(os.getenv("ENRICH_LIMIT", "200"))
PREF_ORDER = [x.strip() for x in os.getenv("ENRICH_PREFER", "og,openverse,wikimedia").split(",") if x.strip()]

UA = {"User-Agent":"Mozilla/5.0 (compatible; LORA-image-enricher/1.0)"}

client = MongoClient(MONGO_URI)
col = client[MONGO_DB][MONGO_COL]

def _pick_lang_text(langmap, langs=("fr","fr-FR","en","es","de","it","nl")):
    if isinstance(langmap, str):
        return langmap.strip() or None
    if isinstance(langmap, list):
        for x in langmap:
            if isinstance(x, str) and x.strip(): return x
            if isinstance(x, dict) and isinstance(x.get("@value"), str) and x["@value"].strip(): return x["@value"]
        return None
    if isinstance(langmap, dict):
        if "@value" in langmap and isinstance(langmap["@value"], str) and langmap["@value"].strip():
            return langmap["@value"]
        for L in langs:
            v = langmap.get(L)
            t = _pick_lang_text(v)
            if t: return t
        for v in langmap.values():
            t = _pick_lang_text(v)
            if t: return t
    return None

def _name_of(doc):
    return _pick_lang_text(doc.get("rdfs:label") or {}) or doc.get("name") or ""

def _locality_of(doc):
    try:
        return doc["isLocatedAt"][0]["schema:address"][0]["schema:addressLocality"]
    except Exception:
        return ""

def _homepage_of(doc):
    # Chemins usuels (ajuste si besoin)
    paths = [
        ("hasContact",0,"foaf:homepage",0),
        ("hasContact",0,"schema:url",0),
        ("homepage",),
        ("schema:url",),
    ]
    for p in paths:
        cur = doc
        try:
            for key in p:
                cur = cur[key]
            if isinstance(cur, str) and cur.startswith("http"):
                return cur
        except Exception:
            continue
    return None

# ---------- Provider 1: Open Graph (site officiel) ----------
OG_IMG_RE = re.compile(r'<meta[^>]+property=["\']og:image["\'][^>]+content=["\']([^"\']+)["\']', re.I)
TW_IMG_RE = re.compile(r'<meta[^>]+name=["\']twitter:image(:src)?["\'][^>]+content=["\']([^"\']+)["\']', re.I)

def fetch_og_image(url: str, timeout=10) -> str | None:
    try:
        r = requests.get(url, headers=UA, timeout=timeout)
        if r.status_code != 200: return None
        ctype = r.headers.get("content-type","")
        if "text/html" not in ctype: return None
        html_text = r.text or ""
        m = OG_IMG_RE.search(html_text)
        img = m.group(1).strip() if m else None
        if not img:
            t = TW_IMG_RE.search(html_text)
            img = t.group(2).strip() if t else None
        if not img:
            return None
        # rendre absolu si relatif
        if img.startswith("//"):
            img = f"{urlparse(url).scheme}:{img}"
        elif img.startswith("/"):
            img = urljoin(url, img)
        return img
    except Exception:
        return None

# ---------- Provider 2: Openverse (CC) ----------
def search_openverse(q: str):
    if not q: return None
    try:
        resp = requests.get(
            "https://api.openverse.engineering/v1/images/",
            params={"q": q, "page_size": 10, "license_type": "all-cc", "fields": "url,thumbnail,creator,license,license_url,source"},
            headers=UA, timeout=12
        )
        if resp.status_code != 200: return None
        data = resp.json()
        for it in (data.get("results") or []):
            url = it.get("url") or it.get("thumbnail")
            if not url: continue
            return {
                "image": url,
                "attribution": {
                    "origin": "openverse",
                    "source": it.get("source"),
                    "creator": it.get("creator"),
                    "license": it.get("license"),
                    "license_url": it.get("license_url"),
                }
            }
    except Exception:
        return None
    return None

# ---------- Provider 3: Wikimedia Commons ----------
ACCEPTED_LICENSES = {"cc0","cc-by","cc-by-sa","public domain","pd","pd-old","pd-art"}
def search_wikimedia(q: str):
    if not q: return None
    try:
        # recherche d’images directes
        params = {
            "action":"query",
            "format":"json",
            "origin":"*",
            "generator":"search",
            "gsrsearch": q,
            "gsrlimit": 6,
            "prop":"imageinfo",
            "iiprop":"url|extmetadata",
            "iiurlwidth": 1600,
        }
        resp = requests.get("https://commons.wikimedia.org/w/api.php", params=params, headers=UA, timeout=12)
        if resp.status_code != 200: return None
        data = resp.json()
        pages = (data.get("query") or {}).get("pages") or {}
        for _, p in pages.items():
            infos = p.get("imageinfo") or []
            if not infos: continue
            info = infos[0]
            # url préférée (thumb grand format si dispo)
            url = info.get("responsiveUrls",{}).get("2") or info.get("thumburl") or info.get("url")
            if not url: continue
            meta = info.get("extmetadata") or {}
            lic = (meta.get("LicenseShortName") or {}).get("value","").lower()
            lic_url = (meta.get("LicenseUrl") or {}).get("value")
            artist = (meta.get("Artist") or {}).get("value") or ""
            # nettoyage auteur éventuel
            artist = re.sub(r"<[^>]+>","",artist).strip()
            # filtrage licence
            if lic and any(lic.startswith(x) for x in ACCEPTED_LICENSES) or "public" in lic:
                return {
                    "image": url,
                    "attribution": {
                        "origin": "wikimedia",
                        "source": "Wikimedia Commons",
                        "creator": artist or None,
                        "license": lic or None,
                        "license_url": lic_url or None,
                    }
                }
    except Exception:
        return None
    return None

def make_query(doc):
    parts = []
    name = _name_of(doc)
    if name: parts.append(name)
    loc = _locality_of(doc)
    if loc: parts.append(loc)
    t = doc.get("@type")
    if isinstance(t, list): t = t[0] if t else ""
    if t: parts.append(t)
    return " ".join(parts)

def enrich_doc(doc) -> bool:
    if doc.get("image"):  # déjà illustré
        return False

    q = make_query(doc)
    homepage = _homepage_of(doc)

    providers = []
    for p in PREF_ORDER:
        if p == "og" and homepage:
            providers.append(("og", homepage))
        elif p == "openverse":
            providers.append(("openverse", q))
        elif p == "wikimedia":
            providers.append(("wikimedia", q))

    for provider, param in providers:
        if provider == "og":
            img = fetch_og_image(param)
            if img:
                col.update_one({"_id": doc["_id"]}, {"$set": {
                    "image": img,
                    "image_attribution": {"origin":"og", "source": param}
                }})
                return True
        elif provider == "openverse":
            r = search_openverse(param)
            if r:
                col.update_one({"_id": doc["_id"]}, {"$set": {
                    "image": r["image"],
                    "image_attribution": r["attribution"]
                }})
                return True
        elif provider == "wikimedia":
            r = search_wikimedia(param)
            if r:
                col.update_one({"_id": doc["_id"]}, {"$set": {
                    "image": r["image"],
                    "image_attribution": r["attribution"]
                }})
                return True
    return False

def run(limit=ENRICH_LIMIT):
    q = {"$or":[{"image":{"$exists":False}}, {"image":None}, {"image":""}]}
    fields = {"rdfs:label":1,"@type":1,"isLocatedAt":1,"hasContact":1}
    cur = col.find(q, fields)
    total = col.count_documents(q)
    done = 0
    updated = 0
    for doc in cur:
        if limit and done >= limit: break
        done += 1
        try:
            if enrich_doc(doc):
                updated += 1
        except Exception:
            pass
        if done % 25 == 0:
            print(f"…traités={done}/{total}, enrichis={updated}")
    print(f"✅ Fini. Traités={done}/{total}, enrichis={updated}")

if __name__ == "__main__":
    run()
