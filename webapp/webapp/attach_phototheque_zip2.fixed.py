
# attach_phototheque_zip.py
# Associe les images Photothèque ARA (déjà téléchargées) à tes objets Mongo
# via le fichier Excel de la photothèque (métadonnées).
#
# Prérequis : pip install pandas openpyxl rapidfuzz pymongo

import os
import re
import csv
import argparse
import unicodedata
from typing import Tuple
from pathlib import Path

import pandas as pd
from rapidfuzz import fuzz, process
from pymongo import MongoClient
from bson import ObjectId


# ---------------- Normalisation ----------------

def _strip_accents(s: str) -> str:
    if not s:
        return ""
    s = s.replace("œ", "oe").replace("Œ", "Oe").replace("æ", "ae").replace("Æ", "Ae")
    return unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")


def norm_txt(s: str) -> str:
    """
    Normalisation robuste pour comparer titres/labels et villes :
      - minuscules
      - normalise st./ste. -> saint/sainte
      - uniformise apostrophes
      - supprime accents
      - supprime ponctuation (sauf tiret et parenthèses pour conserver les (xx))
      - espaces compacts
    """
    s = (s or "").lower()
    s = s.replace("’", "'").replace("`", "'")
    s = re.sub(r"\bst[.-]?\b", "saint", s)
    s = re.sub(r"\bste[.-]?\b", "sainte", s)
    s = _strip_accents(s)
    s = re.sub(r"[^\w\s\-()]", " ", s, flags=re.U)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def parse_dept_from_title(title: str) -> str:
    m = re.search(r"\((\d{2})\)", title or "")
    return m.group(1) if m else ""


# ---------------- Lecture Excel Photothèque ----------------

def load_phototheque_xlsx(xlsx_path: str):
    df = pd.read_excel(xlsx_path)
    cols = {str(c).strip(): c for c in df.columns}

    def pick(*cands):
        for c in cands:
            if c in cols:
                return cols[c]
        return None

    col_file = pick("Nom du fichier", "Fichier", "filename", "Fichier source", "File")
    col_title = pick("Titre", "title", "Libellé", "Libelle", "Nom")
    col_city  = pick("Ville", "Commune", "City", "Localité", "Localite")
    col_credit = pick("Copyright", "Crédit photo", "credit", "Crédit", "Credits")
    col_rights = pick("Date de fin de droits", "Fin droits", "rights_end", "Date fin droits")

    if not (col_file and col_title and col_credit):
        raise ValueError("Colonnes Excel attendues manquantes (Nom du fichier / Titre / Copyright)")

    out = []
    for _, r in df.iterrows():
        filename = str(r.get(col_file, "")).strip()
        title = str(r.get(col_title, "")).strip()
        city = str(r.get(col_city, "")).strip() if col_city else ""
        credit = str(r.get(col_credit, "")).strip()
        rights = str(r.get(col_rights, "")).strip() if col_rights else ""
        out.append({
            "filename_base": filename,
            "title": title,
            "title_norm": norm_txt(title),
            "city": city,
            "city_norm": norm_txt(city),
            "dept": parse_dept_from_title(title),
            "credit": credit,
            "rights_end": rights,
        })
    return out


# ---------------- Indexation des images ----------------

def index_images(images_dir: str):
    exts = {".jpg", ".jpeg", ".png", ".webp", ".gif", ".tif", ".tiff"}
    files = []
    for root, _, fnames in os.walk(images_dir):
        for fn in fnames:
            ext = os.path.splitext(fn)[1].lower()
            if ext in exts:
                files.append(os.path.join(root, fn))
    by_lower_name = {os.path.basename(p).lower(): p for p in files}
    return files, by_lower_name


def find_best_file_for_base(base_name: str, by_lower_name: dict) -> str:
    base = os.path.basename(base_name).lower()
    base_noext = os.path.splitext(base)[0]
    if base in by_lower_name:
        return by_lower_name[base]
    for lower_name, path in by_lower_name.items():
        if lower_name.startswith(base_noext):
            return path
    candidates = list(by_lower_name.keys())
    best = process.extractOne(base_noext, candidates, scorer=fuzz.WRatio)
    if best and best[1] >= 90:
        return by_lower_name[best[0]]
    return ""


# ---------------- Extraction des champs utiles Mongo ----------------

def label_fr(doc):
    lab = (doc.get("rdfs:label") or {})
    if isinstance(lab, dict):
        for k in ("fr", "fr-FR", "en"):
            v = lab.get(k)
            if isinstance(v, list) and v:
                return v[0]
            if isinstance(v, str) and v.strip():
                return v
    if isinstance(lab, str) and lab.strip():
        return lab
    return doc.get("name") or ""


def locality(doc):
    try:
        return doc["isLocatedAt"][0]["schema:address"][0]["schema:addressLocality"]
    except Exception:
        return ""


# ---------------- Scoring Titre+Ville ↔ Label+Localité ----------------

def exact_title_city_match(photo: dict, name_norm: str, loc_norm: str) -> bool:
    # Match strict du titre + (si ville Excel présente) ville == localité
    if photo.get("title_norm") != name_norm:
        return False
    if photo.get("city_norm"):
        return photo["city_norm"] == (loc_norm or "")
    return True


def joint_title_city_score(photo: dict, name_norm: str, loc_norm: str) -> float:
    a = (photo.get("title_norm", "") + " " + photo.get("city_norm", "")).strip()
    b = (name_norm + " " + (loc_norm or "")).strip()
    if not a or not b:
        return 0.0
    return float(fuzz.WRatio(a, b))


def title_score(a_norm: str, b_norm: str) -> float:
    # Combine quelques variantes robustes
    r1 = fuzz.WRatio(a_norm, b_norm)
    r2 = fuzz.token_set_ratio(a_norm, b_norm)
    r3 = fuzz.partial_ratio(a_norm, b_norm)
    return float(max(r1, r2, r3))


def composite_score(photo: dict, name_norm: str, loc_norm: str) -> Tuple[float, dict]:
    # 0) Garde-fou de ville : si deux villes claires mais trop différentes -> 0
    if photo.get("city_norm") and loc_norm:
        r_city = fuzz.WRatio(photo["city_norm"], loc_norm)
        if r_city < 70:
            return 0.0, {"reason": "city_mismatch", "city_sim": float(r_city)}

    # 1) Exact match prioritaire
    if exact_title_city_match(photo, name_norm, loc_norm):
        return 100.0, {"exact": 100.0}

    # 2) Fuzzy joint + titre seul
    joint = joint_title_city_score(photo, name_norm, loc_norm)  # 0..100
    t = title_score(photo.get("title_norm", ""), name_norm)     # 0..100

    score_raw = 0.80 * joint + 0.20 * t
    return min(100.0, float(score_raw)), {"joint": joint, "title": t}


# ---------------- Main ----------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--images-dir",
        default="/app/webapp/static/img/phototheque",
        help="Dossier où TU as extrait les fichiers images",
    )
    ap.add_argument(
        "--xlsx",
        default="/app/webapp/static/img/phototheque/phototheque.auvergnerhonealpes-tourisme.com-20250909-143729.xlsx",
        help="Fichier Excel de la photothèque (métadonnées)",
    )
    # ⬇️ default adapté au docker-compose
    ap.add_argument("--mongo", default="mongodb://mongo:27017/LORA_voyage")
    ap.add_argument("--collection", default="objects")
    ap.add_argument("--region", default="Auvergne-Rhône-Alpes", help="Filtrer les objets par région")
    ap.add_argument(
        "--base-url",
        default="/static/img/phototheque",
        help="URL publique qui correspond à --images-dir (servi par FastAPI /static)",
    )
    ap.add_argument("--min-score", type=int, default=85, help="Score minimum pour accepter le match")
    ap.add_argument("--topk", type=int, default=1, help="Nb max de photos à associer par objet")
    ap.add_argument("--force", action="store_true", help="Ecraser l'image existante (si déjà présente)")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--out", default="matches_out.csv", help="CSV de sortie des matches (trace)")
    args = ap.parse_args()

    # 1) Images + Excel
    imgs, by_lower = index_images(args.images_dir)
    print(f"🖼  Fichiers images trouvés: {len(imgs)}")

    photos = load_phototheque_xlsx(args.xlsx)
    print(f"📑 Lignes Excel (photos): {len(photos)}")

    for p in photos:
        real = find_best_file_for_base(p["filename_base"], by_lower)
        p["file_path"] = real
        p["file_name_actual"] = os.path.basename(real) if real else ""
        p["file_found"] = bool(real)
        # Chaîne jointe normalisée : Titre + Ville (pour matching rapide)
        p["joint_norm"] = (p["title_norm"] + " " + p.get("city_norm", "")).strip()

    missing = sum(1 for p in photos if not p["file_found"])
    if missing:
        print(f"⚠️ {missing} fichiers référencés dans l'Excel non trouvés sur disque (ils seront ignorés).")

    # 2) Connexion Mongo
    client = MongoClient(args.mongo)
    db = client.get_database()
    col = db[args.collection]

    # ✅ Requête région robuste : anciens chemins imbriqués OU nouveaux champs plats
    q = {}
    if args.region:
        q = {
            "$or": [
                {"hasBeenCreatedBy.schema:address.hasAddressCity.isPartOfDepartment.isPartOfRegion.rdfs:label.fr": args.region},
                {"isLocatedAt.schema:address.isPartOfDepartment.isPartOfRegion.rdfs:label.fr": args.region},
                {"region": args.region},
                {"_regions": args.region},
            ]
        }

    fields = {
        "@id": 1,
        "rdfs:label": 1,
        "isLocatedAt": 1,
        "image": 1,
        "region": 1,
        "_regions": 1,
    }
    objs = list(col.find(q, fields))
    print(f"📦 Objets chargés: {len(objs)}")

    # 3) Prépare la liste des choix pour fuzzy (index conservé !)
    found_idx = [i for i, p in enumerate(photos) if p["file_found"]]
    photo_choices = [photos[i]["joint_norm"] for i in found_idx]

    rows_out = []

    # 4) Matching pour chaque objet
    for doc in objs:
        obj_id = doc.get("@id") or str(doc.get("_id"))
        name = label_fr(doc)
        loc = locality(doc)

        name_norm = norm_txt(name)
        loc_norm = norm_txt(loc)

        obj_joint = (name_norm + " " + loc_norm).strip()

        # Pré-sélection des meilleurs candidats par fuzzy joint
        candidates = process.extract(obj_joint, photo_choices, scorer=fuzz.WRatio, limit=20)

        # Scoring précis composite
        scored = []
        for _, fast_score, choice_idx in candidates:
            p = photos[found_idx[choice_idx]]
            s, br = composite_score(p, name_norm, loc_norm)
            # Conserver le meilleur des deux pour robustesse
            s = max(s, float(fast_score))
            scored.append((p, min(100.0, s), br))

        scored.sort(key=lambda x: x[1], reverse=True)
        kept = [s for s in scored if s[1] >= args.min_score][: args.topk]

        for p, s, br in kept:
            public_url = args.base_url.rstrip("/") + "/" + p["file_name_actual"]
            attribution = {
                "origin": "Photothèque Auvergne-Rhône-Alpes Tourisme",
                "source": "",
                "creator": p["credit"],
                "license": "",
                "license_url": "",
                "ref": p["filename_base"],
                "filename": p["file_name_actual"],
                "rights_end": p["rights_end"],
            }

            if (doc.get("image") and not args.force):
                action = "skip_has_image"
            else:
                action = "update"
                q_upd = (
                    {"@id": obj_id} if (isinstance(obj_id, str) and (obj_id.startswith("http") or "/" in obj_id))
                    else {"_id": ObjectId(obj_id)} if ObjectId.is_valid(obj_id)
                    else {"@id": obj_id}
                )
                upd = {"$set": {"image": public_url, "image_attribution": attribution}}
                if not args.dry_run:
                    col.update_one(q_upd, upd)

            rows_out.append({
                "object_id": obj_id,
                "object_name": name,
                "object_locality": loc,
                "photo_title": p["title"],
                "photo_city": p.get("city", ""),
                "photo_file": p["file_name_actual"],
                "score": s,
                "action": action,
                "image_url": public_url if action == "update" else (doc.get("image") or ""),
                "credit": p["credit"],
                "rights_end": p["rights_end"],
                "breakdown": br,
            })

    # 5) Export trace CSV
    if rows_out:
        with open(args.out, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=list(rows_out[0].keys()))
            w.writeheader()
            w.writerows(rows_out)
        print(f"✅ Matches exportés -> {args.out} ({len(rows_out)} lignes)")
    else:
        print("ℹ️ Aucun match accepté (revois --min-score, --region, etc.)")

    print("ℹ️ Si besoin, relance ensuite ton indexeur ES pour propager l'image : es_indexer.py")


if __name__ == "__main__":
    main()
