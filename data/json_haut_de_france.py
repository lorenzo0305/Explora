import argparse
import json
import re
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
DEFAULT_SOURCE_DIR = BASE_DIR / "haut_de_france_objects"
DEFAULT_OUTPUT_FILE = BASE_DIR / "Hauts-de-France-propre.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Extrait et categorise les activites Hauts-de-France depuis des objets JSON."
    )
    parser.add_argument(
        "--source-dir",
        default=str(DEFAULT_SOURCE_DIR),
        help=f"Dossier source des objets JSON (defaut: {DEFAULT_SOURCE_DIR})",
    )
    parser.add_argument(
        "--output",
        default=str(DEFAULT_OUTPUT_FILE),
        help=f"Fichier JSON de sortie (defaut: {DEFAULT_OUTPUT_FILE})",
    )
    return parser.parse_args()


def contient_mots(mots: list[str], texte: str) -> bool:
    escaped = [re.escape(mot) for mot in mots]
    pattern = r"\b(" + "|".join(escaped) + r")[sx]?\b"
    return bool(re.search(pattern, texte, flags=re.IGNORECASE))


def pick_first(value, default=""):
    if isinstance(value, list) and value:
        return value[0]
    if isinstance(value, str):
        return value
    return default


def detect_categorie(texte_complet: str) -> str:
    if contient_mots(
        [
            "ski",
            "sport",
            "velo",
            "vélo",
            "vtt",
            "rando",
            "randonnée",
            "cyclisme",
            "piscine",
            "nautique",
            "raquette",
            "gym",
            "gymnase",
            "stade",
            "patinoire",
            "golf",
            "tennis",
            "fitness",
            "equestre",
            "équestre",
        ],
        texte_complet,
    ):
        return "sport"
    if contient_mots(
        [
            "restaurant",
            "gastronomie",
            "brasserie",
            "snack",
            "creperie",
            "crêperie",
            "degustation",
            "dégustation",
            "terroir",
            "boulangerie",
            "patisserie",
            "pâtisserie",
            "traiteur",
            "glace",
            "food",
            "wine",
        ],
        texte_complet,
    ):
        return "gastronomie"
    if contient_mots(
        [
            "boutique",
            "magasin",
            "librairie",
            "createur",
            "créateur",
            "artisanat",
            "shopping",
            "store",
            "epicerie",
            "épicerie",
            "souvenir",
            "achat",
        ],
        texte_complet,
    ):
        return "boutique"
    if contient_mots(
        [
            "musee",
            "musée",
            "chateau",
            "château",
            "histoire",
            "spectacle",
            "concert",
            "theatre",
            "théâtre",
            "patrimoine",
            "monument",
            "eglise",
            "église",
            "chapelle",
            "cathedrale",
            "cathédrale",
            "basilique",
            "collegiale",
            "collégiale",
            "sanctuaire",
            "religieux",
            "abbaye",
            "culture",
            "art",
            "museum",
            "historic",
            "exhibition",
            "bibliotheque",
            "bibliothèque",
            "lecture",
        ],
        texte_complet,
    ):
        return "culture"
    if contient_mots(
        [
            "parc",
            "jardin",
            "lac",
            "montagne",
            "foret",
            "forêt",
            "plage",
            "grotte",
            "cascade",
            "botanique",
            "nature",
            "garden",
            "lake",
        ],
        texte_complet,
    ):
        return "nature"
    return "detente"


def extract_activite(data: dict) -> dict:
    nom = pick_first(data.get("rdfs:label", {}).get("fr", [""]))

    description = ""
    comments = data.get("rdfs:comment", {})
    if "fr" in comments:
        description = pick_first(comments.get("fr", [""]))
    else:
        has_desc = data.get("hasDescription", [])
        if has_desc and isinstance(has_desc, list):
            short = has_desc[0].get("shortDescription", {})
            if "fr" in short:
                description = pick_first(short.get("fr", [""]))

    categories_tags = data.get("@type", [])
    categories_str = " ".join(categories_tags) if isinstance(categories_tags, list) else ""
    texte_complet = f"{categories_str} {nom} {description}".lower()
    categorie = detect_categorie(texte_complet)

    ville, cp, adresse, region, lat, lon = "", "", "", "Hauts-de-France", "", ""
    is_located = data.get("isLocatedAt", [])
    if is_located and isinstance(is_located, list):
        loc = is_located[0]

        adresse_info = loc.get("schema:address", [])
        if adresse_info and isinstance(adresse_info, list):
            addr = adresse_info[0]
            ville = addr.get("schema:addressLocality", "")
            cp = addr.get("schema:postalCode", "")

            street = addr.get("schema:streetAddress", [])
            adresse = pick_first(street)

            region_val = (
                addr.get("hasAddressCity", {})
                .get("isPartOfDepartment", {})
                .get("isPartOfRegion", {})
                .get("rdfs:label", {})
                .get("fr", [""])
            )
            region_extracted = pick_first(region_val)
            if region_extracted:
                region = region_extracted

        geo = loc.get("schema:geo", {})
        if geo and isinstance(geo, dict):
            lat = geo.get("schema:latitude", "")
            lon = geo.get("schema:longitude", "")

    tel, site_web = "", ""
    contacts = data.get("hasContact", [])
    if contacts and isinstance(contacts, list):
        contact = contacts[0]
        tel = pick_first(contact.get("schema:telephone", []))
        site_web = pick_first(contact.get("foaf:homepage", []))

    return {
        "id": data.get("@id", ""),
        "nom": nom,
        "categorie": categorie,
        "adresse": adresse,
        "code_postal": cp,
        "ville": ville,
        "region": region,
        "latitude": lat,
        "longitude": lon,
        "telephone": tel,
        "site_internet": site_web,
        "description": description,
    }


def main() -> int:
    args = parse_args()
    source_dir = Path(args.source_dir).expanduser().resolve()
    output_file = Path(args.output).expanduser().resolve()

    if not source_dir.exists() or not source_dir.is_dir():
        print(f"Source introuvable ou invalide: {source_dir}")
        return 1

    print("Recherche des fichiers JSON...")
    fichiers_json = sorted(source_dir.rglob("*.json"))
    print(f"{len(fichiers_json)} fichiers trouves. Extraction en cours...")

    activites_propres = []
    erreurs = 0

    for chemin_fichier in fichiers_json:
        try:
            with chemin_fichier.open("r", encoding="utf-8") as handle:
                data = json.load(handle)
            if isinstance(data, dict):
                activites_propres.append(extract_activite(data))
        except Exception:
            erreurs += 1

    output_file.parent.mkdir(parents=True, exist_ok=True)
    with output_file.open("w", encoding="utf-8") as out:
        json.dump(activites_propres, out, ensure_ascii=False, indent=2)

    print("-" * 40)
    print(f"Termine: {len(activites_propres)} activites ecrites dans {output_file}")
    if erreurs:
        print(f"Fichiers ignores (erreur lecture/parse): {erreurs}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())