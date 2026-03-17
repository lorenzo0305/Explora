import json
import re  # NOUVEAU : La bibliothèque pour chercher des mots exacts
from pathlib import Path

# --- CONFIGURATION DES DOSSIERS ---
BASE_DIR = Path(__file__).resolve().parent
DOSSIER_SOURCE = BASE_DIR / "Auvergne_Rhone_Alpes_object"
FICHIER_SORTIE = BASE_DIR / "Auvergne_Rhone_Alpes_propre.json"

activites_propres = []

print("🔍 Recherche des fichiers JSON...")
fichiers_json = sorted(DOSSIER_SOURCE.rglob("*.json"))
print(f"✅ {len(fichiers_json)} fichiers trouvés ! Début de l'extraction...\n")

# --- FONCTION MAGIQUE POUR LES MOTS ---
def contient_mots(mots, texte):
    # Cette ligne cherche le mot exact, en acceptant un éventuel "s" ou "x" à la fin pour le pluriel
    # Le \b garantit qu'on ne trouve pas "sport" dans "transport"
    pattern = r'\b(' + '|'.join(mots) + r')[sx]?\b'
    return bool(re.search(pattern, texte))

for chemin_fichier in fichiers_json:
    try:
        with open(chemin_fichier, 'r', encoding='utf-8') as f:
            data = json.load(f)

        nom = data.get("rdfs:label", {}).get("fr", [""])[0]

        description = ""
        comments = data.get("rdfs:comment", {})
        if "fr" in comments:
            description = comments.get("fr", [""])[0]
        else:
            has_desc = data.get("hasDescription", [])
            if has_desc and isinstance(has_desc, list):
                short = has_desc[0].get("shortDescription", {})
                if "fr" in short:
                    description = short.get("fr", [""])[0]

        # On crée notre gros texte de recherche
        categories_tags = data.get("@type", [])
        categories_str = " ".join(categories_tags)
        texte_complet = f"{categories_str} {nom} {description}".lower()
        
        # --- NOUVEAU SYSTÈME DE CATÉGORISATION (Mots entiers) ---
        if contient_mots(["ski", "sport", "vélo", "vtt", "rando", "randonnée", "cyclisme", "piscine", "nautique", "raquette", "gym", "gymnase", "stade", "patinoire", "golf", "tennis", "fitness", "équestre"], texte_complet):
            categorie = "sport"
        elif contient_mots(["restaurant", "gastronomie", "brasserie", "snack", "crêperie", "dégustation", "terroir", "boulangerie", "pâtisserie", "traiteur", "glace", "food", "wine"], texte_complet):
            categorie = "gastronomie"
        elif contient_mots(["boutique", "magasin", "librairie", "créateur", "artisanat", "shopping", "store", "épicerie", "souvenir", "achat"], texte_complet):
            categorie = "boutique"
        elif contient_mots(["musée", "château", "histoire", "spectacle", "concert", "théâtre", "patrimoine", "monument", "église", "eglise", "chapelle", "cathédrale", "cathedrale", "basilique", "collégiale", "collegiale", "sanctuaire", "religieux", "abbaye", "culture", "art", "museum", "historic", "exhibition", "bibliothèque", "lecture"], texte_complet):
            categorie = "culture"
        elif contient_mots(["parc", "jardin", "lac", "montagne", "forêt", "plage", "grotte", "cascade", "botanique", "nature", "garden", "lake"], texte_complet):
            categorie = "nature"
        else:
            categorie = "détente"

        # (La suite ne change pas)
        ville, cp, adresse, region, lat, lon = "", "", "", "", "", ""
        is_located = data.get("isLocatedAt", [])
        
        if is_located and isinstance(is_located, list):
            loc = is_located[0]
            
            adresse_info = loc.get("schema:address", [])
            if adresse_info and isinstance(adresse_info, list):
                addr = adresse_info[0]
                ville = addr.get("schema:addressLocality", "")
                cp = addr.get("schema:postalCode", "")
                
                street = addr.get("schema:streetAddress", [])
                if street: adresse = street[0]

                try:
                    region = addr.get("hasAddressCity", {}).get("isPartOfDepartment", {}).get("isPartOfRegion", {}).get("rdfs:label", {}).get("fr", [""])[0]
                except:
                    pass

            geo = loc.get("schema:geo", {})
            if geo:
                lat = geo.get("schema:latitude", "")
                lon = geo.get("schema:longitude", "")

        tel, site_web = "", ""
        contacts = data.get("hasContact", [])
        if contacts and isinstance(contacts, list):
            contact = contacts[0]
            tels = contact.get("schema:telephone", [])
            if tels: tel = tels[0]

            webs = contact.get("foaf:homepage", [])
            if webs: site_web = webs[0]

        activite = {
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
            "description": description
        }

        activites_propres.append(activite)

    except Exception as e:
        continue

print("💾 Sauvegarde en cours...")
with open(FICHIER_SORTIE, 'w', encoding='utf-8') as f_out:
    json.dump(activites_propres, f_out, ensure_ascii=False, indent=4)

print("-" * 40)
print(f"🎉 SUCCÈS ! {len(activites_propres)} activités ont été catégorisées intelligemment.")