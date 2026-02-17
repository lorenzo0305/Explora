import json
import os
import pandas as pd

FICHIER_INDEX = "/workspaces/Explora/data/Auvergne-Rhône-Alpes-categorized.json"
DOSSIER_OBJECTS = "/workspaces/Explora/data/full_france_object/objects"

print(f"🚀 Démarrage... Lecture de la liste : {FICHIER_INDEX}")

with open(FICHIER_INDEX, 'r', encoding='utf-8') as f:
    index_data = json.load(f)

liste_activites = []
compteur_succes = 0
compteur_fichiers_manquants = 0
compteur_avec_coords = 0

print(f"🕵️‍♂️ Extraction avec coordonnées GPS en cours...")

for item in index_data:
    chemin_relatif = item.get('file')
    if not chemin_relatif:
        continue

    chemin_complet = os.path.join(DOSSIER_OBJECTS, chemin_relatif)

    try:
        with open(chemin_complet, 'r', encoding='utf-8') as f_detail:
            data = json.load(f_detail)

            # 1. NOM
            nom = item.get('label')
            if not nom:
                nom = data.get('rdfs:label', {}).get('fr', ['Sans nom'])[0]

            # 2. DESCRIPTION
            desc = ""
            comments = data.get('rdfs:comment')
            if isinstance(comments, dict):
                desc_list = comments.get('fr')
                if desc_list and isinstance(desc_list, list) and len(desc_list) > 0:
                    desc = desc_list[0]

            if not desc:
                has_desc = data.get('hasDescription')
                if has_desc and isinstance(has_desc, list):
                    short_desc = has_desc[0].get('shortDescription')
                    if isinstance(short_desc, dict):
                        desc_list = short_desc.get('fr')
                        if desc_list:
                            desc = desc_list[0]

            if not desc:
                desc = "A_GENERER_VIA_IA"

            # 3. IMAGE
            image_url = ""
            main_rep = data.get('hasMainRepresentation', [])
            if main_rep and isinstance(main_rep, list):
                resource = main_rep[0].get('ebucore:hasRelatedResource', [])
                if resource and isinstance(resource, list):
                    image_url = resource[0].get('ebucore:locator')

            # 4. COORDONNÉES GPS (NOUVEAU!)
            latitude = ""
            longitude = ""
            
            # Chercher dans isLocatedAt
            if 'isLocatedAt' in data and isinstance(data['isLocatedAt'], list):
                for location in data['isLocatedAt']:
                    geo = location.get('schema:geo')
                    if geo:
                        latitude = geo.get('schema:latitude', '')
                        longitude = geo.get('schema:longitude', '')
                        if latitude and longitude:
                            compteur_avec_coords += 1
                            break

            # ON GARDE TOUT !
            liste_activites.append({
                "nom": nom,
                "categorie": item.get('category', {}).get('label'),
                "description": desc,
                "image": image_url,
                "latitude": latitude,
                "longitude": longitude,
                "fichier_source": chemin_relatif
            })
            compteur_succes += 1

    except FileNotFoundError:
        compteur_fichiers_manquants += 1
    except Exception as e:
        print(f"⚠️ Erreur sur {chemin_relatif}: {e}")

# --- RÉSULTAT ---
print("-" * 60)
print(f"Terminé !")
print(f"✅ Activités dans le CSV : {len(liste_activites)}")
print(f"🗺️  Activités avec coordonnées GPS : {compteur_avec_coords} ({compteur_avec_coords*100//len(liste_activites) if liste_activites else 0}%)")
print(f"❌ Fichiers introuvables sur le disque : {compteur_fichiers_manquants}")

df = pd.DataFrame(liste_activites)
chemin_sortie = "/workspaces/Explora/data/Auvergne_Rhone_Alpes_avec_GPS.csv"
df.to_csv(chemin_sortie, index=False, sep=";", encoding='utf-8-sig')
print(f"💾 Sauvegardé : {chemin_sortie}")
print("-" * 60)

# Afficher quelques exemples
print("\n📍 Exemples d'activités avec coordonnées :")
exemples = df[df['latitude'] != ''].head(5)
for idx, row in exemples.iterrows():
    print(f"  • {row['nom']} - GPS: {row['latitude']}, {row['longitude']}")
