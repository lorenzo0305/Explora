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

print(f"🕵️‍♂️ Extraction SANS FILTRE en cours...")

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
            desc = "" # On commence vide
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

            # SI PAS DE DESCRIPTION : On met un marqueur pour l'IA plus tard
            if not desc:
                desc = "A_GENERER_VIA_IA"

            # 3. IMAGE
            image_url = ""
            main_rep = data.get('hasMainRepresentation', [])
            if main_rep and isinstance(main_rep, list):
                resource = main_rep[0].get('ebucore:hasRelatedResource', [])
                if resource and isinstance(resource, list):
                    image_url = resource[0].get('ebucore:locator')

            # ON GARDE TOUT !
            liste_activites.append({
                "nom": nom,
                "categorie": item.get('category', {}).get('label'),
                "description": desc,
                "image": image_url,
                "fichier_source": chemin_relatif
            })
            compteur_succes += 1

    except FileNotFoundError:
        # C'est ici que tes 2000 fichiers disparaissent
        compteur_fichiers_manquants += 1
    except Exception as e:
        pass

# --- RÉSULTAT ---
print("-" * 30)
print(f"Terminé !")
print(f"✅ Activités dans le CSV : {len(liste_activites)}")
print(f"❌ Fichiers introuvables sur le disque : {compteur_fichiers_manquants}")

df = pd.DataFrame(liste_activites)
chemin_sortie = "/workspaces/Explora/data/Auvergne_Rhone_Alpes_V2.csv"
df.to_csv(chemin_sortie, index=False, sep=";", encoding='utf-8-sig')
print(f"💾 Sauvegardé : {chemin_sortie}")
