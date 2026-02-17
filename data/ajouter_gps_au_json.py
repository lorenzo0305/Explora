import json
import os

FICHIER_INDEX = "/workspaces/Explora/data/Auvergne-Rhône-Alpes-categorized.json"
DOSSIER_OBJECTS = "/workspaces/Explora/data/full_france_object/objects"

print(f"🚀 Chargement du fichier : {FICHIER_INDEX}")

with open(FICHIER_INDEX, 'r', encoding='utf-8') as f:
    index_data = json.load(f)

print(f"📊 {len(index_data)} entrées trouvées")

compteur_succes = 0
compteur_sans_coords = 0
compteur_fichiers_manquants = 0

print(f"🔍 Extraction des coordonnées GPS...")

for i, item in enumerate(index_data):
    chemin_relatif = item.get('file')
    if not chemin_relatif:
        continue
    
    chemin_complet = os.path.join(DOSSIER_OBJECTS, chemin_relatif)
    
    try:
        with open(chemin_complet, 'r', encoding='utf-8') as f_detail:
            data = json.load(f_detail)
        
        # Extraire les coordonnées GPS
        latitude = None
        longitude = None
        
        if 'isLocatedAt' in data and isinstance(data['isLocatedAt'], list):
            for location in data['isLocatedAt']:
                geo = location.get('schema:geo')
                if geo:
                    lat = geo.get('schema:latitude', '')
                    lon = geo.get('schema:longitude', '')
                    if lat and lon:
                        latitude = lat
                        longitude = lon
                        break
        
        # Ajouter les coordonnées à l'objet (ou None si non trouvées)
        if latitude and longitude:
            item['latitude'] = latitude
            item['longitude'] = longitude
            compteur_succes += 1
        else:
            item['latitude'] = None
            item['longitude'] = None
            compteur_sans_coords += 1
        
        # Afficher la progression tous les 500 éléments
        if (i + 1) % 500 == 0:
            print(f"  Traité {i + 1}/{len(index_data)}...")
    
    except FileNotFoundError:
        item['latitude'] = None
        item['longitude'] = None
        compteur_fichiers_manquants += 1
    except Exception as e:
        item['latitude'] = None
        item['longitude'] = None
        print(f"⚠️  Erreur sur {chemin_relatif}: {e}")

# Sauvegarder le fichier mis à jour
print(f"\n💾 Sauvegarde du fichier...")
with open(FICHIER_INDEX, 'w', encoding='utf-8') as f:
    json.dump(index_data, f, ensure_ascii=False, indent=2)

# Résultats
print("=" * 60)
print(f"✅ Terminé !")
print(f"📍 Activités avec coordonnées GPS : {compteur_succes}")
print(f"⚠️  Activités sans coordonnées : {compteur_sans_coords}")
print(f"❌ Fichiers manquants : {compteur_fichiers_manquants}")
print(f"📁 Fichier mis à jour : {FICHIER_INDEX}")
print("=" * 60)

# Afficher quelques exemples
print("\n📍 Exemples avec coordonnées :")
exemples_avec = [item for item in index_data if item.get('latitude')][:3]
for ex in exemples_avec:
    print(f"  • {ex['label']} - GPS: {ex['latitude']}, {ex['longitude']}")
