import json
import os

FICHIER_INDEX = "/workspaces/Explora/data/Auvergne-Rhône-Alpes-categorized.json"
DOSSIER_OBJECTS = "/workspaces/Explora/data/full_france_object/objects"

print(f"🚀 Chargement du fichier : {FICHIER_INDEX}")

with open(FICHIER_INDEX, 'r', encoding='utf-8') as f:
    index_data = json.load(f)

print(f"📊 {len(index_data)} entrées trouvées")

compteur_succes = 0
compteur_sans_ville = 0
compteur_fichiers_manquants = 0

print(f"🏙️  Extraction des villes...")

for i, item in enumerate(index_data):
    chemin_relatif = item.get('file')
    if not chemin_relatif:
        continue
    
    chemin_complet = os.path.join(DOSSIER_OBJECTS, chemin_relatif)
    
    try:
        with open(chemin_complet, 'r', encoding='utf-8') as f_detail:
            data = json.load(f_detail)
        
        # Extraire la ville
        ville = None
        code_postal = None
        
        if 'isLocatedAt' in data and isinstance(data['isLocatedAt'], list):
            for location in data['isLocatedAt']:
                addr_data = location.get('schema:address', [])
                if addr_data and isinstance(addr_data, list):
                    for addr in addr_data:
                        ville_temp = addr.get('schema:addressLocality', '')
                        cp_temp = addr.get('schema:postalCode', '')
                        
                        if ville_temp:
                            ville = ville_temp
                            code_postal = cp_temp
                            break
                    
                    if ville:
                        break
        
        # Ajouter la ville à l'objet
        if ville:
            item['ville'] = ville
            if code_postal:
                item['codePostal'] = code_postal
            compteur_succes += 1
        else:
            item['ville'] = None
            item['codePostal'] = None
            compteur_sans_ville += 1
        
        # Afficher la progression tous les 500 éléments
        if (i + 1) % 500 == 0:
            print(f"  Traité {i + 1}/{len(index_data)}...")
    
    except FileNotFoundError:
        item['ville'] = None
        item['codePostal'] = None
        compteur_fichiers_manquants += 1
    except Exception as e:
        item['ville'] = None
        item['codePostal'] = None
        print(f"⚠️  Erreur sur {chemin_relatif}: {e}")

# Sauvegarder le fichier mis à jour
print(f"\n💾 Sauvegarde du fichier...")
with open(FICHIER_INDEX, 'w', encoding='utf-8') as f:
    json.dump(index_data, f, ensure_ascii=False, indent=2)

# Résultats
print("=" * 60)
print(f"✅ Terminé !")
print(f"🏙️  Activités avec ville : {compteur_succes}")
print(f"⚠️  Activités sans ville : {compteur_sans_ville}")
print(f"❌ Fichiers manquants : {compteur_fichiers_manquants}")
print(f"📁 Fichier mis à jour : {FICHIER_INDEX}")
print("=" * 60)

# Afficher quelques exemples
print("\n🏙️  Exemples avec ville :")
exemples_avec = [item for item in index_data if item.get('ville')][:5]
for ex in exemples_avec:
    if ex.get('codePostal'):
        print(f"  • {ex['label']} - {ex['ville']} ({ex['codePostal']})")
    else:
        print(f"  • {ex['label']} - {ex['ville']}")
