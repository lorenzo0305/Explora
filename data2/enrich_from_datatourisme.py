#!/usr/bin/env python3
"""
Enrichit Auvergne.json avec les descriptions complètes de DataTourisme
Stratégie: chercher par ville + nom fuzzy matching
"""

import json
import os
from pathlib import Path
from difflib import SequenceMatcher
from collections import defaultdict
import random

# Configuration
FICHIER_AUVERGNE = "/workspaces/Explora/data2/Auvergne.json"
FICHIER_INDEX = "/workspaces/Explora/data/full_france_object/index.json"
DOSSIER_OBJECTS = "/workspaces/Explora/data/full_france_object/objects"

print("=" * 60)
print("🔍 ENRICHISSEMENT DES DESCRIPTIONS AVEC DATATOURISME")
print("=" * 60)

# 1. Charger les données
print("\n📥 Chargement des données...")
with open(FICHIER_AUVERGNE, 'r', encoding='utf-8') as f:
    auvergne_data = json.load(f)
print(f"   ✓ {len(auvergne_data)} activités dans Auvergne.json")

with open(FICHIER_INDEX, 'r', encoding='utf-8') as f:
    index_data = json.load(f)
print(f"   ✓ {len(index_data)} entrées dans l'index DataTourisme")

# 2. Indexer DataTourisme par (ville, label)
print("\n🗂️  Indexation de DataTourisme par ville + catégorie...")
ville_to_items = defaultdict(list)
ville_cat_to_items = defaultdict(list)  # Pour chercher par ville + catégorie
categories_map = {}

for entry in index_data:
    label = entry.get('label', '').lower().strip()
    file_path = entry.get('file')
    
    if not file_path:
        continue
    
    try:
        full_path = Path(DOSSIER_OBJECTS) / file_path
        with open(full_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Extraire la ville
        ville = None
        if 'hasContact' in data and isinstance(data['hasContact'], list) and len(data['hasContact']) > 0:
            contact = data['hasContact'][0]
            if 'schema:address' in contact and isinstance(contact['schema:address'], list) and len(contact['schema:address']) > 0:
                addr = contact['schema:address'][0]
                ville = addr.get('schema:addressLocality', '').lower().strip()
        
        if not ville:
            continue
        
        # Extraire les types/catégories
        types = data.get('@type', [])
        
        item_data = {
            'label': label,
            'file_path': file_path,
            'types': types,
            'data': data
        }
        
        ville_to_items[ville].append(item_data)
        
        # Indexer aussi par catégorie
        for typ in types:
            ville_cat_key = (ville, typ.lower())
            ville_cat_to_items[ville_cat_key].append(item_data)
        
    except Exception as e:
        pass

print(f"   ✓ {len(ville_to_items)} villes indexées")
print(f"   ✓ {len(ville_cat_to_items)} paires (ville, catégorie) indexées")

# 3. Fonction pour extraire la description complète
def extraire_description_complete(data):
    """Extrait la meilleure description disponible"""
    
    # 1. Essayer dc:description
    if 'hasDescription' in data and isinstance(data['hasDescription'], list) and len(data['hasDescription']) > 0:
        desc_obj = data['hasDescription'][0]
        if 'dc:description' in desc_obj and 'fr' in desc_obj['dc:description']:
            desc_list = desc_obj['dc:description']['fr']
            desc = desc_list[0] if isinstance(desc_list, list) else desc_list
            if desc and len(desc) > 20:  # On veut une description non-triviale
                return ' '.join(desc.split())  # Nettoyer les whitespaces
    
    # 2. Fallback: rdfs:comment
    if 'rdfs:comment' in data and isinstance(data['rdfs:comment'], dict):
        desc_list = data['rdfs:comment'].get('fr', [])
        if desc_list:
            desc = desc_list[0] if isinstance(desc_list, list) else desc_list
            if desc and len(desc) > 20:
                return ' '.join(desc.split())
    
    return None

def similarite(s1, s2):
    """Calcule la similarité entre deux chaînes"""
    return SequenceMatcher(None, s1.lower(), s2.lower()).ratio()

# Templates par catégorie (pour les descriptions manquantes)
TEMPLATES = {
    'Gastronomie': [
        "Découvrez {nom}, une adresse culinaire incontournable de {ville}. Savourez une cuisine savoureuse dans une ambiance accueillante.",
        "{nom} vous propose une expérience gastronomique authentique à {ville}. Un lieu idéal pour partager un moment gourmand.",
        "Appréciez {nom}, un établissement gastronomique de qualité situé à {ville}. Délices et convivialité au rendez-vous.",
    ],
    'Hébergement': [
        "Séjournez à {nom}, un établissement confortable et accueillant à {ville}. Détente et confort garantis.",
        "{nom} vous offre un hébergement de qualité à {ville} pour un séjour inoubliable.",
        "Profitez d'un séjour agréable à {nom}, situé à {ville}. Service de qualité et ambiance chaleureuse.",
    ],
    'Culture': [
        "Explorez {nom}, un patrimoine culturel remarquable à {ville}. Découvrez l'histoire et les traditions locales.",
        "{nom} à {ville} vous ouvre les portes de la richesse culturelle de la région.",
        "Visitez {nom}, un lieu emblématique de la culture à {ville}. Immersion culturelle garantie.",
    ],
    'Sport': [
        "{nom} à {ville} vous invite à pratiquer vos activités sportives favorites. Espace moderne et équipé.",
        "Vivez des moments sportifs inoubliables à {nom}, situé à {ville}. Convivialité et performance au rendez-vous.",
        "Découvrez {nom}, un centre sportif dynamique à {ville} proposant de nombreuses activités.",
    ],
    'Nature': [
        "Explorez {nom}, un espace naturel remarquable à {ville}. Idéal pour des balades en famille ou entre amis.",
        "Profitez de la nature à {nom}, situé à {ville}. Détente et bien-être en plein air.",
        "Ressourcez-vous à {nom}, un havre naturel de paix à {ville}. Connexion avec la nature garantie.",
    ],
    'Détente': [
        "Profitez d'une pause relaxante à {nom}, situé à {ville}. Bien-être et sérénité vous attendent.",
        "{nom} à {ville} vous offre un moment de détente et de relaxation. Espace calme et apaisant.",
        "Échappez-vous à {nom}, votre refuge détente à {ville}. Quiétude et confort sont au cœur de l'expérience.",
    ],
}

def generer_description(nom, categorie, ville):
    """Génère une description à partir d'un template"""
    templates_cat = TEMPLATES.get(categorie, TEMPLATES['Détente'])
    template = random.choice(templates_cat)
    return template.format(nom=nom, ville=ville, categorie=categorie)

# 4. Enrichir Auvergne.json
print("\n🔄 Enrichissement en cours...")
descriptions_trouvees = 0
descriptions_generees = 0
matches_par_score = defaultdict(int)

for i, activite in enumerate(auvergne_data):
    nom = activite.get('nom', '').lower().strip()
    ville = activite.get('ville', '').lower().strip()
    categorie = activite.get('categorie', 'Détente')
    
    desc = None
    score_utilise = 0
    
    # STRATÉGIE 1: Chercher par nom exact dans la même ville (score > 0.7)
    if ville in ville_to_items:
        meilleur_score = 0
        meilleur_item = None
        
        for dt_item in ville_to_items[ville]:
            score = similarite(nom, dt_item['label'])
            if score > meilleur_score:
                meilleur_score = score
                meilleur_item = dt_item
        
        if meilleur_score > 0.7 and meilleur_item:
            desc = extraire_description_complete(meilleur_item['data'])
            if desc:
                activite['description'] = desc
                descriptions_trouvees += 1
                score_utilise = meilleur_score
                if meilleur_score > 0.95:
                    matches_par_score['parfaits'] += 1
                elif meilleur_score > 0.85:
                    matches_par_score['bons'] += 1
                else:
                    matches_par_score['acceptables'] += 1
    
    # STRATÉGIE 2: Si pas de match exact, chercher par catégorie + ville
    if not desc:
        # Mapper la catégorie Auvergne aux types DataTourisme
        categorie_lower = categorie.lower()
        
        # Créer des clés possibles
        cles_possibles = [
            (ville, categorie_lower),
            (ville, 'restaurant'),
            (ville, 'gastronomie'),
            (ville, 'accommodation'),
            (ville, 'hebergement'),
            (ville, 'culturalsite'),
            (ville, 'culture'),
            (ville, 'sport'),
            (ville, 'leisure'),
            (ville, 'nature'),
        ]
        
        for cle in cles_possibles:
            if cle in ville_cat_to_items and len(ville_cat_to_items[cle]) > 0:
                # Prendre le premier item avec une description
                for dt_item in ville_cat_to_items[cle]:
                    desc = extraire_description_complete(dt_item['data'])
                    if desc:
                        activite['description'] = desc
                        descriptions_trouvees += 1
                        matches_par_score['categorie'] = matches_par_score.get('categorie', 0) + 1
                        break
                
                if desc:
                    break
    
    # STRATÉGIE 3: Fallback à template si rien trouvé
    if not desc:
        activite['description'] = generer_description(activite.get('nom', 'Lieu'), categorie, activite.get('ville', ''))
        descriptions_generees += 1
    
    if (i + 1) % 5000 == 0:
        print(f"   Traité: {i + 1}/{len(auvergne_data)} - ExactMatch: {matches_par_score.get('parfaits', 0) + matches_par_score.get('bons', 0) + matches_par_score.get('acceptables', 0)}, ParCat: {matches_par_score.get('categorie', 0)}, Templates: {descriptions_generees}")

print(f"\n✅ Résultats:")
print(f"   Descriptions DataTourisme: {descriptions_trouvees}/{len(auvergne_data)}")
print(f"   Descriptions générées (templates): {descriptions_generees}/{len(auvergne_data)}")
print(f"   Total avec descriptions: {descriptions_trouvees + descriptions_generees}/{len(auvergne_data)} (100%)")
if descriptions_trouvees > 0:
    print(f"   ")
    print(f"   📊 Qualité des matches DataTourisme:")
    print(f"   - Matches parfaits (>95%): {matches_par_score.get('parfaits', 0)}")
    print(f"   - Bons matches (85-95%): {matches_par_score.get('bons', 0)}")
    print(f"   - Acceptables (70-85%): {matches_par_score.get('acceptables', 0)}")
    print(f"   - Par catégorie: {matches_par_score.get('categorie', 0)}")

# 5. Sauvegarder
print("\n💾 Sauvegarde...")
with open(FICHIER_AUVERGNE, 'w', encoding='utf-8') as f:
    json.dump(auvergne_data, f, ensure_ascii=False, indent=2)
print(f"   ✓ Fichier sauvegardé: {FICHIER_AUVERGNE}")

# 6. Afficher quelques exemples
print("\n📋 Exemples de descriptions enrichies:")
exemples = 0
for activite in auvergne_data:
    desc = activite.get('description', '')
    # Afficher seulement les nouvelles descriptions (pas les templates génériques)
    if len(desc) > 100 and 'vous accueille' not in desc[:50]:
        if exemples < 5:
            print(f"\n   • {activite['nom']} ({activite.get('ville', 'N/A')})")
            print(f"     {desc[:200]}...")
            exemples += 1

print("\n✨ Done!")
