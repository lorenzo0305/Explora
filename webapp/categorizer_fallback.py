#!/usr/bin/env python3
"""
Script avec fallback pour catégoriser les lieux sans fichier source
en analysant leur nom/label
"""
import json
import os
from pathlib import Path
from typing import List, Dict
import re

# Import du categorizer principal
from categorizer import CATEGORY_MAPPING, get_category_for_types, load_detailed_data

# Mots-clés pour fallback basé sur le nom
KEYWORD_FALLBACK = {
    "restauration": [
        r'\brestaurant\b', r'\bcafe\b', r'\bcafé\b', r'\bbar\b', r'\bbrasserie\b',
        r'\bcreperie\b', r'\bcrêperie\b', r'\bpizzeria\b', r'\bboucherie\b',
        r'\bboulangerie\b', r'\bpatisserie\b', r'\bpâtisserie\b', r'\bmarche\b',
        r'\bmarché\b', r'\bgastronomie\b', r'\bcuisine\b', r'\btable\b',
        r'\bauberge\b', r'\btraiteur\b', r'\bsnack\b', r'\bfood\b'
    ],
    "culture": [
        r'\bmusee\b', r'\bmusée\b', r'\bchateau\b', r'\bchâteau\b', r'\beglise\b',
        r'\béglise\b', r'\bchapelle\b', r'\babbaye\b', r'\bcathedrale\b',
        r'\bcathédrale\b', r'\bmonastere\b', r'\bmonastère\b', r'\bpatrimoine\b',
        r'\bhistoire\b', r'\bhistorique\b', r'\bartistique\b', r'\bculture\b',
        r'\bgalerie\b', r'\bexposition\b', r'\bartiste\b', r'\bmonument\b',
        r'\bmemorial\b', r'\bmémorial\b', r'\bbibliotheque\b', r'\bbibliothèque\b'
    ],
    "spectacles": [
        r'\bconcert\b', r'\bfestival\b', r'\bspectacle\b', r'\btheatre\b',
        r'\bthéâtre\b', r'\bscene\b', r'\bscène\b', r'\bshow\b', r'\banimation\b',
        r'\bevenement\b', r'\bévénement\b', r'\bfete\b', r'\bfête\b', r'\bcabaret\b',
        r'\bmusic\b', r'\bmusique\b', r'\bopera\b', r'\bopéra\b', r'\bdanse\b',
        r'\bcirque\b', r'\bparade\b'
    ],
    "sport": [
        r'\bsport\b', r'\bpiscine\b', r'\btennisb', r'\bgolf\b', r'\bski\b',
        r'\bvtt\b', r'\bvelo\b', r'\bvélo\b', r'\brandonnee\b', r'\brandonnée\b',
        r'\bescalade\b', r'\bclimbing\b', r'\bequitation\b', r'\béquitation\b',
        r'\bfitness\b', r'\bstade\b', r'\bgymnase\b', r'\bpiste\b', r'\bsentier\b',
        r'\bcircuit\b', r'\bcourse\b', r'\bescape\b', r'\bloisir\b'
    ],
    "nature": [
        r'\bparc\b', r'\bjardin\b', r'\blac\b', r'\briviere\b', r'\brivière\b',
        r'\bmontagne\b', r'\bforet\b', r'\bforêt\b', r'\bpanoramique\b',
        r'\bpanorama\b', r'\bbelvedere\b', r'\bbelvédère\b', r'\bcascade\b',
        r'\bgrottes?\b', r'\bnature\b', r'\bfaune\b', r'\bflore\b', r'\bbotanique\b',
        r'\bzoo\b', r'\barboretum\b', r'\bplage\b', r'\bpique-nique\b'
    ],
    "hebergement": [
        r'\bhotel\b', r'\bhôtel\b', r'\bchambre\b', r'\bgite\b', r'\bgîte\b',
        r'\bcamping\b', r'\bhebergement\b', r'\bhébergement\b', r'\blogement\b',
        r'\bmeuble\b', r'\bmeublé\b', r'\bauberge\b', r'\bresidence\b',
        r'\brésidence\b', r'\blocations?\b'
    ],
    "shopping": [
        r'\bboutique\b', r'\bmagasin\b', r'\bcommerce\b', r'\bartisan\b',
        r'\bartisanat\b', r'\bmarche\b', r'\bmarché\b', r'\bfoire\b',
        r'\bbrocante\b', r'\bvente\b', r'\bproduit\b', r'\bfermier\b'
    ],
    "services": [
        r'\bbureau\b', r'\boffice\b', r'\btourisme\b', r'\bguide\b',
        r'\baccompagnateur\b', r'\bservice\b', r'\binformation\b',
        r'\btaxi\b', r'\bagence\b', r'\bprestataire\b'
    ]
}


def guess_category_from_label(label: str) -> Dict[str, str]:
    """
    Devine la catégorie en analysant le nom du lieu
    
    Args:
        label: Nom du lieu
        
    Returns:
        Dict avec label et slug de la catégorie
    """
    label_lower = label.lower()
    
    # Compter les matches par catégorie
    scores = {}
    for category_slug, patterns in KEYWORD_FALLBACK.items():
        score = 0
        for pattern in patterns:
            if re.search(pattern, label_lower):
                score += 1
        if score > 0:
            scores[category_slug] = score
    
    # Retourner la meilleure catégorie
    if scores:
        best_slug = max(scores, key=scores.get)
        return {
            "label": CATEGORY_MAPPING[best_slug]["label"],
            "slug": best_slug
        }
    
    # Par défaut
    return {
        "label": "📍 Autres points d'intérêt",
        "slug": "autres"
    }


def categorize_region_with_fallback(region_file: str, output_file: str, objects_base_path: str):
    """
    Catégorise avec fallback sur le nom si fichier source manquant
    """
    print(f"📂 Lecture de {region_file}...")
    
    with open(region_file, 'r', encoding='utf-8') as f:
        region_data = json.load(f)
    
    print(f"✅ {len(region_data)} lieux trouvés")
    
    enriched_data = []
    category_stats = {}
    fallback_count = 0
    
    for idx, item in enumerate(region_data):
        print(f"⏳ {idx+1}/{len(region_data)}: {item['label'][:50]}...", end='\r')
        
        # Essayer avec le fichier détaillé d'abord
        detailed_data = load_detailed_data(item['file'], objects_base_path)
        
        if detailed_data and '@type' in detailed_data:
            # Méthode normale
            category = get_category_for_types(detailed_data['@type'])
            types = detailed_data['@type']
        else:
            # FALLBACK : deviner par le nom
            category = guess_category_from_label(item['label'])
            types = ["GuessedFromLabel"]
            fallback_count += 1
        
        enriched_item = item.copy()
        enriched_item['category'] = category
        enriched_item['types'] = types
        enriched_data.append(enriched_item)
        
        category_stats[category['slug']] = category_stats.get(category['slug'], 0) + 1
    
    print("\n💾 Sauvegarde...")
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(enriched_data, f, ensure_ascii=False, indent=2)
    
    # Statistiques
    print("\n" + "="*60)
    print("📊 STATISTIQUES")
    print("="*60)
    for category_slug, count in sorted(category_stats.items(), key=lambda x: x[1], reverse=True):
        label = next((cat['label'] for cat in CATEGORY_MAPPING.values() if cat['slug'] == category_slug), category_slug)
        percentage = (count / len(region_data)) * 100
        print(f"{label:40} : {count:4} ({percentage:5.1f}%)")
    
    print(f"\n🤖 Catégorisés par fallback (nom) : {fallback_count}")
    print(f"📁 Catégorisés par fichier source : {len(region_data) - fallback_count}")
    print("="*60)
    print(f"✅ Fichier créé : {output_file}\n")


if __name__ == "__main__":
    import sys
    
    BASE_DIR = Path(__file__).parent.parent
    DATA_DIR = BASE_DIR / "data"
    OBJECTS_DIR = DATA_DIR / "full_france_object" / "objects"
    
    region = sys.argv[1] if len(sys.argv) > 1 else "auvergne"
    
    if region.lower() in ["auvergne", "ara"]:
        region_name = "Auvergne-Rhône-Alpes"
    else:
        region_name = "Hauts-de-France"
    
    print(f"\n🚀 Catégorisation avec FALLBACK : {region_name}\n")
    
    region_file = DATA_DIR / f"{region_name}.json"
    output_file = DATA_DIR / f"{region_name}-categorized-fallback.json"
    
    categorize_region_with_fallback(str(region_file), str(output_file), str(OBJECTS_DIR))
    
    print("✨ Terminé !")
