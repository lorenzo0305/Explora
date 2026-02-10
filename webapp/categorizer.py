#!/usr/bin/env python3
"""
Script pour catégoriser automatiquement les lieux touristiques
selon leurs types DataTourisme avec fallback intelligent sur le nom
"""
import json
import os
import re
from pathlib import Path
from typing import List, Dict, Set

# Mapping des types DataTourisme vers nos catégories
CATEGORY_MAPPING = {
    "restauration": {
        "types": [
            "Restaurant", "FoodEstablishment", "FastFoodRestaurant", 
            "BrasserieOrTavern", "Cafe", "BarOrPub", "Market",
            "Winery", "Store"  # pour les boutiques alimentaires
        ],
        "label": "Restauration et gastronomie",
        "slug": "restauration"
    },
    "culture": {
        "types": [
            "CulturalSite", "Museum", "Castle", "ReligiousSite", 
            "Chapel", "Church", "Cathedral", "Monastery",
            "RemarkableBuilding", "ArcheologicalSite", "TechnicalHeritage",
            "RemembranceSite", "DefenceSite", "Library", "ArtGallery",
            "ConventionalExhibition"
        ],
        "label": "Culture et patrimoine",
        "slug": "culture"
    },
    "spectacles": {
        "types": [
            "Concert", "Festival", "ShowEvent", "CulturalEvent",
            "TheatreEvent", "EntertainmentAndEvent", "Parade",
            "LocalAnimation", "OpenDay", "LocalAnimation",
            "Circus", "Opera", "DanceEvent"
        ],
        "label": "Spectacles et événements",
        "slug": "spectacles"
    },
    "sport": {
        "types": [
            "SportsAndLeisurePlace", "SportsEvent", "SwimmingPool",
            "Rambling", "CyclingTour", "Hiking", "EquestrianCenter",
            "Practice", "ActivityProvider", "ClimbingWall",
            "Skiing", "IceRink", "GolfCourse", "TennisComplex",
            "FitnessCenter", "WaterSport", "AirSport"
        ],
        "label": "Sports et activités outdoor",
        "slug": "sport"
    },
    "nature": {
        "types": [
            "NaturalHeritage", "Park", "ParkAndGarden", "Lake",
            "PointOfView", "PicnicArea", "Beach", "Fountain",
            "Waterfall", "Cave", "Forest", "Mountain",
            "BotanicalGardenOrZoo", "Zoo", "Arboretum"
        ],
        "label": "Nature et détente",
        "slug": "nature"
    },
    "hebergement": {
        "types": [
            "Accommodation", "Hotel", "HotelTrade", "Camping",
            "CampingAndCaravanning", "RentalAccommodation",
            "SelfCateringAccommodation", "BedAndBreakfast",
            "Guesthouse", "GroupLodging", "CollectiveAccommodation"
        ],
        "label": "Hébergement",
        "slug": "hebergement"
    },
    "shopping": {
        "types": [
            "Store", "BricABrac", "CraftsmanShop", "LocalProducer",
            "SaleEvent"
        ],
        "label": "Shopping et artisanat",
        "slug": "shopping"
    },
    "services": {
        "types": [
            "ServiceProvider", "TouristInformationCenter",
            "ConvenientService", "TaxiCompany", "TaxiStation",
            "Cybercafe", "Rental"
        ],
        "label": "Services et activités encadrées",
        "slug": "services"
    }
}

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


def get_category_for_types(types: List[str]) -> Dict[str, str]:
    """
    Détermine la catégorie principale en fonction des types DataTourisme
    
    Args:
        types: Liste des types DataTourisme de l'objet
        
    Returns:
        Dict contenant la catégorie (label, slug) ou None si non catégorisé
    """
    if not types:
        return None
    
    # Compter les correspondances par catégorie
    category_scores = {}
    
    for category_key, category_info in CATEGORY_MAPPING.items():
        score = 0
        for obj_type in types:
            # Correspondance exacte
            if obj_type in category_info["types"]:
                score += 2
            # Correspondance partielle (contient le type)
            elif any(cat_type in obj_type for cat_type in category_info["types"]):
                score += 1
        
        if score > 0:
            category_scores[category_key] = score
    
    # Retourner la catégorie avec le meilleur score
    if category_scores:
        best_category_key = max(category_scores, key=category_scores.get)
        return {
            "label": CATEGORY_MAPPING[best_category_key]["label"],
            "slug": CATEGORY_MAPPING[best_category_key]["slug"]
        }
    
    return {
        "label": "Autres points d'intérêt",
        "slug": "autres"
    }


def guess_category_from_label(label: str) -> Dict[str, str]:
    """
    Devine la catégorie en analysant le nom du lieu (fallback)
    
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
        "label": "Autres points d'intérêt",
        "slug": "autres"
    }


def load_detailed_data(file_path: str, base_path: str) -> Dict:
    """
    Charge les données détaillées d'un fichier JSON
    
    Args:
        file_path: Chemin relatif du fichier (ex: "0/00/13-xxx.json")
        base_path: Chemin de base vers full_france_object/objects
        
    Returns:
        Dict contenant les données complètes ou None si erreur
    """
    full_path = os.path.join(base_path, file_path)
    
    try:
        with open(full_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"⚠️  Fichier non trouvé: {full_path}")
        return None
    except json.JSONDecodeError:
        print(f"⚠️  Erreur de décodage JSON: {full_path}")
        return None


def categorize_region_data(region_file: str, output_file: str, objects_base_path: str):
    """
    Catégorise tous les lieux d'une région et génère un fichier enrichi
    avec fallback intelligent sur le nom
    
    Args:
        region_file: Chemin du fichier JSON régional (ex: Hauts-de-France.json)
        output_file: Chemin du fichier de sortie enrichi
        objects_base_path: Chemin vers le dossier objects/
    """
    print(f"📂 Lecture de {region_file}...")
    
    # Charger le fichier régional
    with open(region_file, 'r', encoding='utf-8') as f:
        region_data = json.load(f)
    
    print(f"✅ {len(region_data)} lieux trouvés")
    
    enriched_data = []
    category_stats = {}
    fallback_count = 0
    
    # Traiter chaque lieu
    for idx, item in enumerate(region_data):
        print(f"⏳ Traitement {idx+1}/{len(region_data)}: {item['label'][:50]}...", end='\r')
        
        # Charger les données détaillées
        detailed_data = load_detailed_data(item['file'], objects_base_path)
        
        if detailed_data and '@type' in detailed_data:
            # Méthode normale avec fichier source
            category = get_category_for_types(detailed_data['@type'])
            types = detailed_data['@type']
        else:
            # FALLBACK : deviner par le nom
            category = guess_category_from_label(item['label'])
            types = ["GuessedFromLabel"]
            fallback_count += 1
        
        # Enrichir l'item avec la catégorie
        enriched_item = item.copy()
        enriched_item['category'] = category
        enriched_item['types'] = types
        
        enriched_data.append(enriched_item)
        
        # Stats
        category_slug = category['slug'] if category else 'autres'
        category_stats[category_slug] = category_stats.get(category_slug, 0) + 1
    
    print("\n")
    
    # Sauvegarder le fichier enrichi
    print(f"💾 Sauvegarde dans {output_file}...")
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(enriched_data, f, ensure_ascii=False, indent=2)
    
    # Afficher les statistiques
    print("\n" + "="*60)
    print("📊 STATISTIQUES DE CATÉGORISATION")
    print("="*60)
    
    for category_slug, count in sorted(category_stats.items(), key=lambda x: x[1], reverse=True):
        # Trouver le label
        label = None
        for cat_info in CATEGORY_MAPPING.values():
            if cat_info['slug'] == category_slug:
                label = cat_info['label']
                break
        if not label:
            label = category_slug
        
        percentage = (count / len(region_data)) * 100
        print(f"{label:40} : {count:4} ({percentage:5.1f}%)")
    
    print(f"\n🤖 Catégorisés par fallback (nom) : {fallback_count}")
    print(f"📁 Catégorisés par fichier source : {len(region_data) - fallback_count}")
    
    print("="*60)
    print(f"✅ Fichier enrichi créé : {output_file}")


if __name__ == "__main__":
    # Configuration des chemins
    BASE_DIR = Path(__file__).parent.parent
    DATA_DIR = BASE_DIR / "data"
    OBJECTS_DIR = DATA_DIR / "full_france_object" / "objects"
    
    # Traiter Auvergne-Rhône-Alpes
    print("🚀 Début de la catégorisation pour Auvergne-Rhône-Alpes\n")
    
    region_file = DATA_DIR / "Auvergne-Rhône-Alpes.json"
    output_file = DATA_DIR / "Auvergne-Rhône-Alpes-categorized.json"
    
    categorize_region_data(
        str(region_file),
        str(output_file),
        str(OBJECTS_DIR)
    )
    
    print("\n✨ Terminé !")
