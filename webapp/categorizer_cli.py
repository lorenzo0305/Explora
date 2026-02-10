#!/usr/bin/env python3
"""
Script pour catégoriser automatiquement les lieux touristiques
Usage: python3 categorizer_cli.py [région]
       python3 categorizer_cli.py hauts-de-france
       python3 categorizer_cli.py auvergne
       python3 categorizer_cli.py all (traite toutes les régions)
"""
import json
import os
import sys
import re
from pathlib import Path
from typing import List, Dict

# Mapping des types DataTourisme vers nos catégories
CATEGORY_MAPPING = {
    "restauration": {
        "types": [
            "Restaurant", "FoodEstablishment", "FastFoodRestaurant", 
            "BrasserieOrTavern", "Cafe", "BarOrPub", "Market",
            "Winery", "Store"
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
            "LocalAnimation", "OpenDay", "Circus", "Opera", "DanceEvent"
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

# Dictionnaire de keywords pour la catégorisation par fallback
KEYWORD_FALLBACK = {
    "restauration": [
        r"\b(restaurant|cafe|brasserie|bistrot|pizzeria|creperie|snack)\b",
        r"\b(auberge|gite|table|repas|gastronomie)\b",
        r"\b(boulangerie|patisserie|salon de the|tea room)\b",
        r"\b(bar|pub|taverne)\b"
    ],
    "culture": [
        r"\b(musee|museum|galerie|expo|exposition|theatre|cinema)\b",
        r"\b(chapelle|chateau|chateau fort|fortification|abbaye|basilique|eglise)\b",
        r"\b(monument|patrimoine|historique|archeologique|pigeonnier)\b",
        r"\b(bibliotheque|archives|mediatheque)\b",
        r"\b(beaux-arts|pinacotbeque|collection|fresco|tableau|sculpture)\b"
    ],
    "sport": [
        r"\b(sport|football|basketball|tennis|golf|piscine|natation)\b",
        r"\b(ski|snowboard|luge|bobsleigh|patinage)\b",
        r"\b(cyclisme|velo|mountain bike|vtt)\b",
        r"\b(fitness|gym|musculation|aerobic|yoga)\b",
        r"\b(sport nautique|voile|kayak|rafting|peche|peche a la mouche)\b",
        r"\b(equitation|centre equestre|cheval)\b",
        r"\b(randonnee|trail|running|jogging)\b",
        r"\b(bowling|patins a glace|roller|paintball|tir a l'arc)\b",
        r"\b(parc ludique|parc attractions|parc aventure|zip line|accrobranche)\b"
    ],
    "nature": [
        r"\b(sentier|chemin|voie verte|circuit|parcours|promenade)\b",
        r"\b(montagne|montagneux|sommet|pic|col|alpage)\b",
        r"\b(lac|etang|plan d'eau|reservoir|canal)\b",
        r"\b(foret|bois|bosquet|arbre|reserve naturelle)\b",
        r"\b(gorge|ravin|canyon|falaise|caverne|grotte|souterrain)\b",
        r"\b(plage|cote|littoral|estuaire|falaise)\b",
        r"\b(vignoble|vigne|viticole|route des vins|exploitation vinicole)\b",
        r"\b(parc|jardin|jardin botanique|arboretum|espace vert)\b"
    ],
    "hebergement": [
        r"\b(hotel|hotellerie|logement|chambre|hebergement)\b",
        r"\b(auberge|gite|gite rural|chambre d'hote|chambre d'hotes)\b",
        r"\b(hostel|auberge jeunesse|auberges jeunesse)\b",
        r"\b(villa|bungalow|chalet|maison de vacances|location)\b",
        r"\b(camping|campsite|caravanning|mobilhome)\b",
        r"\b(havre de paix|resort|village vacances|center vacances|residence)\b"
    ],
    "spectacles": [
        r"\b(festival|fete|fête|celebration|manifestation|evenement|événement|spectacle)\b",
        r"\b(concert|musique|op'|soiree|bal|danse|ballet|danseur|marche|parade)\b",
        r"\b(marche|marche a pied|foire|salon|expo|exposition biennale|biennale)\b",
        r"\b(carnaval|noel|halloween|paques|reveillon|jour de l'an)\b"
    ],
    "shopping": [
        r"\b(boutique|shop|commercial|center commercial|commerce|magasin|store)\b",
        r"\b(boulangerie|boucherie|fromagerie|epicerie|supermarche|hypermarche)\b",
        r"\b(marche|marche provencal|marche couvert|marche public|halles)\b",
        r"\b(artisanat|creation|artiste|art|craft)\b",
        r"\b(vin|cave|vigneron|viticulteur|wine shop|wine store)\b"
    ],
    "services": [
        r"\b(office de tourisme|office touristique|syndicat initiative|si|information|acceuil)\b",
        r"\b(gare|gare routiere|gare sncf|station|aeroport|port|marina)\b",
        r"\b(parking|aire de parking|aire|stationnement)\b",
        r"\b(location|location voiture|location auto|location velo|location bateau|rental)\b",
        r"\b(pompe a essence|station essence|station service)\b",
        r"\b(taxi|transport|bus|car|minibus)\b"
    ]
}

def guess_category_from_label(label: str) -> Dict[str, str]:
    """Essaie de déduire la catégorie à partir du label (fallback)"""
    label_lower = label.lower().replace('é', 'e').replace('è', 'e').replace('ê', 'e')\
                        .replace('à', 'a').replace('â', 'a').replace('ù', 'u')\
                        .replace('ô', 'o').replace('ç', 'c').replace('î', 'i')
    
    best_category = "autres"
    best_score = 0
    
    for category_key, patterns in KEYWORD_FALLBACK.items():
        score = 0
        for pattern in patterns:
            matches = re.findall(pattern, label_lower, re.IGNORECASE)
            score += len(matches)
        
        if score > best_score:
            best_score = score
            best_category = category_key
    
    return {
        "label": CATEGORY_MAPPING.get(best_category, {}).get("label", "Autres points d'intérêt"),
        "slug": CATEGORY_MAPPING.get(best_category, {}).get("slug", "autres")
    }

def get_category_for_types(types: List[str]) -> Dict[str, str]:
    """Détermine la catégorie principale"""
    if not types:
        return {"label": "Autres points d'intérêt", "slug": "autres"}
    
    category_scores = {}
    for category_key, category_info in CATEGORY_MAPPING.items():
        score = sum(2 if obj_type in category_info["types"] else
                   1 if any(cat_type in obj_type for cat_type in category_info["types"]) else 0
                   for obj_type in types)
        if score > 0:
            category_scores[category_key] = score
    
    if category_scores:
        best_category_key = max(category_scores, key=category_scores.get)
        return {
            "label": CATEGORY_MAPPING[best_category_key]["label"],
            "slug": CATEGORY_MAPPING[best_category_key]["slug"]
        }
    
    return {"label": "Autres points d'intérêt", "slug": "autres"}


def load_detailed_data(file_path: str, base_path: str) -> Dict:
    """Charge les données détaillées d'un fichier JSON"""
    full_path = os.path.join(base_path, file_path)
    try:
        with open(full_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except:
        return None

def categorize_region_data(region_file: str, output_file: str, objects_base_path: str):
    """Catégorise tous les lieux d'une région"""
    print(f"📂 Lecture de {region_file}...")
    
    with open(region_file, 'r', encoding='utf-8') as f:
        region_data = json.load(f)
    
    print(f"✅ {len(region_data)} lieux trouvés")
    
    enriched_data = []
    category_stats = {}
    errors = 0
    fallback_count = 0
    
    for idx, item in enumerate(region_data):
        print(f"⏳ {idx+1}/{len(region_data)}: {item['label'][:50]}...", end='\r')
        
        detailed_data = load_detailed_data(item['file'], objects_base_path)
        enriched_item = item.copy()
        
        # Essayer d'obtenir la catégorie à partir du fichier détaillé
        if detailed_data and '@type' in detailed_data:
            category = get_category_for_types(detailed_data['@type'])
            enriched_item['types'] = detailed_data['@type']
        else:
            # Fallback: déduire de la catégorie à partir du label
            category = guess_category_from_label(item['label'])
            enriched_item['types'] = ["GuessedFromLabel"]
            fallback_count += 1
        
        enriched_item['category'] = category
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
    
    print(f"\n📌 Catégorisé par fichier détaillé : {len(region_data) - fallback_count}")
    print(f"📌 Catégorisé par fallback (label)  : {fallback_count}")
    print("="*60)
    print(f"✅ Fichier créé : {output_file}\n")


# Mapping des régions
REGIONS = {
    "hauts-de-france": "Hauts-de-France",
    "hdf": "Hauts-de-France",
    "auvergne": "Auvergne-Rhône-Alpes",
    "auvergne-rhone-alpes": "Auvergne-Rhône-Alpes",
    "ara": "Auvergne-Rhône-Alpes"
}

if __name__ == "__main__":
    BASE_DIR = Path(__file__).parent.parent
    DATA_DIR = BASE_DIR / "data"
    OBJECTS_DIR = DATA_DIR / "full_france_object" / "objects"
    
    # Déterminer quelle(s) région(s) traiter
    regions_to_process = []
    
    if len(sys.argv) > 1:
        arg = sys.argv[1].lower()
        if arg == "all":
            regions_to_process = ["Hauts-de-France", "Auvergne-Rhône-Alpes"]
        elif arg in REGIONS:
            regions_to_process = [REGIONS[arg]]
        else:
            print(f"❌ Région inconnue: {arg}")
            print("\nUsage:")
            print("  python3 categorizer_cli.py hauts-de-france")
            print("  python3 categorizer_cli.py auvergne")
            print("  python3 categorizer_cli.py all")
            sys.exit(1)
    else:
        print("Usage:")
        print("  python3 categorizer_cli.py hauts-de-france")
        print("  python3 categorizer_cli.py auvergne")
        print("  python3 categorizer_cli.py all")
        sys.exit(1)
    
    # Traiter les régions
    for region_name in regions_to_process:
        print(f"\n🚀 Catégorisation de {region_name}\n")
        region_file = DATA_DIR / f"{region_name}.json"
        output_file = DATA_DIR / f"{region_name}-categorized.json"
        
        categorize_region_data(str(region_file), str(output_file), str(OBJECTS_DIR))
    
    print("✨ Terminé !")
