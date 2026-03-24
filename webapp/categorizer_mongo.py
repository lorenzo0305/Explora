#!/usr/bin/env python3
"""Categorisation directe des lieux depuis MongoDB."""

from __future__ import annotations

import argparse
import os
import re
from typing import Any, Dict

from pymongo import MongoClient


CATEGORY_MAPPING = {
    "restauration": {
        "types": [
            "Restaurant",
            "FoodEstablishment",
            "FastFoodRestaurant",
            "BrasserieOrTavern",
            "Cafe",
            "BarOrPub",
            "Market",
            "Winery",
            "Store",
        ],
        "label": "Restauration et gastronomie",
        "slug": "restauration",
    },
    "culture": {
        "types": [
            "CulturalSite",
            "Museum",
            "Castle",
            "ReligiousSite",
            "Cinema",
            "MovieTheater",
            "Chapel",
            "Church",
            "Cathedral",
            "Monastery",
            "RemarkableBuilding",
            "ArcheologicalSite",
            "TechnicalHeritage",
            "RemembranceSite",
            "DefenceSite",
            "Library",
            "ArtGallery",
            "ConventionalExhibition",
        ],
        "label": "Culture et patrimoine",
        "slug": "culture",
    },
    "spectacles": {
        "types": [
            "Concert",
            "Festival",
            "ShowEvent",
            "CulturalEvent",
            "TheatreEvent",
            "EntertainmentAndEvent",
            "Parade",
            "LocalAnimation",
            "OpenDay",
            "Circus",
            "Opera",
            "DanceEvent",
        ],
        "label": "Spectacles et evenements",
        "slug": "spectacles",
    },
    "sport": {
        "types": [
            "SportsAndLeisurePlace",
            "SportsEvent",
            "SwimmingPool",
            "Rambling",
            "CyclingTour",
            "Hiking",
            "EquestrianCenter",
            "Practice",
            "ActivityProvider",
            "ClimbingWall",
            "Skiing",
            "IceRink",
            "GolfCourse",
            "TennisComplex",
            "FitnessCenter",
            "WaterSport",
            "AirSport",
        ],
        "label": "Sports et activites outdoor",
        "slug": "sport",
    },
    "nature": {
        "types": [
            "NaturalHeritage",
            "Park",
            "ParkAndGarden",
            "Lake",
            "PointOfView",
            "PicnicArea",
            "Beach",
            "Fountain",
            "Waterfall",
            "Cave",
            "Forest",
            "Mountain",
            "BotanicalGardenOrZoo",
            "Zoo",
            "Arboretum",
        ],
        "label": "Nature et detente",
        "slug": "nature",
    },
    "hebergement": {
        "types": [
            "Accommodation",
            "Hotel",
            "HotelTrade",
            "Camping",
            "CampingAndCaravanning",
            "RentalAccommodation",
            "SelfCateringAccommodation",
            "BedAndBreakfast",
            "Guesthouse",
            "GroupLodging",
            "CollectiveAccommodation",
        ],
        "label": "Hebergement",
        "slug": "hebergement",
    },
    "shopping": {
        "types": ["Store", "BricABrac", "CraftsmanShop", "LocalProducer", "SaleEvent"],
        "label": "Shopping et artisanat",
        "slug": "shopping",
    },
    "services": {
        "types": [
            "ServiceProvider",
            "TouristInformationCenter",
            "ConvenientService",
            "TaxiCompany",
            "TaxiStation",
            "Cybercafe",
            "Rental",
        ],
        "label": "Services et activites encadrees",
        "slug": "services",
    },
}


KEYWORD_FALLBACK = {
    "restauration": [
        r"\brestaurant\b",
        r"\bcafe\b",
        r"\bbar\b",
        r"\bbrasserie\b",
        r"\bcreperie\b",
        r"\bsnack\b",
        r"\bgastronomie\b",
    ],
    "culture": [
        r"\bmusee\b",
        r"\bmuseum\b",
        r"\bchateau\b",
        r"\bchapelle\b",
        r"\beglise\b",
        r"\bpatrimoine\b",
        r"\bmonument\b",
    ],
    "spectacles": [
        r"\bconcert\b",
        r"\bfestival\b",
        r"\bspectacle\b",
        r"\bevenement\b",
        r"\btheatre\b",
    ],
    "sport": [
        r"\bsport\b",
        r"\bpiscine\b",
        r"\btennis\b",
        r"\bgolf\b",
        r"\bvtt\b",
        r"\bvelo\b",
        r"\brandonnee\b",
    ],
    "nature": [
        r"\bparc\b",
        r"\bjardin\b",
        r"\blac\b",
        r"\bforet\b",
        r"\bmontagne\b",
        r"\bplage\b",
    ],
    "hebergement": [
        r"\bhotel\b",
        r"\bhebergement\b",
        r"\bcamping\b",
        r"\bgite\b",
        r"\bauberge\b",
    ],
    "shopping": [
        r"\bboutique\b",
        r"\bmagasin\b",
        r"\bcommerce\b",
        r"\bartisan\b",
    ],
    "services": [
        r"\boffice de tourisme\b",
        r"\btaxi\b",
        r"\bservice\b",
        r"\bagence\b",
    ],
}


def get_category_for_types(types: list[str]) -> Dict[str, str]:
    if not types:
        return {"label": "Autres points d'interet", "slug": "autres"}

    category_scores: Dict[str, int] = {}
    for category_key, category_info in CATEGORY_MAPPING.items():
        score = sum(
            2
            if obj_type in category_info["types"]
            else 1
            if any(cat_type in obj_type for cat_type in category_info["types"])
            else 0
            for obj_type in types
        )
        if score > 0:
            category_scores[category_key] = score

    if category_scores:
        best_category_key = max(category_scores, key=category_scores.get)
        return {
            "label": CATEGORY_MAPPING[best_category_key]["label"],
            "slug": CATEGORY_MAPPING[best_category_key]["slug"],
        }

    return {"label": "Autres points d'interet", "slug": "autres"}


def guess_category_from_label(label: str) -> Dict[str, str]:
    label_lower = (
        label.lower()
        .replace("é", "e")
        .replace("è", "e")
        .replace("ê", "e")
        .replace("à", "a")
        .replace("â", "a")
        .replace("ù", "u")
        .replace("ô", "o")
        .replace("ç", "c")
        .replace("î", "i")
    )

    best_category = "autres"
    best_score = 0
    priority = {
        "hebergement": 0,
        "restauration": 1,
        "culture": 2,
        "spectacles": 3,
        "sport": 4,
        "nature": 5,
        "shopping": 6,
        "services": 7,
    }

    for category_key, patterns in KEYWORD_FALLBACK.items():
        score = 0
        for pattern in patterns:
            score += len(re.findall(pattern, label_lower, re.IGNORECASE))

        if score > best_score:
            best_score = score
            best_category = category_key
        elif score == best_score and score > 0:
            if priority.get(category_key, 99) < priority.get(best_category, 99):
                best_category = category_key

    if best_category in CATEGORY_MAPPING:
        return {
            "label": CATEGORY_MAPPING[best_category]["label"],
            "slug": CATEGORY_MAPPING[best_category]["slug"],
        }

    return {"label": "Autres points d'interet", "slug": "autres"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Categoriser les documents MongoDB sans utiliser de fichiers JSON."
    )
    parser.add_argument(
        "--mongo-uri",
        default=os.getenv("MONGO_URI", "mongodb://localhost:27017"),
        help="URI MongoDB (defaut: MONGO_URI ou mongodb://localhost:27017)",
    )
    parser.add_argument(
        "--db",
        default=os.getenv("MONGO_DB", "LORA_voyage"),
        help="Nom de la base MongoDB (defaut: MONGO_DB ou LORA_voyage)",
    )
    parser.add_argument(
        "--collection",
        default=os.getenv("MONGO_COLLECTION", "objects"),
        help="Nom de la collection (defaut: MONGO_COLLECTION ou objects)",
    )
    parser.add_argument(
        "--region",
        default=None,
        help="Filtre de region (match sur region ou _regions).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Nombre max de documents a traiter (0 = sans limite).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Calcule les categories sans ecriture Mongo.",
    )
    return parser.parse_args()


def _extract_label(doc: Dict[str, Any]) -> str:
    label_obj = doc.get("rdfs:label", {})
    if isinstance(label_obj, dict):
        fr_value = label_obj.get("fr")
        if isinstance(fr_value, list) and fr_value:
            return str(fr_value[0])
        if isinstance(fr_value, str):
            return fr_value
    for candidate in ("label", "nom", "name"):
        value = doc.get(candidate)
        if isinstance(value, str) and value.strip():
            return value
    return ""


def _extract_types(doc: Dict[str, Any]) -> list[str]:
    raw_types = doc.get("@type", [])
    if isinstance(raw_types, list):
        return [str(t) for t in raw_types]
    if isinstance(raw_types, str) and raw_types:
        return [raw_types]
    return []


def _category_for_doc(doc: Dict[str, Any]) -> tuple[Dict[str, str], str]:
    types = _extract_types(doc)
    if types:
        category = get_category_for_types(types)
        if category and category.get("slug") != "autres":
            return category, "types"

    label = _extract_label(doc)
    category = guess_category_from_label(label)
    return category, "fallback_label"


def _build_query(region: str | None) -> Dict[str, Any]:
    if not region:
        return {}
    return {
        "$or": [
            {"region": region},
            {"_regions": region},
        ]
    }


def main() -> int:
    args = parse_args()

    client = MongoClient(args.mongo_uri)
    col = client[args.db][args.collection]

    query = _build_query(args.region)
    total_docs = col.count_documents(query)
    if args.limit > 0:
        total_target = min(total_docs, args.limit)
    else:
        total_target = total_docs

    print("Connexion MongoDB OK")
    print(f"Base: {args.db} | Collection: {args.collection}")
    if args.region:
        print(f"Filtre region: {args.region}")
    print(f"Documents cibles: {total_target}")

    cursor = col.find(query)
    if args.limit > 0:
        cursor = cursor.limit(args.limit)

    processed = 0
    updated = 0
    unchanged = 0
    by_source = {"types": 0, "fallback_label": 0}
    by_category: Dict[str, int] = {}

    for doc in cursor:
        processed += 1
        category, source = _category_for_doc(doc)
        by_source[source] += 1
        slug = category.get("slug", "autres")
        by_category[slug] = by_category.get(slug, 0) + 1

        if args.dry_run:
            continue

        current = doc.get("category")
        current_source = doc.get("categorySource")
        if current == category and current_source == source:
            unchanged += 1
            continue

        result = col.update_one(
            {"_id": doc["_id"]},
            {
                "$set": {
                    "category": category,
                    "categorySource": source,
                }
            },
        )
        if result.modified_count > 0:
            updated += 1
        else:
            unchanged += 1

    print("--- Resume ---")
    print(f"Traites: {processed}")
    print(f"Via types: {by_source['types']}")
    print(f"Via fallback label: {by_source['fallback_label']}")
    if not args.dry_run:
        print(f"Documents modifies: {updated}")
        print(f"Documents inchanges: {unchanged}")

    print("Repartition categories:")
    for slug, count in sorted(by_category.items(), key=lambda x: x[1], reverse=True):
        print(f"- {slug}: {count}")

    if args.dry_run:
        print("Mode dry-run: aucune ecriture effectuee.")
    else:
        print("Mise a jour Mongo terminee.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
