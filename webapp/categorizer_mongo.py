#!/usr/bin/env python3
"""Categorisation directe des lieux depuis MongoDB."""

from __future__ import annotations

import argparse
import os
from typing import Any, Dict

from pymongo import MongoClient

from categorizer_cli import get_category_for_types, guess_category_from_label


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
