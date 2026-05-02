"""
Configuration centralisée de la base MongoDB.
Charge les variables d'environnement depuis .env (à la racine du projet)
et expose une fonction pour construire l'URI Atlas.

Toutes les credentials sont chargées depuis .env — voir .env.example.
"""
from __future__ import annotations

import os
import urllib.parse
from pathlib import Path

from dotenv import load_dotenv

# Charge le .env à la racine du projet (parent du dossier webapp/)
_ENV_PATH = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=_ENV_PATH if _ENV_PATH.exists() else None)


def get_mongo_uri() -> str:
    """Construit l'URI MongoDB à partir des variables d'environnement."""
    user = os.getenv("MONGO_USER")
    password = os.getenv("MONGO_PASSWORD")
    host = os.getenv("MONGO_HOST")
    app_name = os.getenv("MONGO_APP_NAME", "datas")

    if not user or not password or not host:
        raise RuntimeError(
            "Variables MongoDB manquantes : assurez-vous d'avoir un fichier .env "
            "à la racine du projet avec MONGO_USER, MONGO_PASSWORD et MONGO_HOST définis. "
            "Voir .env.example pour le modèle."
        )

    safe_password = urllib.parse.quote_plus(password)
    return f"mongodb+srv://{user}:{safe_password}@{host}/?appName={app_name}"


def get_db_name() -> str:
    """Nom de la base par défaut (explora)."""
    return os.getenv("MONGO_DB", "explora")


def get_default_collection() -> str:
    """Collection par défaut (Auvergne) pour la recherche globale."""
    return os.getenv("MONGO_COLLECTION", "Auvergne")
