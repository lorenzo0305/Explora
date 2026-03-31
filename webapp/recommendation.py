import os
import pickle
import urllib.parse
import numpy as np
import pandas as pd
import requests
import certifi
from pymongo import MongoClient
from geopy.geocoders import Nominatim
from sklearn.neighbors import NearestNeighbors


# ─────────────────────────────────────────────
#  1. CONNEXION MONGODB
# ─────────────────────────────────────────────
def connecter_mongodb():
    user     = "equipe_explora"
    password = "2BqXsiNi8nCCE@W"
    safe_pwd = urllib.parse.quote_plus(password)
    uri      = f"mongodb+srv://{user}:{safe_pwd}@datas.xc1dpyu.mongodb.net/?appName=datas"

    print("Connexion à MongoDB Atlas...")
    client = MongoClient(uri, tlsCAFile=certifi.where())
    client.admin.command('ping')
    print(" Connexion réussie !\n")
    return client


# ─────────────────────────────────────────────
#  2. CHARGEMENT & FUSION DES DONNÉES
#     .pkl     → scores SBERT déjà calculés + DataFrame complet
#     MongoDB  → enrichissement si champs manquants dans le pkl
# ─────────────────────────────────────────────
def charger_donnees(client, dossier_pkl="data/models", db_name="explora"):
    """
    Les .pkl contiennent déjà le DataFrame avec les scores SBERT calculés.
    MongoDB est utilisé pour enrichir les champs manquants (ex: adresse).
    """
    # Chemin local ou Kaggle
    if not os.path.exists(dossier_pkl):
        dossier_pkl = "/kaggle/input/datasets/sarahesiee/fichier-pkl"
    if not os.path.exists(dossier_pkl):
        raise FileNotFoundError(f"Dossier .pkl introuvable : {dossier_pkl}")

    fichiers_pkl = [f for f in os.listdir(dossier_pkl) if f.endswith('.pkl')]
    if not fichiers_pkl:
        raise FileNotFoundError("Aucun fichier .pkl trouvé dans " + dossier_pkl)

    db       = client[db_name]
    cols_mongo = db.list_collection_names()
    all_df   = []

    print(f"Chargement de {len(fichiers_pkl)} fichier(s) .pkl…")

    for f in fichiers_pkl:
        chemin = os.path.join(dossier_pkl, f)
        with open(chemin, 'rb') as fh:
            data = pickle.load(fh)

        # Le pkl contient un dict {'model': knn, 'df': DataFrame}
        df_pkl = data['df'] if isinstance(data, dict) and 'df' in data else data

        # Nom de région déduit du nom de fichier
        # ex: "auvergne.pkl" → "auvergne"
        # ex: "modele_recommandation.pkl" → on cherche la collection existante
        region = f.replace('.pkl', '')

        # Enrichissement MongoDB si la collection existe
        if region in cols_mongo:
            docs = list(db[region].find({}, {'_id': 0}))
            if docs:
                df_mongo = pd.DataFrame(docs)
                # On fusionne sur 'nom' pour récupérer les champs absents du pkl
                cols_a_ajouter = [c for c in df_mongo.columns
                                  if c not in df_pkl.columns and c != '_id']
                if cols_a_ajouter and 'nom' in df_pkl.columns and 'nom' in df_mongo.columns:
                    df_pkl = df_pkl.merge(
                        df_mongo[['nom'] + cols_a_ajouter],
                        on='nom', how='left'
                    )
            print(f"  • {f:<35} → {len(df_pkl):>4} activités  (enrichi via MongoDB '{region}')")
        else:
            print(f"  • {f:<35} → {len(df_pkl):>4} activités  (pas de collection MongoDB '{region}')")

        df_pkl['_region'] = region
        all_df.append(df_pkl)

    df_global = pd.concat(all_df, ignore_index=True)
    df_global[['latitude', 'longitude']] = (
        df_global[['latitude', 'longitude']].apply(pd.to_numeric, errors='coerce')
    )
    # On supprime les lignes sans coordonnées ou sans scores
    df_global = df_global.dropna(subset=['latitude', 'longitude'])
    df_global = df_global[df_global['scores'].notna()]
    print(f"\n {len(df_global)} activités prêtes.\n")
    return df_global


# ─────────────────────────────────────────────
#  3. GÉOLOCALISATION & DISTANCES
# ─────────────────────────────────────────────
def geocoder_ville(ville: str):
    geolocator = Nominatim(user_agent="explora_application")
    loc = geolocator.geocode(f"{ville}, France")
    if not loc:
        raise ValueError(f"Ville introuvable : {ville}")
    return loc.latitude, loc.longitude


def haversine(lat1, lon1, lats2, lons2):
    """Distance à vol d'oiseau vectorisée (km)."""
    R    = 6371
    dlat = np.radians(np.asarray(lats2, dtype=float) - lat1)
    dlon = np.radians(np.asarray(lons2, dtype=float) - lon1)
    a    = (np.sin(dlat / 2) ** 2
            + np.cos(np.radians(lat1)) * np.cos(np.radians(lats2)) * np.sin(dlon / 2) ** 2)
    return R * 2 * np.arcsin(np.sqrt(a))


def routage_osrm(lat1, lon1, lat2, lon2):
    """Distance routière via OSRM, fallback haversine."""
    url = (f"http://router.project-osrm.org/route/v1/driving/"
           f"{lon1},{lat1};{lon2},{lat2}?overview=false")
    try:
        r    = requests.get(url, timeout=4)
        data = r.json()
        if data.get('code') == 'Ok':
            km = data['routes'][0]['distance'] / 1000
            mn = data['routes'][0]['duration'] / 60
            return f"{km:.1f} km (~{int(mn)} min en voiture)"
    except Exception:
        pass
    dist = haversine(lat1, lon1, np.array([lat2]), np.array([lon2]))[0]
    return f"≈ {dist:.1f} km (à vol d'oiseau)"


# ─────────────────────────────────────────────
#  4. PRÉFÉRENCES UTILISATEUR
# ─────────────────────────────────────────────
THEMES        = ["nature", "gastronomie", "sport", "culture", "detente", "boutique"]
THEMES_LABELS = ["Nature", "Gastronomie", "Sport", "Culture", "Détente", "Boutique"]


def saisir_preferences():
    print("Vos préférences — note de 0 à 10 :")
    notes = []
    for label in THEMES_LABELS:
        val = input(f"  {label:<14} : ").strip()
        notes.append(float(val) if val else 5.0)
    return notes


# ─────────────────────────────────────────────
#  5. SCORING KNN (sur les scores SBERT du pkl)
# ─────────────────────────────────────────────
def scorer_activites(df: pd.DataFrame, lat_c, lon_c, rayon_max, prefs):
    """
    1. Filtre les activités dans le rayon.
    2. KNN cosinus sur les scores SBERT (issus des .pkl).
    3. Score final = 60% similarité thématique + 40% proximité.
    """
    df = df.copy()
    df['dist_oiseau'] = haversine(
        lat_c, lon_c, df['latitude'].values, df['longitude'].values
    )
    df = df[df['dist_oiseau'] <= rayon_max].reset_index(drop=True)

    if df.empty:
        return df

    # Matrice des scores thématiques SBERT (déjà dans le pkl)
    X = np.array([
        [row['scores'].get(t, 0) for t in THEMES]
        for _, row in df.iterrows()
    ])
    user_vec = np.array(prefs).reshape(1, -1)

    knn = NearestNeighbors(n_neighbors=len(df), metric='cosine')
    knn.fit(X)
    dist_knn, indices = knn.kneighbors(user_vec)

    score_sim   = 1 - dist_knn[0]
    score_prox  = 1 - (df['dist_oiseau'].values[indices[0]] / rayon_max)
    score_final = 0.6 * score_sim + 0.4 * score_prox

    df_sorted = df.iloc[indices[0]].copy()
    df_sorted['score_final'] = score_final
    return df_sorted.reset_index(drop=True)


# ─────────────────────────────────────────────
#  6. GÉNÉRATION DU PLANNING JOURNALIER
# ─────────────────────────────────────────────
def generer_planning(df_scored: pd.DataFrame, nb_jours: int, lat_c, lon_c):
    # 1. Nettoyage initial
    df_unique = df_scored.drop_duplicates(subset=['nom']).copy()
    records = df_unique.to_dict('records')
    
    planning = []

    for jour in range(1, nb_jours + 1):
        matin = []
        mots_cles_jour = set() # Pour stocker les types d'activités déjà vus aujourd'hui

        def est_trop_similaire(nom, blacklist):
            # Vérifie si un mot important du nom est déjà dans la blacklist
            mots = set(nom.lower().split())
            # On ignore les petits mots
            mots = {m for m in mots if len(m) > 3}
            return not mots.isdisjoint(blacklist)

        # --- MATIN ---
        count = 0
        while len(matin) < 2 and records:
            act = records.pop(0)
            # Si l'activité ressemble trop à ce qu'on a déjà, on la remet à la fin (ou on la skip)
            if est_trop_similaire(act['nom'], mots_cles_jour):
                records.append(act) # On la décale
                count += 1
                if count > 10: break # Sécurité pour ne pas boucler à l'infini
                continue
            
            act['trajet_ville'] = routage_osrm(lat_c, lon_c, act['latitude'], act['longitude'])
            matin.append(act)
            # On ajoute les mots importants du nom à la blacklist du jour
            mots_cles_jour.update({m for m in act['nom'].lower().split() if len(m) > 3})

        # --- OPTIMISATION SPATIALE ---
        last_lat, last_lon = matin[-1]['latitude'], matin[-1]['longitude']
        records.sort(key=lambda x: haversine(last_lat, last_lon, x['latitude'], x['longitude']))

        # --- APRÈS-MIDI ---
        aprem = []
        count = 0
        while len(aprem) < 2 and records:
            act = records.pop(0)
            if est_trop_similaire(act['nom'], mots_cles_jour):
                records.append(act)
                count += 1
                if count > 10: break
                continue
                
            act['trajet_ville'] = routage_osrm(lat_c, lon_c, act['latitude'], act['longitude'])
            aprem.append(act)
            mots_cles_jour.update({m for m in act['nom'].lower().split() if len(m) > 3})

        # Trajet liaison
        trajet_ma = routage_osrm(matin[-1]['latitude'], matin[-1]['longitude'],
                                aprem[0]['latitude'], aprem[0]['longitude'])

        planning.append({
            'jour': jour, 'matin': matin, 'aprem': aprem, 'trajet_ma': trajet_ma
        })
        
        # --- RESET POUR LE LENDEMAIN ---
        # On remet un coup de tri par score final pour ne pas rester bloqué dans la zone
        records.sort(key=lambda x: x.get('score_final', 0), reverse=True)
    
    print(planning)
    return planning
# ─────────────────────────────────────────────
#  7. AFFICHAGE (AVEC MINUTES ET PROPRETÉ)
# ─────────────────────────────────────────────
def afficher_activite(act, rang):
    match_pct = int(act.get('score_final', 0) * 100)
    route_info = act.get('trajet_ville', "Distance non disponible")
    
    print(f"\n     Activité {rang} : {act['nom']}")
    if 'adresse' in act and pd.notna(act['adresse']):
        print(f"        {act['adresse']}")
    print(f"        Match : {match_pct}%")
    print(f"        Accès depuis le centre : {route_info}")

def afficher_planning(planning, ville):
    SEP = "═" * 65
    SUB = "─" * 65
    
    print(f"\n{SEP}")
    print(f"   VOTRE CARNET DE ROUTE EXPLORA - {ville.upper()}")
    print(f"{SEP}")

    for j in planning:
        if not j['matin'] and not j['aprem']:
            continue
            
        print(f"\n{SUB}")
        print(f"   JOUR {j['jour']}")
        print(f"{SUB}")

        if j['matin']:
            print("\n   MATINÉE")
            for idx, act in enumerate(j['matin'], 1):
                afficher_activite(act, idx)

        if j['trajet_ma']:
            print(f"\n   Liaison midi : {j['trajet_ma']}")

        if j['aprem']:
            print("\n   APRÈS-MIDI")
            for idx, act in enumerate(j['aprem'], 1):
                afficher_activite(act, idx)

    print(f"\n{SEP}")
    print(SEP)
# ─────────────────────────────────────────────
#  8. POINT D'ENTRÉE
# ─────────────────────────────────────────────
def lancer_explora():
    # Connexion & chargement
    client    = connecter_mongodb()
    df_global = charger_donnees(client)

    # Localisation
    ville = input("Dans quelle ville es-tu ? ").strip()
    lat_c, lon_c = geocoder_ville(ville)

    rayon_str = input("Rayon de recherche en km [défaut : 30] : ").strip()
    rayon_max = float(rayon_str) if rayon_str else 30.0

    jours_str = input("Nombre de jours du planning [défaut : 3] : ").strip()
    nb_jours  = int(jours_str) if jours_str else 3

    # Préférences
    print()
    prefs = saisir_preferences()

    # Scoring
    print("\nCalcul des recommandations en cours…")
    df_scored = scorer_activites(df_global, lat_c, lon_c, rayon_max, prefs)

    if df_scored.empty:
        print(f"\n Aucune activité trouvée dans un rayon de {rayon_max} km autour de {ville}.")
        client.close()
        return

    nb_needed = nb_jours * 4
    if len(df_scored) < nb_needed:
        print(f"  Seulement {len(df_scored)} activités disponibles "
              f"(idéalement {nb_needed} pour {nb_jours} jours). Planning adapté.")

    # Planning
    print("Calcul des trajets routiers…")
    planning = generer_planning(df_scored, nb_jours, lat_c, lon_c)
    afficher_planning(planning, ville)

    client.close()


if __name__ == "__main__":
    lancer_explora()