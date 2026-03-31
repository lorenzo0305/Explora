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

def calculer_segments_trajets(matin, aprem):
    """Calcule tous les trajets entre les activités d'une journée."""
    segments = {
        'm1_m2': None,
        'midi':   None,
        'a1_a2': None
    }
    
    # Trajet Matin 1 ⮕ Matin 2
    if len(matin) == 2:
        segments['m1_m2'] = routage_osrm(matin[0]['latitude'], matin[0]['longitude'], 
                                         matin[1]['latitude'], matin[1]['longitude'])
    
    # Trajet Matin (dernier) ⮕ Après-midi (premier)
    if matin and aprem:
        segments['midi'] = routage_osrm(matin[-1]['latitude'], matin[-1]['longitude'], 
                                        aprem[0]['latitude'], aprem[0]['longitude'])
        
    # Trajet Après-midi 1 ⮕ Après-midi 2
    if len(aprem) == 2:
        segments['a1_a2'] = routage_osrm(aprem[0]['latitude'], aprem[0]['longitude'], 
                                         aprem[1]['latitude'], aprem[1]['longitude'])
        
    return segments
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
    # 1. On garde le tri par SCORE (le pkl est déjà trié par score_final normalement)
    # On s'assure que c'est trié par score décroissant dès le départ
    df_unique = df_scored.sort_values(by='score_final', ascending=False).drop_duplicates(subset=['nom'])
    records = df_unique.to_dict('records')
    
    planning = []
    MAX_DIST_KM = 40  # Environ 45min-1h de route selon le trafic

    for jour in range(1, nb_jours + 1):
        matin = []
        mots_cles_jour = set()

        def est_trop_similaire(nom, blacklist):
            mots = {m for m in nom.lower().split() if len(m) > 3}
            return not mots.isdisjoint(blacklist)

        # --- ÉTAPE 1 : CHOIX DU MATIN (Basé sur le Score) ---
        idx = 0
        while len(matin) < 2 and idx < len(records):
            act = records[idx]
            if not est_trop_similaire(act['nom'], mots_cles_jour):
                act['trajet_ville'] = routage_osrm(lat_c, lon_c, act['latitude'], act['longitude'])
                matin.append(act)
                mots_cles_jour.update({m for m in act['nom'].lower().split() if len(m) > 3})
                records.pop(idx) # On la retire des dispos
            else:
                idx += 1

        # --- ÉTAPE 2 : CHOIX DE L'APRÈS-MIDI (Score + Filtre Distance) ---
        aprem = []
        if matin:
            last_lat, last_lon = matin[-1]['latitude'], matin[-1]['longitude']
            
            idx = 0
            while len(aprem) < 2 and idx < len(records):
                act = records[idx]
                
                # Calcul de la distance entre le matin et cette activité potentielle
                dist_interne = haversine(last_lat, last_lon, act['latitude'], act['longitude'])
                
                # CONDITION : Meilleur score possible MAIS à moins de 1h (MAX_DIST_KM)
                if dist_interne <= MAX_DIST_KM and not est_trop_similaire(act['nom'], mots_cles_jour):
                    act['trajet_ville'] = routage_osrm(lat_c, lon_c, act['latitude'], act['longitude'])
                    aprem.append(act)
                    mots_cles_jour.update({m for m in act['nom'].lower().split() if len(m) > 3})
                    records.pop(idx)
                else:
                    idx += 1

        # --- CALCUL LIAISON ---
        trajet_ma = "Non disponible"
        if len(matin) >= 1 and len(aprem) >= 1:
            trajet_ma = routage_osrm(matin[-1]['latitude'], matin[-1]['longitude'], 
                                     aprem[0]['latitude'], aprem[0]['longitude'])

        
        planning.append({
            'jour': jour, 
            'matin': matin, 
            'aprem': aprem, 
            'trajets': calculer_segments_trajets(matin, aprem) # On stocke le dictionnaire de trajets
        })
    return planning
# ─────────────────────────────────────────────
#  7. AFFICHAGE (AVEC MINUTES ET PROPRETÉ)
# ─────────────────────────────────────────────
def afficher_planning(planning, ville):
    SEP = "═" * 65
    SUB = "─" * 65
    
    print(f"\n{SEP}")
    print(f"    VOTRE CARNET DE ROUTE EXPLORA - {ville.upper()}")
    print(f"{SEP}")

    for j in planning:
        if not j['matin'] and not j['aprem']:
            continue
            
        print(f"\n{SUB}")
        print(f"    JOUR {j['jour']}")
        print(f"{SUB}")

        # --- SECTION MATIN ---
        if j['matin']:
            print("\n    MATINÉE")
            # Activité 1
            afficher_activite(j['matin'][0], 1)
            
            # Trajet entre 1 et 2 du matin
            if j['trajets']['m1_m2']:
                print(f"\n        Liaison : {j['trajets']['m1_m2']}")
            
            # Activité 2 (si elle existe)
            if len(j['matin']) > 1:
                afficher_activite(j['matin'][1], 2)

        # --- SECTION MIDI ---
        if j['trajets']['midi']:
            print(f"\n   PAUSE MIDI - Itinéraire : {j['trajets']['midi']}")

        # --- SECTION APRÈS-MIDI ---
        if j['aprem']:
            print("\n    APRÈS-MIDI")
            # Activité 1
            afficher_activite(j['aprem'][0], 1)
            
            # Trajet entre 1 et 2 de l'après-midi
            if j['trajets']['a1_a2']:
                print(f"\n       Liaison (1 ⮕ 2) : {j['trajets']['a1_a2']}")
                
            # Activité 2 (si elle existe)
            if len(j['aprem']) > 1:
                afficher_activite(j['aprem'][1], 2)

    print(f"\n{SEP}")
    print("  Bon voyage avec Explora !")
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