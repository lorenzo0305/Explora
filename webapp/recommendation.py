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
    user, password = "equipe_explora", "2BqXsiNi8nCCE@W"
    uri = f"mongodb+srv://{user}:{urllib.parse.quote_plus(password)}@datas.xc1dpyu.mongodb.net/?appName=datas"
    client = MongoClient(uri, tlsCAFile=certifi.where())
    print(" Connexion réussie !\n")
    return client

# ─────────────────────────────────────────────
#  2. CHARGEMENT DES DONNÉES
# ─────────────────────────────────────────────
def charger_donnees(client, dossier_pkl="data/models", db_name="explora"):
    if not os.path.exists(dossier_pkl):
        dossier_pkl = "/kaggle/input/datasets/sarahesiee/fichier-pkl"
    
    fichiers_pkl = [f for f in os.listdir(dossier_pkl) if f.endswith('.pkl')]
    db = client[db_name]
    cols_mongo = db.list_collection_names()
    all_df = []

    for f in fichiers_pkl:
        chemin = os.path.join(dossier_pkl, f)
        with open(chemin, 'rb') as fh:
            data = pickle.load(fh)
        df_pkl = data['df'] if isinstance(data, dict) and 'df' in data else data
        region = f.replace('.pkl', '')

        if region in cols_mongo:
            docs = list(db[region].find({}, {'_id': 0}))
            if docs:
                df_mongo = pd.DataFrame(docs)
                cols_add = [c for c in df_mongo.columns if c not in df_pkl.columns and c != '_id']
                if 'nom' in df_pkl.columns and 'nom' in df_mongo.columns:
                    df_pkl = df_pkl.merge(df_mongo[['nom'] + cols_add], on='nom', how='left')
        
        all_df.append(df_pkl)

    df_global = pd.concat(all_df, ignore_index=True)
    df_global[['latitude', 'longitude']] = df_global[['latitude', 'longitude']].apply(pd.to_numeric, errors='coerce')
    return df_global.dropna(subset=['latitude', 'longitude'])

# ─────────────────────────────────────────────
#  3. OUTILS DE CALCUL (DISTANCES & ROUTES)
# ─────────────────────────────────────────────
def geocoder_ville(ville: str):
    loc = Nominatim(user_agent="explora_app").geocode(f"{ville}, France")
    if not loc: raise ValueError("Ville introuvable.")
    return loc.latitude, loc.longitude

def haversine(lat1, lon1, lats2, lons2):
    R = 6371
    dlat, dlon = np.radians(lats2 - lat1), np.radians(lons2 - lon1)
    a = (np.sin(dlat / 2)**2 + np.cos(np.radians(lat1)) * np.cos(np.radians(lats2)) * np.sin(dlon / 2)**2)
    return R * 2 * np.arcsin(np.sqrt(a))

def routage_osrm(lat1, lon1, lat2, lon2):
    url = f"http://router.project-osrm.org/route/v1/driving/{lon1},{lat1};{lon2},{lat2}?overview=false"
    try:
        data = requests.get(url, timeout=4).json()
        if data.get('code') == 'Ok':
            route = data['routes'][0]
            return f"{route['distance']/1000:.1f} km (~{int(route['duration']/60)} min)"
    except: pass
    return f"≈ {haversine(lat1, lon1, np.array([lat2]), np.array([lon2]))[0]:.1f} km"

# ─────────────────────────────────────────────
#  4. SCORING & DIVERSITÉ
# ─────────────────────────────────────────────
THEMES_LABELS = ["Nature", "Gastronomie", "Sport", "Culture", "Détente", "Boutique"]

def scorer_activites(df, lat_c, lon_c, rayon_max, prefs):
    df = df.copy()
    df['dist_oiseau'] = haversine(lat_c, lon_c, df['latitude'].values, df['longitude'].values)
    df = df[df['dist_oiseau'] <= rayon_max].reset_index(drop=True)
    if df.empty: return df

    X = np.array([[row['scores'].get(t.lower(), 0) for t in THEMES_LABELS] for _, row in df.iterrows()])
    knn = NearestNeighbors(n_neighbors=len(df), metric='cosine').fit(X)
    dist, indices = knn.kneighbors(np.array(prefs).reshape(1, -1))

    df_sorted = df.iloc[indices[0]].copy()
    df_sorted['score_final'] = 0.6 * (1 - dist[0]) + 0.4 * (1 - (df_sorted['dist_oiseau'] / rayon_max))
    return df_sorted.sort_values(by='score_final', ascending=False).reset_index(drop=True)

# ─────────────────────────────────────────────
#  5. GÉNÉRATION DU PLANNING
# ─────────────────────────────────────────────
def generer_planning(df_scored, nb_jours, lat_c, lon_c):
    records = df_scored.drop_duplicates(subset=['nom']).to_dict('records')
    planning, MAX_DIST_KM = [], 40

    for jour in range(1, nb_jours + 1):
        matin, aprem, blacklist = [], [], set()

        def sim(nom, bl):
            mots = {m for m in nom.lower().split() if len(m) > 3}
            return not mots.isdisjoint(bl)

        # Sélection Matin
        idx = 0
        while len(matin) < 2 and idx < len(records):
            act = records[idx]
            if not sim(act['nom'], blacklist):
                matin.append(act)
                blacklist.update({m for m in act['nom'].lower().split() if len(m) > 3})
                records.pop(idx)
            else: idx += 1

        # Sélection Après-midi
        if matin:
            l_lat, l_lon = matin[-1]['latitude'], matin[-1]['longitude']
            idx = 0
            while len(aprem) < 2 and idx < len(records):
                act = records[idx]
                if haversine(l_lat, l_lon, act['latitude'], act['longitude']) <= MAX_DIST_KM and not sim(act['nom'], blacklist):
                    aprem.append(act)
                    blacklist.update({m for m in act['nom'].lower().split() if len(m) > 3})
                    records.pop(idx)
                else: idx += 1

        # Calcul des trajets uniquement entre activités
        tr = {'m1_m2': None, 'midi': None, 'a1_a2': None}
        if len(matin) == 2: tr['m1_m2'] = routage_osrm(matin[0]['latitude'], matin[0]['longitude'], matin[1]['latitude'], matin[1]['longitude'])
        if matin and aprem: tr['midi'] = routage_osrm(matin[-1]['latitude'], matin[-1]['longitude'], aprem[0]['latitude'], aprem[0]['longitude'])
        if len(aprem) == 2: tr['a1_a2'] = routage_osrm(aprem[0]['latitude'], aprem[0]['longitude'], aprem[1]['latitude'], aprem[1]['longitude'])

        planning.append({'jour': jour, 'matin': matin, 'aprem': aprem, 'trajets': tr})
    return planning

# ─────────────────────────────────────────────
#  6. AFFICHAGE
# ─────────────────────────────────────────────
def afficher_planning(planning, ville):
    SEP, SUB = "═" * 60, "─" * 60
    print(f"\n{SEP}\n    EXPLORA : {ville.upper()}\n{SEP}")

    for j in planning:
        print(f"\n{SUB}\n  JOUR {j['jour']}\n{SUB}")
        if j['matin']:
            print(f"   MATIN : {j['matin'][0]['nom']}")
            if j['trajets']['m1_m2']: print(f"      {j['trajets']['m1_m2']}")
            if len(j['matin']) > 1: print(f"   MATIN : {j['matin'][1]['nom']}")
        
        if j['trajets']['midi']: print(f"\n  🍴 MIDI (Liaison) : {j['trajets']['midi']}")

        if j['aprem']:
            print(f"   APREM : {j['aprem'][0]['nom']}")
            if j['trajets']['a1_a2']: print(f"      {j['trajets']['a1_a2']}")
            if len(j['aprem']) > 1: print(f"   APREM : {j['aprem'][1]['nom']}")
    print(f"\n{SEP}")
def lancer_explora(ville, rayon, jours, nature , gastronomie , sport, culture , detente , boutique ):
    # 1. Connexion et chargement (on garde cette logique interne)
    client = connecter_mongodb()
    df_global = charger_donnees(client)
    
    # 2. Localisation (utilise l'argument 'ville')
    try:
        lat_c, lon_c = geocoder_ville(ville)
    except ValueError:
        print(f"Erreur : La ville '{ville}' n'a pas été trouvée.")
        client.close()
        return

    # 3. Préparation des préférences (on regroupe les arguments dans une liste)
    # L'ordre doit correspondre à celui de ton modèle KNN (THEMES_LABELS)
    prefs = [nature, gastronomie, sport, culture, detente, boutique]

    # 4. Scoring et filtrage
    # On utilise 'rayon' passé en argument
    df_scored = scorer_activites(df_global, lat_c, lon_c, rayon, prefs)
    
    if not df_scored.empty:
        # 5. Génération et affichage (on utilise 'jours' passé en argument)
        planning = generer_planning(df_scored, jours, lat_c, lon_c)
        afficher_planning(planning, ville)
    else:
        print(f"Rien trouvé dans un rayon de {rayon} km autour de {ville}.")
    
    client.close()

if __name__ == "__main__": lancer_explora()
