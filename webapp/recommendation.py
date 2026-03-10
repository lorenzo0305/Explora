# Fusion des fichiers pkl, on n'a pas besoin de préciser la région, juste la ville. Propose les activités proches qui nous correspondent et qui sont dans
# le rayon. Peut proposer des activités qui ne sont pas dans la même région mais dans le rayon...ce n'est pas le cas ici car on n'a que 2 régions qui
# ne sont pas du tout à coté.

import pickle
import numpy as np
import os
import pandas as pd
from sklearn.neighbors import NearestNeighbors
from geopy.geocoders import Nominatim

geolocator = Nominatim(user_agent="explora_simple")

def calculer_distance(lat_c, lon_c, lats, lons):
    R = 6371
    dlat = np.radians(lats - lat_c)
    dlon = np.radians(lons - lon_c)
    a = np.sin(dlat/2)**2 + np.cos(np.radians(lat_c)) * np.cos(np.radians(lats)) * np.sin(dlon/2)**2
    return R * 2 * np.arcsin(np.sqrt(a))

def lancer_explora_complet(ville_user, rayon_max, nature, gastronomie, sport, culture, detente):
    dossier = "/kaggle/working/data/models"
    fichiers = [f for f in os.listdir(dossier) if f.endswith('_REGIONALE.pkl')]

    if not fichiers:
        print("Aucun fichier .pkl trouvé.")
        return

    # --- FUSION DES DONNÉES ---
    all_df = []
    print(f" Fusion des régions : {[f.replace('_REGIONALE.pkl', '') for f in fichiers]}...")
    for f in fichiers:
        with open(os.path.join(dossier, f), 'rb') as tmp:
            data = pickle.load(tmp)
            all_df.append(data['df'])
    
    df_global = pd.concat(all_df, ignore_index=True)
    
    # Nettoyage
    df_global['latitude'] = pd.to_numeric(df_global['latitude'], errors='coerce')
    df_global['longitude'] = pd.to_numeric(df_global['longitude'], errors='coerce')
    df_global = df_global.dropna(subset=['latitude', 'longitude'])
    
    print(f"{len(df_global)} activités chargées au total.")

    # --- LOCALISATION ---
    #ville_user = input("\nDans quelle ville es-tu ? ").strip()
    print(f"Recherche du centre de {ville_user}...")
    loc = geolocator.geocode(f"{ville_user}, France")

    if not loc:
        print("Ville introuvable.")
        return

    lat_c, lon_c = loc.latitude, loc.longitude
    print(f"→ Coordonnées : {lat_c:.4f}, {lon_c:.4f}")

    #rayon_max = float(input("Rayon de recherche (km) : ").strip() or 20)

    # --- FILTRAGE GÉOGRAPHIQUE ---
    df_global['dist'] = calculer_distance(lat_c, lon_c, 
                                          df_global['latitude'].values, 
                                          df_global['longitude'].values)
    
    df_proche = df_global[df_global['dist'] <= rayon_max].copy().reset_index(drop=True)
    
    if df_proche.empty:
        print(f"Aucune activité trouvée dans un rayon de {rayon_max} km.")
        return

    print(f" {len(df_proche)} activités trouvées à proximité.")

    # --- PRÉFÉRENCES ---
    print("\nNotes (0-10) pour vos préférences :")
    themes = ["Nature", "Gastronomie", "Sport", "Culture", "Détente"]
    n_user = [nature,gastronomie,sport,culture,detente]

    # --- TRI HYBRIDE (IA + PROXIMITÉ) ---
    X = np.array([[s['nature'], s['gastronomie'], s['sport'], s['culture'], s['detente']] 
                  for s in df_proche['scores']])

    knn = NearestNeighbors(n_neighbors=len(df_proche), metric='cosine')
    knn.fit(X)
    dist_knn, indices = knn.kneighbors(np.array([n_user]))

    score_sim = 1 - dist_knn[0]
    # Normalisation de la distance pour le score
    dist_norm = df_proche['dist'].values[indices[0]] / rayon_max
    score_prox = 1 - dist_norm
    
    # Mix : 60% Goûts, 40% Distance
    score_final = 0.4 * score_prox + 0.6 * score_sim
    ordre = np.argsort(score_final)[::-1][:20]

    # --- AFFICHAGE ---
    print(f"\nTOP 20 — {ville_user.upper()} ({rayon_max} km)")
    print("-" * 65)
    for rank, i in enumerate(ordre, start=1):
        idx = indices[0][i]
        act = df_proche.iloc[idx]
        score = int(score_final[i] * 100)
        print(f"{rank:>2}. {score}% - {act['nom']} - {act['ville']} - {act['dist']:.1f}km")

# Lancement
lancer_explora_complet()
