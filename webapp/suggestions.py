import os
import pickle
from pathlib import Path
import numpy as np
import pandas as pd
import requests
import certifi
from pymongo import MongoClient
from sklearn.neighbors import NearestNeighbors

try:
    from webapp.db_config import get_mongo_uri, get_db_name
except ImportError:
    from db_config import get_mongo_uri, get_db_name

# ─────────────────────────────────────────────
#  1. CHARGEMENT COMPLET (MONGODB + PKL)
# ─────────────────────────────────────────────
def charger_donnees_completes():
    try:
        client = MongoClient(get_mongo_uri(), tlsCAFile=certifi.where())
        db = client[get_db_name()]
        
        # Chemin vers tes fichiers PKL (Kaggle ou Local)
        dossier_pkl = "/kaggle/input/datasets/sarahesiee/fichier-pkl"
        if not os.path.exists(dossier_pkl):
            base_dir = Path(__file__).resolve().parent
            dossier_pkl = str(base_dir / "data" / "models")

        fichiers_pkl = [f for f in os.listdir(dossier_pkl) if f.endswith('.pkl')]
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
                    cols_add = [c for c in df_mongo.columns if c not in df_pkl.columns]
                    if 'nom' in df_pkl.columns and 'nom' in df_mongo.columns:
                        df_pkl = df_pkl.merge(df_mongo[['nom'] + cols_add], on='nom', how='left')
            
            all_df.append(df_pkl)

        client.close()
        df_global = pd.concat(all_df, ignore_index=True)
        df_global[['latitude', 'longitude']] = df_global[['latitude', 'longitude']].apply(pd.to_numeric, errors='coerce')
        
        return df_global.dropna(subset=['latitude', 'longitude', 'scores'])
    except Exception as e:
        print(f"Erreur de chargement : {e}")
        return None

# ─────────────────────────────────────────────
#  2. CALCUL DISTANCE (HAVERSINE)
# ─────────────────────────────────────────────
def haversine(lat1, lon1, lat2, lon2):
    R = 6371
    dlat, dlon = np.radians(lat2 - lat1), np.radians(lon2 - lon1)
    a = (np.sin(dlat / 2)**2 + np.cos(np.radians(lat1)) * np.cos(np.radians(lat2)) * np.sin(dlon / 2)**2)
    return R * 2 * np.arcsin(np.sqrt(a))

# ─────────────────────────────────────────────
#  3. ALGORITHME DE SUGGESTION
# ─────────────────────────────────────────────
def suggerer_top_10_alternatives(nom_act_ref, df_global, rayon_km=40):
    themes = ["nature", "gastronomie", "sport", "culture", "detente", "boutique"]
    
    # Trouver l'activité cible
    cible_df = df_global[df_global['nom'] == nom_act_ref]
    if cible_df.empty:
        return f"L'activité '{nom_act_ref}' n'existe pas dans la base."
    
    cible = cible_df.iloc[0]
    
    # DEBUG : Affichage des scores de l'activité cliquée
    print(f"\n[DEBUG] Activité : {nom_act_ref}")
    theme_dominant = max(cible['scores'], key=cible['scores'].get)
    print(f"[DEBUG] Thème dominant détecté : {theme_dominant.upper()}")

    lat_ref, lon_ref = cible['latitude'], cible['longitude']
    vecteur_ref = np.array([[cible['scores'].get(t, 0) for t in themes]])

    # Filtrage Géographique + Suppression doublons
    df_temp = df_global.copy()
    df_temp['Dist_Val'] = haversine(lat_ref, lon_ref, df_temp['latitude'], df_temp['longitude'])
    
    candidats = df_temp[(df_temp['Dist_Val'] <= rayon_km) & (df_temp['nom'] != nom_act_ref)]
    candidats = candidats.drop_duplicates(subset=['nom']).copy()

    if candidats.empty:
        return f"Aucune alternative trouvée dans un rayon de {rayon_km} km."

    # Calcul de similarité (KNN)
    X = np.array([[row['scores'].get(t, 0) for t in themes] for _, row in candidats.iterrows()])
    nb_resultats = min(15, len(candidats)) 
    knn = NearestNeighbors(n_neighbors=nb_resultats, metric='cosine').fit(X)
    distances, indices = knn.kneighbors(vecteur_ref)

    # Préparation de la liste de résultats
    suggestions_list = []
    for i, idx in enumerate(indices[0]):
        row = candidats.iloc[idx]
        match_pct = round((1 - distances[0][i]) * 100)
        
        suggestions_list.append({
            "Activité": row['nom'],
            "Distance_Km": row['Dist_Val'], # Pour le tri
            "Distance": f"{row['Dist_Val']:.1f} km",
            "Match_Val": match_pct,         # Pour le tri
            "Match": f"{match_pct}%"
        })

    # Création du DataFrame et TRI FINAL
    df_res = pd.DataFrame(suggestions_list)

    # TRI : Match décroissant (les 100% en haut) 
    # PUIS Distance croissante (les plus proches en haut si Match égal)
    df_res = df_res.sort_values(
        by=["Match_Val", "Distance_Km"], 
        ascending=[False, True]
    )

    # On retourne uniquement les colonnes propres et les 10 meilleurs
    return df_res[["Activité", "Distance", "Match"]].head(10)

# ─────────────────────────────────────────────
#  4. TEST ET AFFICHAGE
# ─────────────────────────────────────────────
if __name__ == "__main__":
    print("Chargement des données en cours...")
    df_data = charger_donnees_completes()
    
    if df_data is not None:
        nom_test = input("\nNom de l'activité cliquée : ").strip()
        resultats = suggerer_top_10_alternatives(nom_test, df_data)
        
        if isinstance(resultats, pd.DataFrame):
            print("\n" + "═" * 65)
            # Affichage formaté sans l'index de gauche
            print(resultats.to_string(index=False, justify='left'))
            print("═" * 65)
        else:
            print(resultats)