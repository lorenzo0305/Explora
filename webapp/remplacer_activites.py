import os
import pickle
import urllib.parse
from pathlib import Path
import numpy as np
import pandas as pd
import certifi
from pymongo import MongoClient

# ─────────────────────────────────────────────
# 1. CHARGEMENT DONNÉES
# ─────────────────────────────────────────────
def charger_donnees_completes():
    print(" Connexion à MongoDB...")
    user, password = "equipe_explora", "2BqXsiNi8nCCE@W"
    uri = f"mongodb+srv://{user}:{urllib.parse.quote_plus(password)}@datas.xc1dpyu.mongodb.net/?appName=datas"

    try:
        client = MongoClient(uri, tlsCAFile=certifi.where())
        db = client["explora"]

        dossier_pkl = "/kaggle/input/datasets/sarahesiee/fichier-pkl"
        if not os.path.exists(dossier_pkl):
            base_dir = Path(__file__).resolve().parent
            dossier_pkl = str(base_dir / "data" / "models")

        print(f" Chargement des fichiers PKL depuis : {dossier_pkl}")
        fichiers_pkl = [f for f in os.listdir(dossier_pkl) if f.endswith('.pkl')]
        all_df = []

        for f in fichiers_pkl:
            chemin = os.path.join(dossier_pkl, f)
            with open(chemin, 'rb') as fh:
                data = pickle.load(fh)
            df_pkl = data['df'] if isinstance(data, dict) and 'df' in data else data
            all_df.append(df_pkl)

        client.close()
        df_global = pd.concat(all_df, ignore_index=True)
        df_global[['latitude', 'longitude']] = df_global[['latitude', 'longitude']].apply(pd.to_numeric, errors='coerce')
        df_global = df_global.dropna(subset=['latitude', 'longitude', 'scores'])

        print(f" Données chargées : {len(df_global)} activités")
        return df_global
    except Exception as e:
        print(f" Erreur de chargement : {e}")
        return None

# ─────────────────────────────────────────────
# 2. HAVERSINE (Distance géographique)
# ─────────────────────────────────────────────
def haversine(lat1, lon1, lat2, lon2):
    R = 6371
    dlat, dlon = np.radians(lat2 - lat1), np.radians(lon2 - lon1)
    a = (np.sin(dlat / 2)**2 +
         np.cos(np.radians(lat1)) *
         np.cos(np.radians(lat2)) *
         np.sin(dlon / 2)**2)
    return R * 2 * np.arcsin(np.sqrt(a))

# ─────────────────────────────────────────────
# 3. ALGO DIFFÉRENT (DIVERSITÉ)
# ─────────────────────────────────────────────
def suggerer_top_10_differents(nom_act_ref, df_global, rayon_km=40):
    themes = ["nature", "gastronomie", "sport", "culture", "detente", "boutique"]
    
    # 1. Trouver l'activité cible
    cible_df = df_global[df_global['nom'] == nom_act_ref]
    if cible_df.empty:
        return f"L'activité '{nom_act_ref}' n'existe pas dans la base."
    
    cible = cible_df.iloc[0]
    
    # Détection du thème à éviter
    theme_ref = max(cible['scores'], key=cible['scores'].get)
    print(f"\n[DEBUG] Activité : {nom_act_ref}")
    print(f"[DEBUG] Thème à ÉCARTER : {theme_ref.upper()}")

    lat_ref, lon_ref = cible['latitude'], cible['longitude']

    # 2. Filtrage Géographique + Suppression doublons
    df_temp = df_global.copy()
    df_temp['Dist_Val'] = haversine(lat_ref, lon_ref, df_temp['latitude'], df_temp['longitude'])
    
    # Filtre distance + exclusion de l'activité elle-même
    candidats = df_temp[(df_temp['Dist_Val'] <= rayon_km) & (df_temp['nom'] != nom_act_ref)].copy()
    candidats = candidats.drop_duplicates(subset=['nom'])

    # Filtre sur les noms proches (pour éviter de proposer le resto du château)
    premier_mot = nom_act_ref.split()[0]
    if len(premier_mot) > 3:
        candidats = candidats[~candidats['nom'].str.contains(premier_mot, case=False, na=False)]

    if candidats.empty:
        return f"Aucune alternative différente trouvée dans un rayon de {rayon_km} km."

    # 3. Logique de Diversité : Exclusion du thème dominant
    def get_dominant(s): return max(s, key=s.get)
    candidats['Theme_Dom'] = candidats['scores'].apply(get_dominant)
    
    # On ne garde que ce qui n'est pas du même thème
    candidats_diff = candidats[candidats['Theme_Dom'] != theme_ref].copy()

    if candidats_diff.empty:
        return "Pas assez de diversité thématique à proximité."

    # 4. Sélection des meilleurs représentants par catégorie
    # On veut les 10 meilleures alternatives (2 par thème restant environ)
    autres_themes = [t for t in themes if t != theme_ref]
    suggestions_list = []

    for t in autres_themes:
        # On prend les activités qui scorent le plus haut dans ce thème spécifique
        subset = candidats_diff.copy()
        subset['Score_Theme'] = subset['scores'].apply(lambda x: x.get(t, 0))
        
        # On prend les 2 meilleures activités pour chaque thème différent
        top_pour_ce_theme = subset.sort_values(by="Score_Theme", ascending=False).head(2)
        
        for _, row in top_pour_ce_theme.iterrows():
            suggestions_list.append({
                "Activité": row['nom'],
                "Distance_Km": row['Dist_Val'],
                "Distance": f"{row['Dist_Val']:.1f} km",
                "Type": row['Theme_Dom'].upper(),
                "Note_Interet": round(row['scores'].get(row['Theme_Dom'], 0) * 100) # Score du thème dominant
            })

    # 5. Création du DataFrame et TRI FINAL
    df_res = pd.DataFrame(suggestions_list).drop_duplicates(subset=['Activité'])

    # TRI : On met en avant les activités avec les plus gros scores thématiques (qualité)
    # PUIS par distance
    df_res = df_res.sort_values(by=["Note_Interet", "Distance_Km"], ascending=[False, True])

    # On retourne les colonnes propres (on affiche le Type au lieu du Match %)
    return df_res[["Activité", "Distance", "Type"]].head(10)

# ─────────────────────────────────────────────
# 4. MAIN
# ─────────────────────────────────────────────
if __name__ == "__main__":
    df = charger_donnees_completes()

    if df is not None:
        

        nom_input = input("\n Entrez le nom de l'activité à remplacer : ").strip()
        resultats = suggerer_top_10_differents(nom_input, df)
        print(resultats)