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

        print(f" Chargement PKL : {dossier_pkl}")

        fichiers_pkl = [f for f in os.listdir(dossier_pkl) if f.endswith(".pkl")]
        all_df = []

        for f in fichiers_pkl:
            chemin = os.path.join(dossier_pkl, f)

            with open(chemin, "rb") as fh:
                data = pickle.load(fh)

            df_pkl = data["df"] if isinstance(data, dict) and "df" in data else data
            all_df.append(df_pkl)

        client.close()

        df_global = pd.concat(all_df, ignore_index=True)

        df_global[['latitude', 'longitude']] = df_global[['latitude', 'longitude']].apply(
            pd.to_numeric, errors='coerce'
        )


        # 🔥 STABILISATION IMPORTANT
        df_global = df_global.drop_duplicates(subset=["nom"], keep="last")

        print(f" OK {len(df_global)} activités chargées")
        return df_global

    except Exception as e:
        print(" Erreur :", e)
        return None


# ─────────────────────────────────────────────
# 2. DISTANCE
# ─────────────────────────────────────────────
def haversine(lat1, lon1, lat2, lon2):
    R = 6371
    dlat, dlon = np.radians(lat2 - lat1), np.radians(lon2 - lon1)

    a = (np.sin(dlat / 2) ** 2 +
         np.cos(np.radians(lat1)) *
         np.cos(np.radians(lat2)) *
         np.sin(dlon / 2) ** 2)

    return R * 2 * np.arcsin(np.sqrt(a))


# ─────────────────────────────────────────────
# 3. THÈME STABLE (IMPORTANT FIX)
# ─────────────────────────────────────────────
def get_theme(scores):
    if isinstance(scores, dict) and len(scores) > 0:
        return max(scores, key=scores.get)
    return "unknown"


# ─────────────────────────────────────────────
# 4. ALGO DIVERSITÉ
# ─────────────────────────────────────────────
def suggerer_top_10_differents(nom_act_ref, df_global, rayon_km=40):

    themes = ["nature", "gastronomie", "sport", "culture", "detente", "boutique"]

    cible_df = df_global[df_global["nom"] == nom_act_ref]
    if cible_df.empty:
        return f"Activité '{nom_act_ref}' introuvable."

    cible = cible_df.iloc[0]

    theme_ref = max(cible["scores"], key=cible["scores"].get)

    print(f"\n[DEBUG] Activité : {nom_act_ref}")
    print(f"[DEBUG] Thème exclu : {theme_ref}")

    lat_ref, lon_ref = cible["latitude"], cible["longitude"]

    df_temp = df_global.copy()

    df_temp["Dist_Val"] = haversine(
        lat_ref, lon_ref,
        df_temp["latitude"], df_temp["longitude"]
    )

    candidats = df_temp[
        (df_temp["Dist_Val"] <= rayon_km) &
        (df_temp["nom"] != nom_act_ref)
    ].copy()

    candidats = candidats.drop_duplicates(subset=["nom"])

    #  filtre anti même thème
    candidats["Theme_Dom"] = candidats["scores"].apply(get_theme)
    candidats = candidats[candidats["Theme_Dom"] != theme_ref]

    if candidats.empty:
        return "Pas assez de diversité locale."

    results = []

    for t in themes:
        if t == theme_ref:
            continue

        subset = candidats.copy()
        subset["score_theme"] = subset["scores"].apply(lambda x: x.get(t, 0))

        top = subset.sort_values(by="score_theme", ascending=False).head(2)

        for _, row in top.iterrows():
            results.append({
                "Activité": row["nom"],
                "Distance": f"{row['Dist_Val']:.1f} km",
                "Type": row["Theme_Dom"]
            })

    df_res = pd.DataFrame(results).drop_duplicates()

    return df_res.head(10)


# ─────────────────────────────────────────────
# 5. MAIN
# ─────────────────────────────────────────────
if __name__ == "__main__":

    df = charger_donnees_completes()

    if df is not None:
        nom_input = input("\n Entrez activité : ").strip()

        resultats = suggerer_top_10_differents(nom_input, df)

        print("\n RESULTATS :\n")
        print(resultats)