import numpy as np
import pandas as pd
from sklearn.neighbors import NearestNeighbors
from geopy.geocoders import Nominatim
from pymongo import MongoClient # <-- Nouvel import indispensable

geolocator = Nominatim(user_agent="explora_simple")

def calculer_distance(lat_c, lon_c, lats, lons):
    """Calcul de la distance à vol d'oiseau (Formule de Haversine)"""
    R = 6371
    dlat = np.radians(lats - lat_c)
    dlon = np.radians(lons - lon_c)
    a = np.sin(dlat/2)**2 + np.cos(np.radians(lat_c)) * np.cos(np.radians(lats)) * np.sin(dlon/2)**2
    return R * 2 * np.arcsin(np.sqrt(a))

def lancer_explora_mongodb():
    # --- 1. CONNEXION À MONGODB ---
    print("Connexion à la base de données MongoDB...")
    # Remplace par ton URI Atlas (ex: "mongodb+srv://user:mdp@cluster.mongodb.net/")
    # Ou laisse localhost si ta base est sur ton PC
    uri = "mongodb://localhost:27017/" 
    client = MongoClient(uri)
    
    # Sélection de la base et de la collection (à adapter selon tes noms)
    db = client["LORA_voyage"] 
    collection = db["objects"]

    # --- 2. EXTRACTION DES DONNÉES ---
    print("Récupération des activités depuis la base...")
    # On récupère tous les documents (équivalent de la fusion de tes anciens PKL)
    cursor = collection.find({}) 
    df_global = pd.DataFrame(list(cursor))

    if df_global.empty:
        print("La base de données est vide ou introuvable.")
        return
        
    # Nettoyage
    df_global['latitude'] = pd.to_numeric(df_global['latitude'], errors='coerce')
    df_global['longitude'] = pd.to_numeric(df_global['longitude'], errors='coerce')
    df_global = df_global.dropna(subset=['latitude', 'longitude'])
    
    print(f"{len(df_global)} activités chargées au total.")

    # --- 3. LOCALISATION UTILISATEUR ---
    ville_user = input("\nDans quelle ville es-tu ? ").strip()
    print(f"Recherche du centre de {ville_user}...")
    loc = geolocator.geocode(f"{ville_user}, France")

    if not loc:
        print("Ville introuvable.")
        return

    lat_c, lon_c = loc.latitude, loc.longitude
    print(f"→ Coordonnées : {lat_c:.4f}, {lon_c:.4f}")

    rayon_max = float(input("Rayon de recherche (km) : ").strip() or 20)

    # --- 4. FILTRAGE GÉOGRAPHIQUE ---
    df_global['dist'] = calculer_distance(lat_c, lon_c, 
                                          df_global['latitude'].values, 
                                          df_global['longitude'].values)
    
    df_proche = df_global[df_global['dist'] <= rayon_max].copy().reset_index(drop=True)
    
    if df_proche.empty:
        print(f"Aucune activité trouvée dans un rayon de {rayon_max} km.")
        return

    print(f" {len(df_proche)} activités trouvées à proximité.")

    # --- 5. PRÉFÉRENCES ---
    print("\nNotes (0-10) pour vos préférences :")
    themes = ["Nature", "Gastronomie", "Sport", "Culture", "Détente"]
    n_user = [float(input(f"  {t} : ") or 5.0) for t in themes]

    # --- 6. TRI HYBRIDE (IA + PROXIMITÉ) ---
    # Extraction des scores comme dans ton ancien code
    X = np.array([[s.get('nature', 0), s.get('gastronomie', 0), s.get('sport', 0), 
                   s.get('culture', 0), s.get('detente', 0)] 
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

    # --- 7. AFFICHAGE ---
    print(f"\nTOP 20 — {ville_user.upper()} ({rayon_max} km)")
    print("-" * 65)
    for rank, i in enumerate(ordre, start=1):
        idx = indices[0][i]
        act = df_proche.iloc[idx]
        score = int(score_final[i] * 100)
        print(f"{rank:>2}. {score}% - {act['nom']} - {act.get('ville', 'Inconnue')} - {act['dist']:.1f}km")

# Lancement
if __name__ == "__main__":
    lancer_explora_mongodb()