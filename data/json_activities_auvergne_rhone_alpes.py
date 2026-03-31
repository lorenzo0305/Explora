import os
import json
from pathlib import Path
from datetime import datetime

# --- CONFIGURATION ---
BASE_DIR = Path(__file__).resolve().parent
DOSSIER_SOURCE = BASE_DIR / "Auvergne_Rhone_Alpes_object"
FICHIER_SORTIE = BASE_DIR / "Auvergne_Rhone_Alpes_propre_intersaison.json"

# --- LE DICTIONNAIRE OFFICIEL (Inchangé) ---
DICTIONNAIRE_CATEGORIES = {
    # 🏅 SPORT & LOISIRS ACTIFS
    "AdventurePark": "sport", "AccompaniedPractice": "sport", "BilliardRoom": "sport", 
    "BoulesPitch": "sport", "BowlingAlley": "sport", "ClimbingWall": "sport", 
    "Competition": "sport", "CrossCountrySkiTrail": "sport", "CyclingTour": "sport", 
    "DogSleddingTrail": "sport", "DownhillSkiRun": "sport", "EquestrianCenter": "sport", 
    "FitnessCenter": "sport", "FitnessPath": "sport", "FreePractice": "sport", "Game": "sport", 
    "GolfCourse": "sport", "Gymnasium": "sport", "IceSkatingRink": "sport", 
    "LeisureSportActivityProvider": "sport", "MiniGolf": "sport", "MultiActivity": "sport", 
    "NauticalCentre": "sport", "Practice": "sport", "Racetrack": "sport", 
    "RacingCircuit": "sport", "RailBike": "sport", "Rally": "sport", "Rambling": "sport", 
    "SportsAndLeisurePlace": "sport", "SportsClub": "sport", "SportsCompetition": "sport", 
    "SportsEvent": "sport", "SquashCourt": "sport", "Stadium": "sport", 
    "SummerToboggan": "sport", "SwimmingPool": "sport", "TennisComplex": "sport", 
    "TobogganBobsleigh": "sport", "TrackRollerOrSkateBoard": "sport", "Trampoline": "sport", 
    "Velodrome": "sport", "ViaFerrata": "sport", "schema:GolfCourse": "sport", 
    "schema:SportsEvent": "sport", "schema:StadiumOrArena": "sport",

    # 🥐 GASTRONOMIE & TERROIR
    "Bakery": "gastronomie", "BistroOrWineBar": "gastronomie", "BrasserieOrTavern": "gastronomie", 
    "Brewery": "gastronomie", "CafeOrTeahouse": "gastronomie", "Cellar": "gastronomie", 
    "CoveredMarket": "gastronomie", "FarmhouseInn": "gastronomie", "FastFoodRestaurant": "gastronomie", 
    "FoodEstablishment": "gastronomie", "GourmetRestaurant": "gastronomie", 
    "HotelRestaurant": "gastronomie", "IceCreamShop": "gastronomie", "Market": "gastronomie", 
    "Producer": "gastronomie", "ProducersGroup": "gastronomie", "Restaurant": "gastronomie", 
    "SelfServiceCafeteria": "gastronomie", "TastingProvider": "gastronomie", 
    "schema:Bakery": "gastronomie", "schema:CafeOrCoffeeShop": "gastronomie", 
    "schema:FastFoodRestaurant": "gastronomie", "schema:FoodEstablishment": "gastronomie", 
    "schema:IceCreamShop": "gastronomie", "schema:Restaurant": "gastronomie", 
    "schema:Winery": "gastronomie",

    # 🎭 CULTURE & PATRIMOINE
    "Abbey": "culture", "ArcheologicalSite": "culture", "ArtGalleryOrExhibitionGallery": "culture", 
    "ArtistSigning": "culture", "Basilica": "culture", "Castle": "culture", "Cathedral": "culture", 
    "Chapel": "culture", "Chartreuse": "culture", "Church": "culture", "Cinema": "culture", 
    "Cinematheque": "culture", "CircusPlace": "culture", "Cirque": "culture", "Citadel": "culture", 
    "CityHeritage": "culture", "CivilCemetery": "culture", "Cliff": "culture", "Cloister": "culture", 
    "Coastline": "culture", "Col": "culture", "Collegiate": "culture", "Commanderie": "culture", 
    "Commemoration": "culture", "Concert": "culture", "Convent": "culture", 
    "CulturalEvent": "culture", "CulturalSite": "culture", "Culture": "culture", 
    "DefenceSite": "culture", "Dungeon": "culture", "EducationalTrail": "culture", 
    "Exhibition": "culture", "Festival": "culture", "Fort": "culture", "FortifiedCastle": "culture", 
    "InterpretationCentre": "culture", "Library": "culture", "MegalithDolmenMenhir": "culture", 
    "Monastery": "culture", "Mosque": "culture", "Museum": "culture", "Opera": "culture", 
    "Palace": "culture", "Parade": "culture", "PilgrimageAndProcession": "culture", 
    "Reading": "culture", "Recital": "culture", "ReligiousEvent": "culture", "ReligiousSite": "culture", 
    "RemarkableBuilding": "culture", "RemembranceSite": "culture", "Ruins": "culture", 
    "ShowEvent": "culture", "Synagogue": "culture", "TechnicalHeritage": "culture", "Temple": "culture", 
    "Theater": "culture", "TheaterEvent": "culture", "TraditionalCelebration": "culture", 
    "VisualArtsEvent": "culture", "schema:CivicStructure": "culture", "schema:ExhibitionEvent": "culture", 
    "schema:Festival": "culture", "schema:Library": "culture", "schema:MovieTheater": "culture", 
    "schema:Museum": "culture", "schema:MusicEvent": "culture", "schema:TheaterEvent": "culture",

    # 🌲 NATURE & DÉCOUVERTE
    "AlpinePasture": "nature", "Beach": "nature", "BeachClub": "nature", "Bocage": "nature", 
    "Bog": "nature", "Canal": "nature", "Canyon": "nature", "CaveSinkholeOrAven": "nature", 
    "Cirque": "nature", "Cliff": "nature", "Coastline": "nature", "Col": "nature", 
    "ConeNeck": "nature", "Crest": "nature", "Dune": "nature", "Forest": "nature", 
    "Glacier": "nature", "Gorge": "nature", "Hillsides": "nature", "IslandPeninsula": "nature", 
    "Lake": "nature", "Landes": "nature", "Mountain": "nature", "NaturalCuriosity": "nature", 
    "NaturalHeritage": "nature", "Orchard": "nature", "ParkAndGarden": "nature", "Peak": "nature", 
    "Plain": "nature", "Plateau": "nature", "Pond": "nature", "River": "nature", "Source": "nature", 
    "Stone": "nature", "Stream": "nature", "Swamp": "nature", "Valley": "nature", 
    "Volcano": "nature", "Waterfall": "nature", "Wetland": "nature", "VivariumAquarium": "nature", 
    "ZooAnimalPark": "nature", "schema:Aquarium": "nature", "schema:Landform": "nature", 
    "schema:Park": "nature", "schema:Zoo": "nature",

    # 🛍️ BOUTIQUES & ARTISANAT
    "AntiqueAndSecondhandGoodDealer": "boutique", "BoutiqueOrLocalShop": "boutique", 
    "BricABrac": "boutique", "CraftsmanShop": "boutique", "DepartmentStore": "boutique", 
    "EquipmentRentalShop": "boutique", "GarageSale": "boutique", 
    "HypermarketAndSupermarket": "boutique", "LocalProductsShop": "boutique", 
    "ShoppingCentreAndGallery": "boutique", "Store": "boutique", "Trader": "boutique",

    # 🧖‍♀️ DÉTENTE & BIEN-ÊTRE
    "BalneotherapyCentre": "détente", "Hammam": "détente", "Spa": "détente", 
    "ThalassotherapyCentre": "détente"
}

def calculer_duree_mois(start_str, end_str):
    try:
        debut = datetime.strptime(start_str, "%Y-%m-%d")
        fin = datetime.strptime(end_str, "%Y-%m-%d")
        nb_mois = (fin.year - debut.year) * 12 + (fin.month - debut.month)
        return abs(nb_mois), debut.month
    except:
        return None, None

print("🔍 Extraction et analyse des durées...")
activites_propres = []
fichiers_json = sorted(DOSSIER_SOURCE.rglob("*.json"))

for chemin_fichier in fichiers_json:
    try:
        with open(chemin_fichier, 'r', encoding='utf-8') as f:
            data = json.load(f)

        nom = data.get("rdfs:label", {}).get("fr", [""])[0]
        tags_officiels = data.get("@type", [])
        
        # --- CATÉGORISATION ---
        categories_trouvees = list(set([DICTIONNAIRE_CATEGORIES[t] for t in tags_officiels if t in DICTIONNAIRE_CATEGORIES]))
        if not categories_trouvees:
            categories_trouvees.append("autre")

        # --- CALCUL DURÉE ---
        nb_mois = None
        mois_debut = None
        offres = data.get("offers", [])
        if offres:
            specs = offres[0].get("schema:priceSpecification", [])
            if specs:
                periodes = specs[0].get("appliesOnPeriod", [])
                if periodes:
                    start = periodes[0].get("startDate", "")
                    end = periodes[0].get("endDate", "")
                    if start and end:
                        nb_mois, mois_debut = calculer_duree_mois(start, end)

        # --- LOGIQUE SAISONNIÈRE ---
        est_sport_nature = any(c in ["sport", "nature"] for c in categories_trouvees)
        saison_finale = "toute l'année"

        if nb_mois is not None:
            if est_sport_nature:
                if nb_mois < 4:
                    saison_finale = "hiver" if mois_debut in [11, 12, 1, 2] else "été"
                elif 4 <= nb_mois <= 8:
                    saison_finale = "Intersaison (sport/nature)"
                else:
                    saison_finale = "toute l'année(sport/nature)"
            else:
                # Autres catégories (Culture, Gastronomie, Détente...)
                if nb_mois > 8:
                    saison_finale = "toute l'année(Autres)"
                else:
                    saison_finale = "Intersaison(Autres)"
        else:
            # Sécurité si pas de dates : on regarde les tags prioritaires
            if any(t in ["SkiResort", "DownhillSkiRun"] for t in tags_officiels):
                saison_finale = "hiver"

        # --- SÉCURITÉ MOTS-CLÉS (Correction des anomalies comme le Bowling ou Balicina) ---
        nom_l = nom.lower()
        if any(w in nom_l for w in ["bowling", "cinéma", "spa", "balnéo", "billard"]):
            saison_finale = "toute l'année"
            if "spa" in nom_l or "balnéo" in nom_l:
                if "détente" not in categories_trouvees: categories_trouvees.append("détente")

        # --- LOCALISATION & CONTACT (Ton code d'origine) ---
        ville, cp, adresse, region, lat, lon = "", "", "", "", "", ""
        is_located = data.get("isLocatedAt", [])
        if is_located:
            loc = is_located[0]
            addr = loc.get("schema:address", [{}])[0]
            ville = addr.get("schema:addressLocality", "")
            cp = addr.get("schema:postalCode", "")
            street = addr.get("schema:streetAddress", [])
            if street: adresse = street[0]
            geo = loc.get("schema:geo", {})
            lat = geo.get("schema:latitude", "")
            lon = geo.get("schema:longitude", "")

        tel, site_web = "", ""
        contacts = data.get("hasContact", [])
        if contacts:
            contact = contacts[0]
            tel = contact.get("schema:telephone", [""])[0]
            site_web = contact.get("foaf:homepage", [""])[0]

        description = data.get("rdfs:comment", {}).get("fr", [""])[0]

        activite = {
            "nom": nom,
            "categories": categories_trouvees,
            "saison": saison_finale,
            "adresse": adresse,
            "code_postal": cp,
            "ville": ville,
            "latitude": lat,
            "longitude": lon,
            "telephone": tel,
            "site_internet": site_web,
            "description": description
        }
        activites_propres.append(activite)

    except Exception:
        continue

print(f"💾 Sauvegarde de {len(activites_propres)} activités...")
with open(FICHIER_SORTIE, 'w', encoding='utf-8') as f_out:
    json.dump(activites_propres, f_out, ensure_ascii=False, indent=4)
print("🎉 Terminé !")