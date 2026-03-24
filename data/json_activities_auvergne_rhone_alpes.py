import os
import json
import glob

# --- CONFIGURATION DES DOSSIERS ---
DOSSIER_SOURCE = r"C:\Users\roman\ESIEE\explora\Explora\data\Auvergne_Rhone_Alpes_object"
FICHIER_SORTIE = r"C:\Users\roman\ESIEE\explora\Explora\data\Auvergne_Rhone_Alpes_multicategories.json"

activites_propres = []

# --- LE DICTIONNAIRE OFFICIEL DATATOURISME ---
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
    "Cinematheque": "culture", "CircusPlace": "culture", "Citadel": "culture", 
    "CityHeritage": "culture", "Cloister": "culture", "Collegiate": "culture", 
    "Commanderie": "culture", "Commemoration": "culture", "Concert": "culture", 
    "Convent": "culture", "CulturalEvent": "culture", "CulturalSite": "culture", "Culture": "culture", 
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

print("🔍 Recherche des fichiers JSON...")
fichiers_json = glob.glob(os.path.join(DOSSIER_SOURCE, "**", "*.json"), recursive=True)
print(f"✅ {len(fichiers_json)} fichiers trouvés ! Début de l'extraction...\n")

for chemin_fichier in fichiers_json:
    try:
        with open(chemin_fichier, 'r', encoding='utf-8') as f:
            data = json.load(f)

        nom = data.get("rdfs:label", {}).get("fr", [""])[0]

        description = ""
        comments = data.get("rdfs:comment", {})
        if "fr" in comments:
            description = comments.get("fr", [""])[0]
        else:
            has_desc = data.get("hasDescription", [])
            if has_desc and isinstance(has_desc, list):
                short = has_desc[0].get("shortDescription", {})
                if "fr" in short:
                    description = short.get("fr", [""])[0]

        # --- NOUVEAU SYSTÈME DE CATÉGORISATION (Par Ontologie) ---
        categories_trouvees = []
        tags_officiels = data.get("@type", [])
        
        for tag in tags_officiels:
            if tag in DICTIONNAIRE_CATEGORIES:
                categories_trouvees.append(DICTIONNAIRE_CATEGORIES[tag])
                
        # On supprime les doublons (ex: si ça a matché deux fois "sport")
        categories_trouvees = list(set(categories_trouvees))

        # Le filet de sécurité
        if len(categories_trouvees) == 0:
            categories_trouvees.append("autre")

        # --- LOCALISATION ---
        ville, cp, adresse, region, lat, lon = "", "", "", "", "", ""
        is_located = data.get("isLocatedAt", [])
        
        if is_located and isinstance(is_located, list):
            loc = is_located[0]
            
            adresse_info = loc.get("schema:address", [])
            if adresse_info and isinstance(adresse_info, list):
                addr = adresse_info[0]
                ville = addr.get("schema:addressLocality", "")
                cp = addr.get("schema:postalCode", "")
                
                street = addr.get("schema:streetAddress", [])
                if street: adresse = street[0]

                try:
                    region = addr.get("hasAddressCity", {}).get("isPartOfDepartment", {}).get("isPartOfRegion", {}).get("rdfs:label", {}).get("fr", [""])[0]
                except:
                    pass

            geo = loc.get("schema:geo", {})
            if geo:
                lat = geo.get("schema:latitude", "")
                lon = geo.get("schema:longitude", "")

        # --- CONTACTS ---
        tel, site_web = "", ""
        contacts = data.get("hasContact", [])
        if contacts and isinstance(contacts, list):
            contact = contacts[0]
            tels = contact.get("schema:telephone", [])
            if tels: tel = tels[0]

            webs = contact.get("foaf:homepage", [])
            if webs: site_web = webs[0]

        activite = {
            "nom": nom,
            "categories": categories_trouvees,  # ICI AU PLURIEL
            "adresse": adresse,
            "code_postal": cp,
            "ville": ville,
            "region": region,
            "latitude": lat,
            "longitude": lon,
            "telephone": tel,
            "site_internet": site_web,
            "description": description
        }

        activites_propres.append(activite)

    except Exception as e:
        continue

print("💾 Sauvegarde en cours...")
with open(FICHIER_SORTIE, 'w', encoding='utf-8') as f_out:
    json.dump(activites_propres, f_out, ensure_ascii=False, indent=4)

print("-" * 40)
print(f"🎉 SUCCÈS ! {len(activites_propres)} activités ont été catégorisées avec le dictionnaire officiel.")