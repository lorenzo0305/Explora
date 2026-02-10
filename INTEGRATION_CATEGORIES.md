"""
GUIDE D'INTÉGRATION DES CATÉGORIES DANS L'APPLICATION

1. Charger les données catégorisées au démarrage de l'app
---------------------------------------------------------

# Dans main.py, après les imports
import json
from pathlib import Path

# Charger les données catégorisées
CATEGORIZED_DATA = {}
def load_categorized_data():
    data_dir = Path(__file__).parent.parent / "data"
    for region_file in ["Hauts-de-France-categorized.json", "Auvergne-Rhône-Alpes-categorized.json"]:
        file_path = data_dir / region_file
        if file_path.exists():
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                region = region_file.split('-categorized')[0]
                CATEGORIZED_DATA[region] = data
                print(f"✅ Données catégorisées chargées: {region} ({len(data)} lieux)")

# Appeler au démarrage
load_categorized_data()


2. Créer de nouvelles routes API
---------------------------------

@app.get("/api/categories")
async def get_categories():
    """Retourne la liste de toutes les catégories avec leurs compteurs"""
    categories = {}
    for region, places in CATEGORIZED_DATA.items():
        for place in places:
            slug = place['category']['slug']
            if slug not in categories:
                categories[slug] = {
                    'label': place['category']['label'],
                    'count': 0
                }
            categories[slug]['count'] += 1
    return {"categories": categories}


@app.get("/api/places/by-category/{category_slug}")
async def get_places_by_category(
    category_slug: str,
    region: Optional[str] = None,
    limit: int = 50
):
    """Retourne les lieux d'une catégorie donnée"""
    results = []
    for region_name, places in CATEGORIZED_DATA.items():
        if region and region != region_name:
            continue
        for place in places:
            if place['category']['slug'] == category_slug:
                results.append(place)
                if len(results) >= limit:
                    break
    return {"places": results, "count": len(results)}


@app.get("/api/regions/{region}/categories")
async def get_region_categories(region: str):
    """Statistiques des catégories pour une région"""
    if region not in CATEGORIZED_DATA:
        raise HTTPException(status_code=404, detail="Région non trouvée")
    
    categories = {}
    for place in CATEGORIZED_DATA[region]:
        slug = place['category']['slug']
        if slug not in categories:
            categories[slug] = {
                'label': place['category']['label'],
                'count': 0,
                'places': []
            }
        categories[slug]['count'] += 1
        categories[slug]['places'].append({
            'label': place['label'],
            'identifier': place['identifier']
        })
    
    return {"region": region, "categories": categories}


3. Créer une nouvelle page template
------------------------------------

# templates/ExplorationCategories.html (similaire à Exploration.html)

<!DOCTYPE html>
<html lang="fr">
<head>
    <meta charset="UTF-8">
    <title>Explorer par catégories</title>
    <link rel="stylesheet" href="/static/styles/app.css">
</head>
<body>
    <div class="categories-page">
        <h1>🗺️ Explorer par catégories</h1>
        
        <!-- Filtres de catégories -->
        <div class="category-filters" id="categoryFilters"></div>
        
        <!-- Résultats -->
        <div class="places-grid" id="placesGrid"></div>
    </div>
    
    <script>
        // Charger les catégories
        fetch('/api/categories')
            .then(r => r.json())
            .then(data => {
                renderCategoryFilters(data.categories);
            });
        
        // Filtrer par catégorie
        function filterByCategory(slug) {
            fetch(`/api/places/by-category/${slug}`)
                .then(r => r.json())
                .then(data => {
                    renderPlaces(data.places);
                });
        }
    </script>
</body>
</html>


4. Ajouter une route pour la page
----------------------------------

@app.get("/exploration/categories", response_class=HTMLResponse)
async def exploration_categories(request: Request):
    return templates.TemplateResponse("ExplorationCategories.html", {
        "request": request
    })


5. Intégrer dans la navigation
-------------------------------

Dans vos templates, ajouter un lien :
<a href="/exploration/categories">📍 Explorer par catégories</a>


6. Pour Auvergne-Rhône-Alpes
-----------------------------

# Modifier categorizer.py, ligne finale :

if __name__ == "__main__":
    BASE_DIR = Path(__file__).parent.parent
    DATA_DIR = BASE_DIR / "data"
    OBJECTS_DIR = DATA_DIR / "full_france_object" / "objects"
    
    # Traiter Auvergne-Rhône-Alpes
    print("🚀 Début de la catégorisation pour Auvergne-Rhône-Alpes\\n")
    
    region_file = DATA_DIR / "Auvergne-Rhône-Alpes.json"
    output_file = DATA_DIR / "Auvergne-Rhône-Alpes-categorized.json"
    
    categorize_region_data(
        str(region_file),
        str(output_file),
        str(OBJECTS_DIR)
    )
    
    print("\\n✨ Terminé !")

# Puis exécuter :
# python3 webapp/categorizer.py


AVANTAGES DE CETTE APPROCHE
============================

✅ Filtrage rapide par catégorie
✅ Meilleure UX pour découvrir les lieux
✅ Facile à intégrer dans l'app existante
✅ Données pré-calculées (pas de calcul en temps réel)
✅ Compatible avec vos fonctionnalités actuelles (panier, voyages)
✅ API RESTful pour utilisation ailleurs
"""
