import json

def mapper_categorie(categorie_originale):
    """Mapper les catégories originales vers les 5 catégories principales"""
    
    if not categorie_originale or categorie_originale == 'Sans catégorie':
        return 'Détente'
    
    cat = categorie_originale.lower()
    
    # Gastronomie
    if any(word in cat for word in ['restauration', 'dégustation', 'boulangerie', 'pâtisserie', 
                                      'fromagerie', 'cave', 'vignoble', 'producteur']):
        return 'Gastronomie'
    
    # Nature
    if any(word in cat for word in ['naturel', 'nature', 'parc', 'jardin', 'site naturel', 
                                      'rivière', 'fleuve', 'lac', 'cascade', 'montagne',
                                      'forêt', 'réserve']):
        return 'Nature'
    
    # Sport
    if any(word in cat for word in ['sportif', 'sport', 'récréatif', 'loisirs', 'itinéraire',
                                      'pédestre', 'cyclable', 'randonnée', 'vtt', 'ski',
                                      'escalade', 'piscine', 'stade', 'golf', 'tennis',
                                      'vélo', 'routier', 'trail']):
        return 'Sport'
    
    # Culture
    if any(word in cat for word in ['culturel', 'culture', 'musée', 'patrimoine', 'église',
                                      'chapelle', 'château', 'monument', 'historique',
                                      'abbaye', 'cathédrale', 'théâtre', 'galerie',
                                      'exposition', 'religieux']):
        return 'Culture'
    
    # Détente (par défaut pour hébergement et autres)
    if any(word in cat for word in ['hébergement', 'hôtel', 'gîte', 'camping', 'chambre',
                                      'location', 'spa', 'bien-être', 'détente', 'sauna',
                                      'hammam', 'thermal']):
        return 'Détente'
    
    # Services et commerce → Détente par défaut
    if any(word in cat for word in ['commerce', 'boutique', 'shopping', 'service',
                                      'office', 'pratique', 'transport']):
        return 'Détente'
    
    # Par défaut
    return 'Détente'


# Charger le fichier JSON
with open('Auvergne.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

print(f"📊 Recatégorisation de {len(data)} activités...\n")

# Statistiques avant
old_categories = {}
for activite in data:
    cat = activite['categorie'] or 'Sans catégorie'
    old_categories[cat] = old_categories.get(cat, 0) + 1

print(f"Avant : {len(old_categories)} catégories différentes")

# Recatégoriser
for activite in data:
    activite['categorieOriginale'] = activite['categorie']
    activite['categorie'] = mapper_categorie(activite['categorie'])

# Statistiques après
new_categories = {}
for activite in data:
    cat = activite['categorie']
    new_categories[cat] = new_categories.get(cat, 0) + 1

print(f"Après : {len(new_categories)} catégories principales\n")

print("📈 Répartition des nouvelles catégories:\n")
for cat, count in sorted(new_categories.items(), key=lambda x: x[1], reverse=True):
    pct = (count * 100) / len(data)
    print(f"  {cat:.<20} {count:>6} ({pct:>5.2f}%)")

# Sauvegarder
with open('Auvergne.json', 'w', encoding='utf-8') as f:
    json.dump(data, f, ensure_ascii=False, indent=2)

print(f"\n✅ Fichier mis à jour : Auvergne.json")
print(f"\n💡 Note : La catégorie originale est conservée dans 'categorieOriginale'")
