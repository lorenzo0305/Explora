#!/usr/bin/env python3
"""
Script pour recatégoriser les activités basé sur des mots-clés dans le nom
Catégories: Gastronomie, Culture, Sport, Nature, Shopping, Détente
"""

import json
import re

FICHIER_JSON = "/workspaces/Explora/data2/Haut_de_france.json"

# Mots-clés par catégorie (ordre important: du plus spécifique au plus général)
KEYWORDS_BY_CATEGORY = {
    "Gastronomie": [
        r"\b(restaurant|café|bar|brasserie|bistro|bistrot|auberge|crêperie|pizzeria|traiteur)\b",
        r"\b(boulangerie|pâtisserie|salon de thé|snack|buffet|kebab|burger|tacos?|sandwicherie)\b",
        r"\b(friterie|grillade|cantine|cafétéria|fast[- ]?food|food truck|rôtisserie)\b",
        r"\b(cuisine|culinaire|gastronomique|dégustation|menu|repas|dîner|déjeuner)\b",
        r"\b(wine|vins?|vignoble|cave|domaine viticole|brasserie artisanale)\b",
    ],
    
    "Culture": [
        r"\b(musée|museum|exposition|galerie d'art|galerie)\b",
        r"\b(château|fort|citadelle|abbaye|monastère|prieuré|cathédrale|église|chapelle|basilique)\b",
        r"\b(théâtre|cinéma|salle de spectacle|opéra|scène|auditorium)\b",
        r"\b(bibliothèque|médiathèque|archives|centre culturel)\b",
        r"\b(monument|patrimoine|historique|mémoriel|mémorial|site historique)\b",
        r"\b(concert|festival|spectacle|show|représentation|cabaret|cirque)\b",
        r"\b(artiste|artistique|culturel|culturelle|expo|vernissage)\b",
        r"\b(cinéma|film|projection|séance)\b",
    ],
    
    "Sport": [
        r"\b(piscine|aquatique|natation|baignade)\b",
        r"\b(stade|terrain de sport|complexe sportif|gymnase|dojo|tatami)\b",
        r"\b(golf|tennis|squash|badminton|ping[- ]?pong|bowling)\b",
        r"\b(fitness|gym|musculation|crossfit|yoga|pilates|zumba)\b",
        r"\b(football|rugby|basket|handball|volley|baseball)\b",
        r"\b(vélo|vtt|cyclisme|cyclo|bicyclette|bike)\b",
        r"\b(escalade|accrobranche|parcours aventure|via ferrata)\b",
        r"\b(karting|quad|paintball|laser[- ]?game|escape[- ]?game)\b",
        r"\b(équitation|centre équestre|poney club|manège)\b",
        r"\b(ski|patinoire|glace|luge|snowboard)\b",
        r"\b(sports?|sportif|sportive|activité sportive)\b",
    ],
    
    "Nature": [
        r"\b(parc naturel|réserve naturelle|espace naturel|site naturel)\b",
        r"\b(jardin|parc|arboretum|jardin botanique|roseraie)\b",
        r"\b(forêt|bois|bosquet|clairière)\b",
        r"\b(lac|étang|marais|plan d'eau|rivière|fleuve|ruisseau|cascade|source)\b",
        r"\b(montagne|col|sommet|vallée|gorge|canyon|grotte)\b",
        r"\b(plage|littoral|côte|mer|océan|dune)\b",
        r"\b(sentier|randonnée|promenade|balade|chemin|parcours|trek|gr\d+)\b",
        r"\b(faune|flore|observation|ornithologie|biodiversité)\b",
        r"\b(nature|naturel|naturelle|écologique|écosystème)\b",
    ],
    
    "Shopping": [
        r"\b(boutique|magasin|shop|store|commerce)\b",
        r"\b(marché|marché couvert|halle|galerie marchande)\b",
        r"\b(centre commercial|shopping center|mall)\b",
        r"\b(artisan|artisanat|créateur|atelier[- ]boutique)\b",
        r"\b(vente|shopping|achat|boutique)\b",
        r"\b(librairie|papeterie|mercerie|quincaillerie)\b",
        r"\b(bijouterie|joaillerie|horlogerie|parfumerie)\b",
        r"\b(fleuriste|jardinerie|pépinière)\b",
        r"\b(fromagerie|épicerie|supermarché|superette)\b",
        r"\b(empreinte|concept store|enseigne)\b",
    ],
    
    "Hébergement": [
        r"\b(hôtel|hotel|motel|auberge de jeunesse|hostel)\b",
        r"\b(chambre d'hôte|maison d'hôte|bed and breakfast|b&b|bnb)\b",
        r"\b(gîte|gite|location de vacances|meublé de tourisme)\b",
        r"\b(camping|campground|caravaning|mobil[- ]?home|aire de camping)\b",
        r"\b(résidence de tourisme|appart[- ]?hôtel|apparthotel|résidence)\b",
        r"\b(lodge|cottage|chalet|bungalow|villa)\b",
        r"\b(hébergement|logement|nuitée|séjour)\b",
        r"\b(pension|guest house|village vacances|club de vacances)\b",
    ],
    
    # Détente en dernier (par défaut)
    "Détente": [
        r"\b(spa|bien[- ]?être|massage|relaxation|détente|zen)\b",
        r"\b(sauna|hammam|jacuzzi|balnéo|thalasso|thermes?)\b",
        r"\b(parc d'attractions|parc de loisirs|ludique|jeux)\b",
        r"\b(salon|centre de|espace|aire de|base de loisirs)\b",
    ],
}

def categorize_by_keywords(nom, current_category=""):
    """Catégorise une activité basée sur les mots-clés dans son nom"""
    nom_lower = nom.lower()
    
    # Parcourir les catégories dans l'ordre de priorité
    for category, patterns in KEYWORDS_BY_CATEGORY.items():
        for pattern in patterns:
            if re.search(pattern, nom_lower, re.IGNORECASE):
                return category
    
    # Si aucun mot-clé trouvé, garder la catégorie actuelle ou mettre Détente
    return current_category if current_category else "Détente"

def main():
    print("📥 Chargement de Haut_de_france.json...")
    with open(FICHIER_JSON, 'r', encoding='utf-8') as f:
        activites = json.load(f)
    
    print(f"✓ {len(activites)} activités chargées")
    
    # Statistiques avant
    from collections import Counter
    categories_avant = Counter([a.get('categorie', '') for a in activites])
    
    print("\n📊 Distribution AVANT:")
    for cat, count in sorted(categories_avant.items(), key=lambda x: x[1], reverse=True):
        print(f"   - {cat}: {count}")
    
    # Recatégoriser
    print("\n🔄 Recatégorisation en cours...")
    changes = 0
    
    for activite in activites:
        old_cat = activite.get('categorie', '')
        new_cat = categorize_by_keywords(activite.get('nom', ''), old_cat)
        
        if new_cat != old_cat:
            activite['categorie'] = new_cat
            changes += 1
    
    print(f"✓ {changes} activités recatégorisées")
    
    # Statistiques après
    categories_apres = Counter([a.get('categorie', '') for a in activites])
    
    print("\n📊 Distribution APRÈS:")
    for cat, count in sorted(categories_apres.items(), key=lambda x: x[1], reverse=True):
        pct = (count / len(activites)) * 100
        print(f"   - {cat}: {count} ({pct:.1f}%)")
    
    # Sauvegarder
    print("\n💾 Sauvegarde...")
    with open(FICHIER_JSON, 'w', encoding='utf-8') as f:
        json.dump(activites, f, ensure_ascii=False, indent=2)
    
    print("✅ Terminé!")
    
    # Exemples de changements
    print("\n📋 Exemples de recatégorisations:")
    with open(FICHIER_JSON, 'r', encoding='utf-8') as f:
        activites = json.load(f)
    
    count = 0
    for a in activites[:100]:  # Vérifier les 100 premiers
        if "empreinte" in a.get('nom', '').lower() or "végétal" in a.get('nom', '').lower():
            print(f"   • {a['nom']} → {a['categorie']}")
            count += 1
            if count >= 5:
                break

if __name__ == "__main__":
    main()
