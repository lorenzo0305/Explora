import json
import os
import pandas as pd

CSV_PATH = "/workspaces/Explora/data/Auvergne_Rhone_Alpes_V2.csv"
OBJECTS_DIR = "/workspaces/Explora/data/full_france_object/objects"

def generer_description_auto(nom, categorie, fichier_source):
    """Génère une description automatique basée sur les infos disponibles"""
    
    chemin_complet = os.path.join(OBJECTS_DIR, fichier_source)
    try:
        with open(chemin_complet, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Extraire les informations disponibles
        types = data.get('@type', [])
        
        # Localisation
        ville = ""
        code_postal = ""
        if 'isLocatedAt' in data and len(data['isLocatedAt']) > 0:
            addr_data = data['isLocatedAt'][0].get('schema:address', [])
            if addr_data and len(addr_data) > 0:
                ville = addr_data[0].get('schema:addressLocality', '')
                code_postal = addr_data[0].get('schema:postalCode', '')
        
        # Contact
        telephone = ""
        email = ""
        site_web = ""
        if 'hasContact' in data and len(data['hasContact']) > 0:
            contact = data['hasContact'][0]
            if 'schema:telephone' in contact:
                telephone = contact['schema:telephone'][0] if isinstance(contact['schema:telephone'], list) else contact['schema:telephone']
            if 'schema:email' in contact:
                email = contact['schema:email'][0] if isinstance(contact['schema:email'], list) else contact['schema:email']
            if 'foaf:homepage' in contact:
                site_web = contact['foaf:homepage'][0] if isinstance(contact['foaf:homepage'], list) else contact['foaf:homepage']
        
        # Construire la description selon le type
        description_parts = []
        
        # Partie 1 : Introduction
        if 'schema:Accommodation' in types or 'Accommodation' in types:
            description_parts.append(f"{nom} vous accueille pour votre séjour")
        elif 'schema:Restaurant' in types or 'FoodEstablishment' in types:
            description_parts.append(f"{nom} vous propose une expérience gastronomique")
        elif 'schema:Museum' in types or 'CulturalSite' in types:
            description_parts.append(f"Découvrez {nom}")
        elif 'schema:Store' in types or 'Store' in types:
            description_parts.append(f"{nom}, votre boutique")
        elif 'SportsAndLeisurePlace' in types:
            description_parts.append(f"{nom} vous invite à pratiquer vos activités")
        else:
            description_parts.append(f"Visitez {nom}")
        
        # Partie 2 : Localisation
        if ville:
            if code_postal:
                description_parts.append(f"à {ville} ({code_postal})")
            else:
                description_parts.append(f"à {ville}")
        
        # Partie 3 : Catégorie/Type
        if categorie and categorie != "nan":
            description_parts.append(f"- {categorie}")
        
        # Partie 4 : Détails du type d'établissement
        type_details = []
        if 'GroupLodging' in types:
            type_details.append("hébergement de groupe")
        if 'Hotel' in types:
            type_details.append("hôtel")
        if 'Camping' in types:
            type_details.append("camping")
        if 'Restaurant' in types:
            type_details.append("restaurant")
        if 'Museum' in types:
            type_details.append("musée")
        if 'NaturalHeritage' in types:
            type_details.append("patrimoine naturel")
        if 'CulturalSite' in types:
            type_details.append("site culturel")
            
        if type_details:
            description_parts.append(f". {', '.join(type_details).capitalize()}")
        
        # Assembler la description
        description = " ".join(description_parts) + "."
        
        # Ajouter info de contact si disponible
        contact_info = []
        if telephone:
            contact_info.append(f"Tél: {telephone}")
        if site_web and len(description) < 100:
            contact_info.append("Plus d'infos en ligne")
        
        if contact_info and len(description) < 120:
            description += " " + " - ".join(contact_info) + "."
        
        return description
        
    except Exception as e:
        # En cas d'erreur, description minimale
        return f"{nom}. {categorie}."

# Charger le CSV
df = pd.read_csv(CSV_PATH, sep=';', encoding='utf-8-sig')

# Trouver quelques exemples sans description
exemples = df[df['description'] == 'A_GENERER_VIA_IA'].head(10)

print("=" * 80)
print("EXEMPLES DE DESCRIPTIONS AUTO-GÉNÉRÉES")
print("=" * 80)
print()

for idx, row in exemples.iterrows():
    desc_auto = generer_description_auto(
        row['nom'],
        row['categorie'],
        row['fichier_source']
    )
    
    print(f"📍 {row['nom']}")
    print(f"   Catégorie: {row['categorie']}")
    print(f"   📝 Description auto: {desc_auto}")
    print()

print("=" * 80)
print(f"Total à générer: {len(df[df['description'] == 'A_GENERER_VIA_IA'])} descriptions")
print("=" * 80)
