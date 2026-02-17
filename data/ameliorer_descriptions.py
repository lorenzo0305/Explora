import json
import os

def extraire_description_etendue(data, nom, categorie):
    """Tente d'extraire une description depuis d'autres champs"""
    
    # 1. Chercher dans différents champs de description
    descriptions_possibles = []
    
    # hasDescription avec d'autres sous-champs
    if 'hasDescription' in data and isinstance(data['hasDescription'], list):
        for desc_obj in data['hasDescription']:
            # Essayer différents champs
            for field in ['description', 'longDescription', 'detailedDescription']:
                if field in desc_obj:
                    if isinstance(desc_obj[field], dict):
                        text = desc_obj[field].get('fr', [])
                        if text and isinstance(text, list):
                            descriptions_possibles.append(text[0])
    
    # schema:description
    if 'schema:description' in data:
        if isinstance(data['schema:description'], dict):
            text = data['schema:description'].get('fr', [])
            if text and isinstance(text, list):
                descriptions_possibles.append(text[0])
    
    # dc:description
    if 'dc:description' in data:
        if isinstance(data['dc:description'], dict):
            text = data['dc:description'].get('fr', [])
            if text and isinstance(text, list):
                descriptions_possibles.append(text[0])
    
    # Si on a trouvé quelque chose, retourner la plus longue
    if descriptions_possibles:
        return max(descriptions_possibles, key=len)
    
    # 2. Construire une description à partir des caractéristiques
    infos = []
    
    # Type d'établissement
    types = data.get('@type', [])
    if 'schema:Accommodation' in types:
        infos.append("Hébergement")
    if 'schema:Restaurant' in types:
        infos.append("Restaurant")
    if 'schema:Museum' in types:
        infos.append("Musée")
    
    # Équipements/services
    if 'hasService' in data:
        infos.append("avec services")
    
    if infos:
        return f"{nom}. {', '.join(infos)} situé en {data.get('isLocatedAt', [{}])[0].get('schema:address', [{}])[0].get('schema:addressLocality', 'région')}."
    
    # 3. Dernière option : description générique
    return f"{nom} - {categorie}. Plus d'informations sur place."

# Test sur le fichier exemple
fichier_test = "/workspaces/Explora/data/full_france_object/objects/0/00/13-00552fae-0ed8-354c-993b-7b2b88869d40.json"
with open(fichier_test, 'r', encoding='utf-8') as f:
    data = json.load(f)
    
desc = extraire_description_etendue(data, "Gîte de groupe", "Hébergement")
print("Description générée :")
print(desc)
