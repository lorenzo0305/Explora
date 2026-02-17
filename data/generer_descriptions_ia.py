import pandas as pd
import json
import os
from anthropic import Anthropic

# Configuration
CSV_PATH = "/workspaces/Explora/data/Auvergne_Rhone_Alpes_V2.csv"
OBJECTS_DIR = "/workspaces/Explora/data/full_france_object/objects"

# Initialiser Claude (nécessite une clé API)
# client = Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY"))

def generer_description(nom, categorie, fichier_source):
    """Génère une description avec l'IA en utilisant les infos disponibles"""
    
    # Charger le fichier JSON pour récupérer d'autres infos
    chemin_complet = os.path.join(OBJECTS_DIR, fichier_source)
    try:
        with open(chemin_complet, 'r', encoding='utf-8') as f:
            data = json.load(f)
            
        # Extraire des infos supplémentaires
        types = data.get('@type', [])
        adresse = ""
        if 'isLocatedAt' in data and len(data['isLocatedAt']) > 0:
            addr_data = data['isLocatedAt'][0].get('schema:address', [])
            if addr_data and len(addr_data) > 0:
                ville = addr_data[0].get('schema:addressLocality', '')
                adresse = f" à {ville}" if ville else ""
        
        # Créer un prompt pour l'IA
        prompt = f"""Génère une description touristique courte (2-3 phrases, max 150 mots) en français pour :
        
Nom: {nom}
Catégorie: {categorie}
Type: {', '.join(types) if isinstance(types, list) else types}
Localisation: {adresse}

La description doit être attractive, informative et donner envie de visiter/réserver."""

        # Appel à l'API Claude (décommenter si clé API disponible)
        # message = client.messages.create(
        #     model="claude-3-5-sonnet-20241022",
        #     max_tokens=200,
        #     messages=[{"role": "user", "content": prompt}]
        # )
        # return message.content[0].text
        
        # Version sans API : description générique basique
        return f"{nom} - {categorie}{adresse}. Découvrez ce lieu unique lors de votre visite."
        
    except Exception as e:
        print(f"Erreur pour {fichier_source}: {e}")
        return f"{nom} - {categorie}."

# Charger le CSV
df = pd.read_csv(CSV_PATH, sep=';', encoding='utf-8-sig')

# Filtrer les lignes sans description
a_completer = df[df['description'] == 'A_GENERER_VIA_IA']

print(f"📝 {len(a_completer)} descriptions à générer")

# Générer les descriptions
for idx, row in a_completer.iterrows():
    nouvelle_desc = generer_description(
        row['nom'], 
        row['categorie'], 
        row['fichier_source']
    )
    df.at[idx, 'description'] = nouvelle_desc
    print(f"✅ {idx+1}/{len(a_completer)}: {row['nom']}")

# Sauvegarder
output_path = "/workspaces/Explora/data/Auvergne_Rhone_Alpes_V2_complete.csv"
df.to_csv(output_path, index=False, sep=';', encoding='utf-8-sig')
print(f"\n💾 Sauvegardé : {output_path}")
