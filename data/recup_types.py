import os
import json
import glob

# --- CONFIGURATION ---
# Mets le chemin vers ton dossier qui contient les 36 000 petits JSON bruts
DOSSIER_SOURCE = r"C:\Users\roman\ESIEE\explora\Explora\data\Auvergne_Rhone_Alpes_object"
FICHIER_SORTIE = r"C:\Users\roman\ESIEE\explora\Explora\data\liste_types_datatourisme.txt"

types_uniques = set() # Le "set" empêche automatiquement les doublons !

print("🔍 Recherche des fichiers et extraction des types...")
fichiers_json = glob.glob(os.path.join(DOSSIER_SOURCE, "**", "*.json"), recursive=True)

print(f"📂 {len(fichiers_json)} fichiers trouvés. Lecture en cours (ça peut prendre 1 ou 2 min)...")

for chemin_fichier in fichiers_json:
    try:
        with open(chemin_fichier, 'r', encoding='utf-8') as f:
            data = json.load(f)
            
            # On récupère la liste des types
            types_du_fichier = data.get("@type", [])
            
            # On ajoute chaque type dans notre "set"
            for t in types_du_fichier:
                types_uniques.add(t)
    except Exception as e:
        continue

# --- TRI ALPHABÉTIQUE ET SAUVEGARDE ---
print("🔤 Tri alphabétique des types...")
types_tries = sorted(list(types_uniques))

print("💾 Sauvegarde dans le fichier texte...")
with open(FICHIER_SORTIE, 'w', encoding='utf-8') as f_out:
    for t in types_tries:
        f_out.write(f"{t}\n")

print("-" * 40)
print(f"🎉 SUCCÈS ! {len(types_tries)} types uniques officiels ont été trouvés et rangés dans {FICHIER_SORTIE}.")