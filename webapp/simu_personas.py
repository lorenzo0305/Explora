import json
import pandas as pd
import recommendation
import remplacer_activites 
import suggestions
import random

# Définition des "Templates" de scores par défaut pour la création
TEMPLATES = {
    "aventurier": {"nature": 9.0, "sport": 9.0, "culture": 2.0, "gastronomie": 3.0, "detente": 2.0, "boutique": 1.0},
    "culturel": {"nature": 2.0, "sport": 1.0, "culture": 9.5, "gastronomie": 6.0, "detente": 4.0, "boutique": 3.0},
    "epicurien": {"nature": 3.0, "sport": 1.0, "culture": 4.0, "gastronomie": 9.5, "detente": 8.0, "boutique": 5.0}
}

# ─────────────────────────────────────────────
#  1. GESTION UTILISATEURS
# ─────────────────────────────────────────────
def charger_utilisateurs(fichier="utilisateurs_personas.json"):
    try:
        with open(fichier, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        return []

def sauvegarder_utilisateurs(data, fichier="utilisateurs_personas.json"):
    with open(fichier, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=4, ensure_ascii=False)

def creer_nouveau_profil():
    print("\n--- CRÉATION DE VOTRE PROFIL ---")
    nom = input("Votre nom : ")
    prenom = input("Votre prénom : ")
    
    print("\nChoisissez votre style de voyage :")
    for style in TEMPLATES.keys():
        print(f"- {style.capitalize()}")
    
    type_choisi = input("\nType : ").lower()
    while type_choisi not in TEMPLATES:
        type_choisi = input("Type inconnu. Réessayez : ").lower()
    
    villes_possibles = ["Lille", "Arras", "Lyon", "Grenoble", "Annecy"]
    ville = random.choice(villes_possibles) # On simule une ville de tes régions

    nouveau_user = {
        "id_user": random.randint(1000, 9999),
        "nom": nom,
        "prenom": prenom,
        "ville_residence": ville,
        "type_persona": type_choisi.capitalize(),
        "preferences_ia": TEMPLATES[type_choisi].copy()
    }
    return nouveau_user

# ─────────────────────────────────────────────
#  2. LOGIQUE DE MISE À JOUR (FEEDBACK LOOP)
# ─────────────────────────────────────────────
def mettre_a_jour_preferences(id_user, theme_ajoute, theme_supprime):
    users = charger_utilisateurs()
    for u in users:
        if u['id_user'] == id_user:
            # On booste l'intérêt pour le nouveau thème choisi
            u['preferences_ia'][theme_ajoute] = round(min(10.0, u['preferences_ia'].get(theme_ajoute, 0) + 0.5), 2)
            # On réduit l'intérêt pour le thème rejeté
            u['preferences_ia'][theme_supprime] = round(max(0.0, u['preferences_ia'].get(theme_supprime, 0) - 0.3), 2)
            break
    sauvegarder_utilisateurs(users)

# ─────────────────────────────────────────────
#  3. SIMULATION ET INTERACTION
# ─────────────────────────────────────────────
def executer_simulation():
    # Connexion Data
    print(" Connexion MongoDB et chargement des données...")
    client = recommendation.connecter_mongodb()
    df_global = recommendation.charger_donnees(client)

    users = charger_utilisateurs()

    print("\n" + "═"*55)
    print("           EXPLORA : SYSTÈME INTELLIGENT")
    print("═"*55)

    # Sélection ou Création
    print("0 : Créer un nouveau profil")
    for i, u in enumerate(users):
        print(f"{i+1} : {u['prenom']} {u['nom']} ({u['type_persona']})")

    choix_u = int(input("\n Qui êtes-vous ? (Entrez le numéro) : "))

    if choix_u == 0:
        user = creer_nouveau_profil()
        users.append(user)
        sauvegarder_utilisateurs(users)
        print(f" Profil créé avec succès !")
    else:
        user = users[choix_u - 1]

    # Génération
    print(f"\n Génération de l'itinéraire pour {user['prenom']} ({user['type_persona']})...")
    p = user['preferences_ia']
    
    recommendation.lancer_explora(
        ville=user['ville_residence'], rayon=30, jours=1, **p
    )

    # Remplacement et Apprentissage
    act_old = input("\n Activité à remplacer : ").strip()
    mode = input(" Type de remplacement (similaire / different) : ").strip().lower()

    try:
        if mode == "similaire":
            df_alternatives = suggestions.suggerer_top_10_alternatives(act_old, df_global)
        else:
            df_alternatives = remplacer_activites.suggerer_top_10_differents(act_old, df_global)

        if isinstance(df_alternatives, pd.DataFrame) and not df_alternatives.empty:
            print("\n ALTERNATIVES TROUVÉES :")
            
            # On détecte automatiquement le nom de la colonne (nom, Activité ou Activite)
            col_nom = next((c for c in ['nom', 'Activité', 'Activite'] if c in df_alternatives.columns), None)
            
            if col_nom:
                liste_noms = df_alternatives[col_nom].tolist()
                for i, nom in enumerate(liste_noms):
                    print(f"{i} : {nom}")

                choix_alt = int(input("\n Choisissez votre nouvelle activité : "))
                act_new = liste_noms[choix_alt]

            # Feedback Loop automatique
            from __main__ import extraire_theme_dominant # ou définit la fonction avant
            theme_old = extraire_theme_dominant(act_old, df_global)
            theme_new = extraire_theme_dominant(act_new, df_global)

            if theme_old and theme_new:
                print(f"\n Apprentissage IA : On augmente {theme_new} et on baisse {theme_old}.")
                mettre_a_jour_preferences(user['id_user'], theme_new, theme_old)
                print(" Profil mis à jour dans utilisateurs_personas.json pour votre prochain voyage.")

    except Exception as e:
        print(f" Erreur lors du remplacement : {e}")

    client.close()

# Fonction utilitaire nécessaire
def extraire_theme_dominant(nom_activite, df_global):
    ligne = df_global[df_global['nom'] == nom_activite]
    if ligne.empty:
        ligne = df_global[df_global['nom'].str.contains(nom_activite, case=False, na=False)]
    if not ligne.empty:
        scores = ligne.iloc[0]['scores']
        return max(scores, key=scores.get)
    return None

if __name__ == "__main__":
    executer_simulation()