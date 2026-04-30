# Plan de soutenance finale — Explora Smart Reco

**Durée cible : 22 minutes** (la consigne demande 20–25 min hors questions)
**6 oratrices et orateurs** : Léonore, Keren, Tristan, Sarah, Romane, Andréa
**Objectif** : montrer la qualité du travail, l'implication, l'organisation, la communication, et conclure par un bilan critique technique **et** humain.

---

## Vue d'ensemble (chronologie)

| # | Partie                                       | Durée    | Qui parle                | Idée-clé                                                             |
|---|----------------------------------------------|----------|--------------------------|----------------------------------------------------------------------|
| 1 | Accroche & présentation d'Explora            | 1 min 30 | Léonore                  | « Le Spotify du voyage » : poser la promesse                         |
| 2 | Problématique & objectifs                    | 1 min 30 | Andréa                   | Trop d'info, trop de temps : 4 objectifs concrets                    |
| 3 | Organisation, méthodo et outils de comm.     | 2 min    | Tristan                  | 3 binômes, chef de projet, GitHub, Discord, réunions hebdo           |
| 4 | Pôle Data                                    | 3 min 30 | Romane → Keren           | Du flux DATAtourisme à MongoDB Atlas (schéma étape par étape)        |
| 5 | Pôle Algorithme                              | 4 min    | Sarah → Léonore          | SBERT + vecteur 5D + cosine + KNN                                    |
| 6 | Pôle Frontend / Backend                      | 3 min    | Tristan → Andréa         | Architecture HTML/CSS/JS séparée, jauges, route API, Leaflet         |
| 7 | Démo live                                    | 2 min    | Andréa pilote, Léonore commente | Un parcours utilisateur de bout en bout                       |
| 8 | Bilan critique : succès & difficultés        | 2 min 30 | Keren (technique) + Sarah (humain) | Ce qui a marché, ce qui a coincé, ce qu'on en retient      |
| 9 | Perspectives & conclusion                    | 2 min    | Romane → Léonore         | Saisonnalité, Collaborative Filtering, autres régions, ouverture     |

**Total : ≈ 22 min.** Prévoir un chrono visible et marquer les transitions à l'oral pour fluidifier.

---

## Détails partie par partie

### 1. Accroche & présentation d'Explora — *Léonore (1 min 30)*

- Accroche forte : une petite anecdote ou un chiffre ("on passe en moyenne X heures à organiser un week-end").
- Définir Explora en une phrase : **« Le Spotify du voyage »**, un moteur de recommandation qui transforme des milliers de données touristiques en itinéraires personnalisés.
- Annoncer le plan en 3 temps : ce qu'on a construit (Data → Algo → Site), une démo, puis bilan + perspectives.

### 2. Problématique & objectifs — *Andréa (1 min 30)*

- Problématique : trop d'informations, peu de personnalisation, mise en avant systématique des mêmes lieux saturés.
- 4 objectifs : base fiable, scoring sur mesure, classification multi-classe + géolocalisation, interface fluide.
- Public cible et valeur ajoutée pour Explora (différenciation, rétention, scalabilité).

### 3. Organisation et outils de communication — *Tristan (2 min)*

- 6 personnes en **3 binômes** : Data (Keren & Romane), Algo (Léonore & Sarah), Fullstack (Tristan & Andréa).
- **Chef·fe de projet** : annoncer clairement qui (à choisir dans l'équipe) et son rôle (planning, points hebdo, interface avec les commanditaires Lucie & Lorenzo).
- Méthodologie : sprints courts, points d'avancement hebdomadaires.
- **Outils de communication** à montrer : dépôt **GitHub** (commits/branches), **Discord** ou Teams, suivi de tâches (Trello / Notion / GitHub Projects), comptes-rendus de réunion.
- Insister sur l'autonomie de chaque binôme + les points de synchronisation (interfaces entre Data → Algo → Front).

### 4. Pôle Data — *Romane puis Keren (3 min 30)*

- **Romane** : pourquoi DATAtourisme (flux national, mise à jour continue) ; périmètre Auvergne-Rhône-Alpes + Hauts-de-France ; abandon des CSV séparés au profit d'un script unique.
- **Keren** : montrer le schéma "étape par étape" du script (lecture → objets → @type → mapping multi-classe → extraction → nettoyage → JSON épuré → MongoDB Atlas) ; expliquer le passage des regex/mots-clés au mapping `@type` (~300 types → 6 catégories) qui a gommé les faux positifs.
- Conclure sur **MongoDB Atlas + automatisation** (pipeline rejouable) et la valeur ajoutée pour les autres pôles : tout le monde tape dans la même base à jour.

> Support visuel : le schéma "Pipeline Data" et le schéma "Script étape par étape" déjà produits.

### 5. Pôle Algorithme — *Sarah puis Léonore (4 min)*

- **Sarah** : étape 1 — construire un **vecteur à 5 dimensions** (Nature, Culture, Gastronomie, Sport, Détente) pour chaque activité. Pourquoi on a abandonné les regex naïfs : trop d'erreurs. Choix de **SBERT** (recherche sémantique) pour des scores plus fins.
- **Léonore** : étape 2 — moteur de recherche par **jauges + rayon** : calcul géographique (haversine) + calcul de **match par cosine similarity** entre vecteur utilisateur et vecteurs activités.
- **Léonore** : étape 3 — recommandations "tu as aimé X, tu vas aimer Y" via **KNN** (plus proches voisins) sur la matrice 5D.
- Insister sur la cohérence avec le brief : on coche cosine similarity, vector search, embeddings, Python (sklearn / SBERT).

> Support visuel : un schéma simple "vecteur utilisateur ↔ vecteur activité" + flèche cosine.

### 6. Pôle Frontend / Backend — *Tristan puis Andréa (3 min)*

- **Tristan** : refonte de l'architecture front (séparation HTML / CSS / JS) — analogie corps humain (squelette / physique / système nerveux) ; page de création de voyage avec **jauges interactives** (pas un sélecteur de chiffres).
- **Andréa** : formulaire JS qui envoie les critères au backend via une **route** (analogie de l'adresse postale) ; backend Python qui exécute l'algo de Léonore & Sarah ; carte **Leaflet** pour la sélection des villes et l'affichage de l'itinéraire ; sauvegarde du carnet de voyage en MongoDB.

> Support visuel : capture d'écran de la page jauges + carte Leaflet + bout de schéma front ↔ back.

### 7. Démo live — *Andréa pilote, Léonore commente (2 min)*

- Saisir un profil (jauges + ville + rayon).
- Montrer la liste de résultats personnalisés et la carte.
- Montrer la fonction "tu as aimé cette activité → voici des activités similaires" (KNN).
- Comme on ne prévoit **pas** de vidéo de secours, prévoir un **parcours de démo répété** plusieurs fois en amont avec les mêmes données d'entrée (ville + jauges) pour limiter les surprises, et garder à portée 1 ou 2 captures d'écran clés à commenter si la démo bloque.

### 8. Bilan critique — *Keren (technique) + Sarah (humain) (2 min 30)*

**Succès techniques (à valoriser) :**
- pipeline Data automatisé et reproductible, multi-régions ;
- moteur de scoring fonctionnel avec SBERT + cosine + KNN ;
- interface complète avec carte interactive et carnets de voyage ;
- chaîne de bout en bout qui tourne (Data → Algo → Front).

**Difficultés techniques rencontrées :**
- structure complexe du flux DATAtourisme (centaines de champs inutiles) ;
- échec de la première classification par regex → bascule vers `@type` côté Data, puis vers SBERT côté Algo ;
- absence d'images dans une partie des fiches ;
- bug de fin de sprint qui a "tout cassé" et qu'il a fallu déboguer dans l'urgence ;
- pas de tests utilisateurs externes ni de mesures formelles (précision / pertinence / taux de clic).

**Bilan humain (Sarah) :**
- montée en compétences : Python data, SBERT, MongoDB, JS / Leaflet, gestion de projet ;
- coordination entre 3 binômes (synchronisation des formats, des catégories, des routes) ;
- ce qu'on referait : poser plus tôt les "contrats" entre pôles (schéma de données, noms de catégories, format des coordonnées) ;
- ce qu'on garderait : binômes spécialisés + points hebdo courts.

### 9. Perspectives & conclusion — *Romane puis Léonore (2 min)*

- **Data (Romane)** : étendre à toutes les régions, ajouter la **saisonnalité**, intégrer des images, automatiser les MAJ via l'API DATAtourisme.
- **Algo (Léonore)** : passer au **Collaborative Filtering** avec personas (« aventurier », « gourmet »…) et utilisateurs simulés (Faker), affiner les scores avec les descriptions enrichies.
- **Produit (Léonore)** : version mobile, comptes utilisateurs, partage de carnets, ouverture aux beta-testeurs.
- **Phrase de fin (à préparer)** : revenir sur la promesse de départ, "le Spotify du voyage", et conclure sur la fierté d'avoir livré un prototype convaincant prêt à être ouvert à de vrais utilisateurs.

---

## Conseils transverses pour la soutenance

- **Équilibre du temps de parole** : viser ~3 à 4 min de prise de parole par personne. Aucun binôme ne dépasse 5 min sans transition.
- **Transitions explicites** : « Maintenant que la base est propre, on peut la donner à l'algo… » → fluidifie et montre que vous maîtrisez l'enchaînement.
- **Supports** : alterner slides synthétiques (1 idée par slide), schémas faits maison, captures d'écran, et la démo. Pas de mur de texte.
- **Anticiper les questions** : volumétrie de la base, choix de SBERT vs autres modèles, sécurité, RGPD, scalabilité, coût d'hébergement.
- **Répétitions** : 2 répétitions chronométrées en conditions réelles (avec slides + démo) avant le jour J.
- **Plan B démo (sans vidéo)** : répéter la démo avec un jeu d'entrée fixe et tester le serveur juste avant de passer ; garder à portée quelques captures d'écran clés à commenter si quelque chose bloque en direct.
- **Conclusion forte** : la dernière phrase doit donner envie au jury ; soigner le mot de fin.

---

*Ce plan reprend tout ce qu'on a livré (Data, Algo, Front/Back) et coche l'ensemble des points évalués : qualité du travail, implication, organisation, outils de communication, supports, contenu technique, capacité de conviction et bilan critique.*
