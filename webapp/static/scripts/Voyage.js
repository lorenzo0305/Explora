document.addEventListener('DOMContentLoaded', function() {
    const data = sessionStorage.getItem('algorithmRes');
    
    if (!data) {
        console.error("Aucune donnée trouvée dans le stockage session.");
        window.location.href = '/Creation.html';
        return; 
    }
    
    const parsedData = JSON.parse(data);
    const dataVoyage = Array.isArray(parsedData) ? parsedData : parsedData.data; 

    const conteneur = document.getElementById('conteneur-itineraire');
    let htmlGenere = "";

    // Fonction utilitaire pour créer le bouton et les détails (évite de répéter le code)
    function genererBlocActivite(activite) {
        return `
            <div class="activite-container" style="margin-left: 20px; margin-bottom: 10px;">
                <button class="btn-activite" style="width: 100%; text-align: left; padding: 10px; font-size: 16px; font-weight: bold; cursor: pointer; background-color: #f4f4f9; border: 1px solid #ddd; border-radius: 5px; transition: background-color 0.2s;">
                    ▶ ${activite.nom}
                </button>
                
                <div class="details-activite" style="display: none; padding: 15px; border: 1px solid #eee; border-top: none; background-color: #fafafa; border-radius: 0 0 5px 5px;">
                    <p style="margin-top: 0;"><strong>📍 Lieu :</strong> ${activite.ville} (${activite.code_postal})</p>
                    <p><em>${activite.categories.join(', ')}</em></p>
                    <p style="margin-bottom: 0;">${activite.description || 'Aucune description disponible.'}</p>
                </div>
            </div>
        `;
    }

    // 3. On boucle sur chaque jour
    dataVoyage.forEach(jourData => {
        
        htmlGenere += `
            <div class="jour-carte" style="margin-bottom: 30px; border: 1px solid #ccc; padding: 15px; border-radius: 8px;">
                <h2>Jour ${jourData.jour}</h2>
        `;

        // --- MATIN ---
        htmlGenere += `<h3>☀️ Matin</h3>`;
        if (jourData.matin && jourData.matin.length > 0) {
            jourData.matin.forEach(activite => {
                htmlGenere += genererBlocActivite(activite); // On utilise notre fonction utilitaire
            });
        }

        // --- APRÈS-MIDI ---
        htmlGenere += `<h3>🌤️ Après-midi</h3>`;
        if (jourData.aprem && jourData.aprem.length > 0) {
            jourData.aprem.forEach(activite => {
                htmlGenere += genererBlocActivite(activite); // On utilise notre fonction utilitaire
            });
        }

        htmlGenere += `</div>`;
    });

    // 4. On injecte tout le HTML généré
    conteneur.innerHTML = htmlGenere;

    // 5. GESTION DES CLICS : On rend les boutons interactifs
    // On récupère tous les boutons qu'on vient de créer
    const boutons = document.querySelectorAll('.btn-activite');
    
    boutons.forEach(bouton => {
        bouton.addEventListener('click', function() {
            // Le bloc "détails" est l'élément HTML juste après le bouton
            const detailsDiv = this.nextElementSibling;
            
            // On vérifie si le bloc est caché
            if (detailsDiv.style.display === "none") {
                detailsDiv.style.display = "block"; // On l'affiche
                this.innerHTML = this.innerHTML.replace('▶', '▼'); // On change la petite flèche
                this.style.backgroundColor = "#e2e2e8"; // On assombrit légèrement le bouton
            } else {
                detailsDiv.style.display = "none"; // On le cache
                this.innerHTML = this.innerHTML.replace('▼', '▶');
                this.style.backgroundColor = "#f4f4f9";
            }
        });
    });
});