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

    // Fonction HTML épurée de tout style inline
    function genererBlocActivite(activite) {
        return `
            <div class="activite-container">
                <button class="btn-activite">
                    <span class="icone-fleche">▶</span> ${activite.nom}
                </button>
                
                <div class="details-activite">
                    <p><strong>📍 Lieu :</strong> ${activite.ville} (${activite.code_postal})</p>
                    <p><em>${activite.categories.join(', ')}</em></p>
                    <p>${activite.description || 'Aucune description disponible.'}</p>
                </div>
            </div>
        `;
    }

    // Boucle sur chaque jour
    dataVoyage.forEach(jourData => {
        htmlGenere += `
            <div class="jour-carte">
                <h2>Jour ${jourData.jour}</h2>
        `;

        // MATIN
        htmlGenere += `<h3>☀️ Matin</h3>`;
        if (jourData.matin && jourData.matin.length > 0) {
            jourData.matin.forEach(activite => {
                htmlGenere += genererBlocActivite(activite);
            });
        }

        // APRÈS-MIDI
        htmlGenere += `<h3>🌤️ Après-midi</h3>`;
        if (jourData.aprem && jourData.aprem.length > 0) {
            jourData.aprem.forEach(activite => {
                htmlGenere += genererBlocActivite(activite);
            });
        }

        htmlGenere += `</div>`;
    });

    // Injection du HTML
    conteneur.innerHTML = htmlGenere;

    // Gestion des clics : On bascule (toggle) les classes CSS
    const boutons = document.querySelectorAll('.btn-activite');
    
    boutons.forEach(bouton => {
        bouton.addEventListener('click', function() {
            const detailsDiv = this.nextElementSibling;
            const icone = this.querySelector('.icone-fleche');
            
            // toggle() ajoute la classe si elle n'y est pas, et l'enlève si elle y est
            detailsDiv.classList.toggle('visible');
            this.classList.toggle('ouvert');
            
            // Mise à jour de la flèche selon l'état
            if (detailsDiv.classList.contains('visible')) {
                icone.textContent = '▼';
            } else {
                icone.textContent = '▶';
            }
        });
    });
});