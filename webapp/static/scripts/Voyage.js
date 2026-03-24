document.addEventListener('DOMContentLoaded', function() {
    const data = sessionStorage.getItem('algorithmRes');
    
    if (!data) {
        console.error("Aucune donnée trouvée dans le stockage session.");
        window.location.href = '/Creation.html';
        return; 
    }
    
    // Le ".data" à la fin dépend de si ton backend a renvoyé {"data": [...] }
    // Si dataVoyage plante car c'est un objet qui contient un tableau, fais : JSON.parse(data).data;
    const dataVoyage = JSON.parse(data); 

    // 1. On cible notre conteneur HTML
    const conteneur = document.getElementById('conteneur-itineraire');
    
    // 2. On prépare une grande chaîne de caractères vide pour stocker notre HTML
    let htmlGenere = "";

    // 3. On boucle sur chaque jour
    dataVoyage.forEach(jour => {
        
        // On ouvre la "carte" du jour
        htmlGenere += `
            <div class="jour-carte" style="margin-bottom: 30px; border: 1px solid #ccc; padding: 15px; border-radius: 8px;">
                <h2>Jour ${jour.numero}</h2>
        `;

        // --- MATIN ---
        htmlGenere += `<h3>☀️ Matin</h3>`;
        jour.matin.forEach(activite => {
            htmlGenere += `
                <div class="activite" style="margin-left: 20px; margin-bottom: 15px;">
                    <h4>${activite.nom}</h4>
                    <p><strong>📍 Lieu :</strong> ${activite.ville} (${activite.code_postal})</p>
                    <p><em>${activite.categories.join(', ')}</em></p>
                    <p>${activite.description}</p>
                </div>
            `;
        });

        // --- APRÈS-MIDI ---
        htmlGenere += `<h3>🌤️ Après-midi</h3>`;
        jour.apresMidi.forEach(activite => {
            htmlGenere += `
                <div class="activite" style="margin-left: 20px; margin-bottom: 15px;">
                    <h4>${activite.nom}</h4>
                    <p><strong>📍 Lieu :</strong> ${activite.ville} (${activite.code_postal})</p>
                    <p><em>${activite.categories.join(', ')}</em></p>
                    <p>${activite.description}</p>
                </div>
            `;
        });

        // On ferme la "carte" du jour
        htmlGenere += `</div>`;
    });

    // 4. On injecte tout le HTML généré d'un seul coup dans la page
    conteneur.innerHTML = htmlGenere;
});