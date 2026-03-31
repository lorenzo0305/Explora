document.addEventListener('DOMContentLoaded', function() {
    const data = sessionStorage.getItem('algorithmRes');
    
    if (!data) {
        console.error("Aucune donnée trouvée dans le stockage session.");
        window.location.href = '/Creation.html';
        return; 
    }
    
    // On parse les données. 
    // Si c'est un tableau direct, on le prend. Si c'est dans { "data": [...] }, on prend .data
    const parsedData = JSON.parse(data);
    const dataVoyage = Array.isArray(parsedData) ? parsedData : parsedData.data; 

    // 1. On cible notre conteneur HTML
    const conteneur = document.getElementById('conteneur-itineraire');
    
    // 2. On prépare une grande chaîne de caractères vide pour stocker notre HTML
    let htmlGenere = "";

    // 3. On boucle sur chaque jour
    dataVoyage.forEach(jourData => {
        
        // On ouvre la "carte" du jour (Attention : la clé est 'jour' et non 'numero')
        htmlGenere += `
            <div class="jour-carte" style="margin-bottom: 30px; border: 1px solid #ccc; padding: 15px; border-radius: 8px;">
                <h2>Jour ${jourData.jour}</h2>
        `;

        // --- MATIN ---
        htmlGenere += `<h3>☀️ Matin</h3>`;
        // On vérifie que le matin existe et contient des éléments
        if (jourData.matin && jourData.matin.length > 0) {
            jourData.matin.forEach(activite => {
                htmlGenere += `
                    <div class="activite" style="margin-left: 20px; margin-bottom: 5px;">
                        <h4>${activite.nom}</h4>
                    </div>
                `;
            });
        }

        // --- APRÈS-MIDI ---
        htmlGenere += `<h3>🌤️ Après-midi</h3>`;
        // On vérifie que l'aprem existe (Attention : la clé est 'aprem' et non 'apresMidi')
        if (jourData.aprem && jourData.aprem.length > 0) {
            jourData.aprem.forEach(activite => {
                htmlGenere += `
                    <div class="activite" style="margin-left: 20px; margin-bottom: 5px;">
                        <h4>${activite.nom}</h4>
                    </div>
                `;
            });
        }

        // On ferme la "carte" du jour
        htmlGenere += `</div>`;
    });

    // 4. On injecte tout le HTML généré d'un seul coup dans la page
    conteneur.innerHTML = htmlGenere;
});