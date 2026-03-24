document.addEventListener('DOMContentLoaded', function() {
    const data = sessionStorage.getItem('algorithmRes');
    
    if (!data) {
        console.error("Aucune donnée trouvée dans le stockage session.");
        window.location.href = '/Creation.html';
        return; // Stoppe l'exécution du reste du code
    }
    
    const dataVoyage = JSON.parse(data);
    
    // Pour vérifier que tout fonctionne bien :
    console.log("Mes données prêtes à être affichées :", dataVoyage);

    // C'est ici qu'on va générer le HTML !
});