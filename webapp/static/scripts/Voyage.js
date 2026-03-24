addEventListener.document('DOMContentLoaded', function() {
    const data = sessionStorage.getItem('algorithmRes');
    if (!data) {
        console.error("Aucune donnée trouvée dans le stockage session.");
        window.location.href = '/Creation.html';
        return;
    }
    const dataVoyage = JSON.parse(data);
});