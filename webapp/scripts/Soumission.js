function updateVal(id) {
    document.getElementById('val-' + id).textContent = document.getElementById(id).value;
}

document.getElementById('criteriaForm').addEventListener('submit', function (e) {
    e.preventDefault();

    const formData = {
        detente: document.getElementById('detente').value,
        nature: document.getElementById('nature').value,
        sport: document.getElementById('sport').value,
        gastronomie: document.getElementById('gastronomie').value,
        culture: document.getElementById('culture').value
    };

    console.log("Critères soumis :", formData);

    // Animation bouton
    const btn = document.getElementById('submitBtn');
    const originalText = btn.innerHTML;
    btn.innerHTML = "Création en cours...";
    btn.style.opacity = "0.8";

    // Simulation traitement ou redirection
    setTimeout(() => {
        // Redirection vers la page de création/édition classique avec ces préréglages (pour l'instant, juste redirection)
        // Vous pouvez passer ces paramètres en URL ou localStorage
        localStorage.setItem('wish_last_criteria', JSON.stringify(formData));
        window.location.href = '/creation';
    }, 1000);
});