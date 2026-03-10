function updateVal(id) {
    document.getElementById('val-' + id).textContent = document.getElementById(id).value;
}

document.getElementById('criteriaForm').addEventListener('submit', async function (e) {
    e.preventDefault();

    const formData = {
        detente: document.getElementById('detente').value,
        nature: document.getElementById('nature').value,
        sport: document.getElementById('sport').value,
        gastronomie: document.getElementById('gastronomie').value,
        culture: document.getElementById('culture').value
    };

    console.log("Critères soumis :", formData);

    const btn = document.getElementById('submitBtn');
    btn.disabled = true;
    btn.textContent = 'Soumission en cours...';

    try{
        const res = await fetch('/algorithm', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(formData)
        });

        if (!res.ok) {
            throw new Error(`Erreur HTTP ${res.status}`);
        }

        const data = await res.json();
        console.log("Réponse reçue :", data);
    } catch (error) {
        console.error("Erreur lors de la soumission :", error);
        alert("Une erreur est survenue lors de la soumission. Veuillez réessayer.");
    } finally {
        btn.disabled = false;
        btn.textContent = 'Soumettre';
    }
});