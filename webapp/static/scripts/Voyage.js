function prettyPrint(targetId, storageKey, emptyMessage) {
    const target = document.getElementById(targetId);
    try {
        const raw = sessionStorage.getItem(storageKey);
        if (!raw) {
            target.textContent = emptyMessage;
            return;
        }
        const parsed = JSON.parse(raw);
        target.textContent = JSON.stringify(parsed, null, 2);
    } catch (err) {
        target.textContent = 'Impossible de lire les donnees stockees.';
        console.error(err);
    }
}

prettyPrint('algoResponse', 'algorithmResponse', 'Aucune reponse disponible.');
prettyPrint('submittedCriteria', 'criteriaFormPayload', 'Aucun critere disponible.');