document.addEventListener('DOMContentLoaded', function () {
    const conteneur = document.getElementById('conteneur-itineraire');
    const subtitle = document.getElementById('voyage-subtitle');
    const saveBtn = document.getElementById('saveVoyageBtn');

    // ─── Helpers ────────────────────────────────────────────────
    const escapeHtml = (s) => String(s ?? '')
        .replace(/&/g, '&amp;').replace(/</g, '&lt;')
        .replace(/>/g, '&gt;').replace(/"/g, '&quot;').replace(/'/g, '&#39;');

    const asArray = (v) => Array.isArray(v) ? v : (v && typeof v === 'object' ? Object.values(v) : []);

    const renderEmpty = (msg) => {
        conteneur.innerHTML = `
            <div class="voyage-empty">
                <h2>Aucun itinéraire à afficher</h2>
                <p>${escapeHtml(msg || "Nous n'avons pas trouvé de voyage correspondant à vos critères.")}</p>
                <a href="/makejourney" class="btn-explorer" style="display:inline-block; text-decoration:none;">Créer un nouveau voyage</a>
            </div>`;
    };

    // ─── Récupération des données ───────────────────────────────
    const raw = sessionStorage.getItem('algorithmRes');
    if (!raw) {
        renderEmpty("Aucune donnée trouvée — revenez créer un voyage.");
        return;
    }

    let parsed;
    try { parsed = JSON.parse(raw); } catch (e) {
        console.error("JSON invalide :", e);
        renderEmpty("Les données du voyage sont invalides.");
        return;
    }

    const dataVoyage = Array.isArray(parsed) ? parsed : asArray(parsed.data);
    if (!dataVoyage.length) {
        renderEmpty(parsed && parsed.message);
        return;
    }

    // Sous-titre contextuel à partir des critères du formulaire
    try {
        const payload = JSON.parse(sessionStorage.getItem('criteriaFormPayload') || 'null');
        if (payload && payload.ville) {
            const jours = payload.jours || dataVoyage.length;
            subtitle.textContent =
                `${jours} jour${jours > 1 ? 's' : ''} autour de ${payload.ville} — cliquez sur une activité pour en savoir plus.`;
        }
    } catch (_) { /* non bloquant */ }

    // ─── Génération du HTML ─────────────────────────────────────
    const formatCategories = (cats) => {
        const arr = asArray(cats).filter(Boolean);
        return arr.length ? arr.join(' · ') : '';
    };

    const blocActivite = (act) => {
        if (!act || typeof act !== 'object') return '';
        const nom = escapeHtml(act.nom || act.name || 'Activité');
        const ville = escapeHtml(act.ville || '');
        const cp = act.code_postal ? escapeHtml(act.code_postal) : '';
        const cats = escapeHtml(formatCategories(act.categories));
        const desc = escapeHtml(act.description || '').trim();

        const lignes = [];
        if (ville) lignes.push(`<p><strong>📍 Lieu :</strong> ${ville}${cp ? ` (${cp})` : ''}</p>`);
        if (cats) lignes.push(`<p><em>${cats}</em></p>`);
        lignes.push(`<p>${desc || 'Aucune description disponible.'}</p>`);

        return `
            <div class="activite-container">
                <button type="button" class="btn-activite">
                    <span class="icone-fleche">▶</span>
                    <span class="activite-nom">${nom}</span>
                </button>
                <div class="details-activite">
                    ${lignes.join('')}
                </div>
            </div>`;
    };

    const blocTrajet = (label) => label
        ? `<div class="trajet-info">${escapeHtml(label)}</div>`
        : '';

    const blocMoment = (titre, icone, activites, trajets = {}, momentKey = '') => {
        const arr = asArray(activites);
        let inner = '';
        if (!arr.length) {
            inner = `<div class="moment-vide">Rien de prévu pour ce moment.</div>`;
        } else {
            arr.forEach((a, i) => {
                inner += blocActivite(a);
                if (momentKey === 'matin' && i === 0 && arr.length > 1 && trajets.m1_m2) {
                    inner += blocTrajet(trajets.m1_m2);
                }
                if (momentKey === 'aprem' && i === 0 && arr.length > 1 && trajets.a1_a2) {
                    inner += blocTrajet(trajets.a1_a2);
                }
            });
        }
        return `
            <section class="moment-section">
                <h3 class="moment-titre">
                    <span class="moment-icone">${icone}</span>${escapeHtml(titre)}
                </h3>
                ${inner}
            </section>`;
    };

    let html = '';
    dataVoyage.forEach((jourData, idx) => {
        const numero = jourData.jour ?? (idx + 1);
        const trajets = jourData.trajets || {};
        html += `
            <article class="jour-carte">
                <header class="jour-entete">
                    <span class="jour-numero">Jour ${escapeHtml(numero)}</span>
                    <h2 class="jour-titre">Votre programme</h2>
                </header>
                ${blocMoment('Matin', '☀️', jourData.matin, trajets, 'matin')}
                ${trajets.midi ? `<div class="trajet-info">🍴 Pause déjeuner — ${escapeHtml(trajets.midi)}</div>` : ''}
                ${blocMoment('Après-midi', '🌤️', jourData.aprem, trajets, 'aprem')}
            </article>`;
    });

    conteneur.innerHTML = html;

    // ─── Accordéon ──────────────────────────────────────────────
    conteneur.querySelectorAll('.btn-activite').forEach((bouton) => {
        bouton.addEventListener('click', function () {
            const detailsDiv = this.nextElementSibling;
            detailsDiv.classList.toggle('visible');
            this.classList.toggle('ouvert');
        });
    });

    // ─── Sauvegarde du voyage ──────────────────────────────────
    const JOURNEYS_KEY = 'wish_journeys_v1';
    const readCache = () => { try { return JSON.parse(localStorage.getItem(JOURNEYS_KEY) || '[]'); } catch { return []; } };
    const writeCache = (arr) => { try { localStorage.setItem(JOURNEYS_KEY, JSON.stringify(arr)); } catch {} };
    const upsertCache = (journey) => {
        const arr = readCache();
        const i = arr.findIndex(x => String(x.id) === String(journey.id));
        if (i >= 0) arr[i] = journey; else arr.unshift(journey);
        writeCache(arr);
    };

    const pickCoverFromPlan = (plan) => {
        for (const day of asArray(plan)) {
            for (const key of ['matin', 'aprem']) {
                for (const act of asArray(day?.[key])) {
                    if (act && typeof act === 'object') {
                        const img = act.image || act.photo || act.cover;
                        if (img) return img;
                    }
                }
            }
        }
        return '/static/img/no-image.jpg';
    };

    if (saveBtn) {
        saveBtn.addEventListener('click', async () => {
            const criteria = (() => { try { return JSON.parse(sessionStorage.getItem('criteriaFormPayload') || 'null'); } catch { return null; } })();
            const nowIso = new Date().toISOString();
            const locationLabel = criteria?.ville || '';
            const name = locationLabel
                ? `Voyage à ${locationLabel} — ${new Date().toLocaleDateString('fr-FR')}`
                : `Voyage du ${new Date().toLocaleDateString('fr-FR')}`;

            const payload = {
                name,
                location: locationLabel,
                cover: pickCoverFromPlan(dataVoyage),
                createdAt: nowIso,
                updatedAt: nowIso,
                criteria,
                plan: dataVoyage,
            };

            saveBtn.disabled = true;
            const originalText = saveBtn.textContent;
            saveBtn.textContent = 'Sauvegarde...';
            try {
                const res = await fetch('/journeys', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(payload),
                });
                let serverResp = null;
                try { serverResp = await res.json(); } catch (_) { /* pas de JSON */ }
                if (!res.ok) {
                    const msg = (serverResp && (serverResp.message || serverResp.detail)) || `HTTP ${res.status}`;
                    throw new Error(msg);
                }

                // On reflète immédiatement en cache local pour que "Mes voyages"
                // l'affiche sans attendre un refetch, même offline.
                upsertCache({ ...payload, id: serverResp.id });

                saveBtn.textContent = '✔️ Sauvegardé';
                setTimeout(() => { saveBtn.textContent = originalText; saveBtn.disabled = false; }, 2200);
            } catch (err) {
                console.error('Erreur sauvegarde :', err);
                alert("Impossible de sauvegarder ce voyage : " + (err.message || 'réessayez.'));
                saveBtn.textContent = originalText;
                saveBtn.disabled = false;
            }
        });
    }
});
