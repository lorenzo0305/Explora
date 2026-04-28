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

    // dataVoyage est mutable : on retire / remplace des activités dedans
    let dataVoyage = Array.isArray(parsed) ? parsed : asArray(parsed.data);
    if (!dataVoyage.length) {
        renderEmpty(parsed && parsed.message);
        return;
    }

    // Critères du formulaire
    const criteria = (() => { try { return JSON.parse(sessionStorage.getItem('criteriaFormPayload') || 'null'); } catch { return null; } })();

    // Sous-titre contextuel
    if (criteria && criteria.ville) {
        const jours = criteria.jours || dataVoyage.length;
        subtitle.textContent =
            `${jours} jour${jours > 1 ? 's' : ''} autour de ${criteria.ville} — vous pouvez retirer ou remplacer une activité.`;
    }

    // ─── Génération du HTML ─────────────────────────────────────
    const formatCategories = (cats) => {
        const arr = asArray(cats).filter(Boolean);
        return arr.length ? arr.join(' · ') : '';
    };

    const blocActivite = (act, jourIdx, moment, slotIdx) => {
        if (!act || typeof act !== 'object') return '';
        const nom = escapeHtml(act.nom || act.name || 'Activité');
        const ville = escapeHtml(act.ville || act.locality || '');
        const cp = act.code_postal ? escapeHtml(act.code_postal) : '';
        const cats = escapeHtml(formatCategories(act.categories));
        const desc = escapeHtml(act.description || act.desc || act.summary || '').trim();
        const website = escapeHtml(act.website || act.url || act.site || '');

        const lignes = [];
        if (ville) lignes.push(`<p><strong>📍 Lieu :</strong> ${ville}${cp ? ` (${cp})` : ''}</p>`);
        if (cats) lignes.push(`<p><em>${cats}</em></p>`);
        lignes.push(`<p>${desc || 'Aucune description disponible.'}</p>`);
        if (website) {
            const href = website.startsWith('http') ? website : `https://${website}`;
            lignes.push(`<p><strong>🌐 Site :</strong> <a href="${href}" target="_blank" rel="noopener noreferrer">${href}</a></p>`);
        }

        return `
            <div class="activite-container" data-jour="${jourIdx}" data-moment="${moment}" data-slot="${slotIdx}">
                <button type="button" class="btn-activite">
                    <span class="icone-fleche">▶</span>
                    <span class="activite-nom">${nom}</span>
                </button>
                <div class="details-activite">
                    ${lignes.join('')}
                    <div class="activite-actions" style="display:flex; gap:8px; margin-top:10px; flex-wrap:wrap;">
                        <button type="button" class="btn-act-replace" title="Remplacer par une autre activité">↔ Remplacer</button>
                        <button type="button" class="btn-act-remove" title="Retirer cette activité">✕ Retirer</button>
                    </div>
                </div>
            </div>`;
    };

    const blocTrajet = (label) => label
        ? `<div class="trajet-info">${escapeHtml(label)}</div>`
        : '';

    const blocMoment = (titre, icone, activites, trajets = {}, momentKey, jourIdx) => {
        const arr = asArray(activites);
        let inner = '';
        if (!arr.length) {
            inner = `<div class="moment-vide">Rien de prévu pour ce moment.</div>`;
        } else {
            arr.forEach((a, i) => {
                inner += blocActivite(a, jourIdx, momentKey, i);
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

    function renderItineraire() {
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
                    ${blocMoment('Matin', '☀️', jourData.matin, trajets, 'matin', idx)}
                    ${trajets.midi ? `<div class="trajet-info">🍴 Pause déjeuner — ${escapeHtml(trajets.midi)}</div>` : ''}
                    ${blocMoment('Après-midi', '🌤️', jourData.aprem, trajets, 'aprem', idx)}
                </article>`;
        });
        conteneur.innerHTML = html;
        wireAccordion();
        wireEditButtons();
    }

    function wireAccordion() {
        conteneur.querySelectorAll('.btn-activite').forEach((bouton) => {
            bouton.addEventListener('click', function (e) {
                if (e.target.closest('.btn-act-replace') || e.target.closest('.btn-act-remove')) return;
                const detailsDiv = this.nextElementSibling;
                detailsDiv.classList.toggle('visible');
                this.classList.toggle('ouvert');
            });
        });
    }

    function wireEditButtons() {
        conteneur.querySelectorAll('.btn-act-remove').forEach(btn => {
            btn.addEventListener('click', e => {
                e.stopPropagation();
                const cont = btn.closest('.activite-container');
                const j = parseInt(cont.dataset.jour, 10);
                const m = cont.dataset.moment;
                const s = parseInt(cont.dataset.slot, 10);
                if (!confirm('Retirer cette activité du jour ' + (j + 1) + ' ?')) return;
                const list = asArray(dataVoyage[j][m]);
                list.splice(s, 1);
                dataVoyage[j][m] = list;
                // On met à jour le sessionStorage pour persistance temporaire
                sessionStorage.setItem('algorithmRes', JSON.stringify({ data: dataVoyage }));
                renderItineraire();
            });
        });
        conteneur.querySelectorAll('.btn-act-replace').forEach(btn => {
            btn.addEventListener('click', e => {
                e.stopPropagation();
                const cont = btn.closest('.activite-container');
                openReplaceModal({
                    jourIdx: parseInt(cont.dataset.jour, 10),
                    moment:  cont.dataset.moment,
                    slotIdx: parseInt(cont.dataset.slot, 10),
                });
            });
        });
    }

    // ─── Modal de remplacement ────────────────────────────────
    function ensureModal() {
        let modal = document.getElementById('replaceModal');
        if (modal) return modal;
        modal = document.createElement('div');
        modal.id = 'replaceModal';
        modal.style.cssText = 'position:fixed;inset:0;background:rgba(28,28,28,0.55);display:none;align-items:center;justify-content:center;z-index:9999;padding:20px;';
        modal.innerHTML = `
            <div class="rm-card" style="background:#FAF8F5;max-width:760px;width:100%;max-height:85vh;overflow:auto;border-radius:16px;padding:24px;box-shadow:0 20px 60px rgba(0,0,0,0.3);font-family:Lora,serif;">
                <div class="rm-header" style="display:flex;justify-content:space-between;align-items:flex-start;gap:16px;margin-bottom:18px;">
                    <div>
                        <h3 style="font-family:'Cormorant Garamond',serif;font-size:24px;margin:0;color:#1C1C1C;">Choisir une activité de remplacement</h3>
                        <p class="rm-helper" style="margin:8px 0 0;color:#6b5f57;font-size:14px;line-height:1.6;max-width:620px;">Conservez la barre de recherche et découvrez ci-dessous des activités recommandées dans un style inspiré du catalogue.</p>
                    </div>
                    <button id="rmClose" type="button" style="background:none;border:none;font-size:26px;cursor:pointer;color:#6b5f57;line-height:1;">×</button>
                </div>
                <input id="rmSearch" type="text" placeholder="Rechercher (nom, lieu, catégorie)…"
                       style="width:100%;padding:12px 16px;border:1px solid #D4C3B3;border-radius:10px;background:#fff;font-family:Lora,serif;font-size:14px;margin-bottom:22px;">
                <div class="rm-search-results">
                    <div class="rm-rec-title">Résultats de recherche</div>
                    <div id="rmResults" class="rm-results"></div>
                </div>
                <div class="rm-recommendations">
                    <div class="rm-rec-title">Suggestions recommandées</div>
                    <div id="rmCarousel" class="rm-carousel"></div>
                </div>
            </div>`;
        document.body.appendChild(modal);
        modal.addEventListener('click', e => { if (e.target === modal) closeModal(); });
        modal.querySelector('#rmClose').addEventListener('click', closeModal);
        return modal;
    }

    function closeModal() {
        const m = document.getElementById('replaceModal');
        if (m) m.style.display = 'none';
    }

    let _currentTarget = null;
    let _searchTimer = null;

    function openReplaceModal(target) {
        _currentTarget = target;
        const modal = ensureModal();
        modal.style.display = 'flex';
        const input = modal.querySelector('#rmSearch');
        const results = modal.querySelector('#rmResults');
        const carousel = modal.querySelector('#rmCarousel');
        input.value = '';
        results.innerHTML = '<p style="color:#6b5f57;font-style:italic;text-align:center;padding:20px 0;">Chargement des résultats…</p>';
        carousel.innerHTML = '<p style="color:#6b5f57;font-style:italic;text-align:center;padding:20px 0;width:100%;">Chargement des recommandations…</p>';

        const currentAct = dataVoyage[target.jourIdx]?.[target.moment]?.[target.slotIdx];
        const currentName = currentAct?.nom || currentAct?.name || currentAct?.label || '';
        if (currentName) {
            loadRecommendations(currentName).then(items => renderRecommendationCards(items, carousel));
        } else {
            carousel.innerHTML = '<p style="color:#6b5f57;font-style:italic;text-align:center;padding:20px 0;width:100%;">Aucune activité de référence trouvée.</p>';
        }

        // Charge des suggestions initiales en fonction de la région
        loadSuggestions('').then(items => renderModalResults(items, results));

        input.oninput = () => {
            clearTimeout(_searchTimer);
            _searchTimer = setTimeout(() => {
                loadSuggestions(input.value).then(items => renderModalResults(items, results));
            }, 280);
        };
    }

    async function loadSuggestions(q) {
        const region = criteria?.region || '';
        const slug = (region && region !== 'all') ? region : '';
        const query = (q || '').trim();

        // 1) si une région est connue, on tape /regions/{slug}/cards
        if (slug) {
            const url = `/regions/${encodeURIComponent(slug)}/cards?limit=24` + (query ? `&q=${encodeURIComponent(query)}` : '');
            try {
                const r = await fetch(url);
                if (r.ok) {
                    const data = await r.json();
                    if (Array.isArray(data) && data.length) return data;
                }
            } catch (_) {}
        }

        // 2) sinon (ou en complément), on retombe sur /search
        if (query.length >= 2) {
            try {
                const r = await fetch(`/search?query=${encodeURIComponent(query)}&limit=24`);
                if (r.ok) {
                    const data = await r.json();
                    if (Array.isArray(data)) return data;
                }
            } catch (_) {}
        }

        return [];
    }

    async function loadRecommendations(activityName) {
        try {
            const r = await fetch(`/suggestions?activity=${encodeURIComponent(activityName)}`);
            if (r.ok) {
                const data = await r.json();
                return Array.isArray(data) ? data : [];
            }
        } catch (_) {}
        return [];
    }

    function renderRecommendationCards(items, container) {
        if (!items || !items.length) {
            container.innerHTML = '<div class="rm-empty">Aucune recommandation disponible pour cette activité.</div>';
            return;
        }
        container.innerHTML = items.map(item => {
            const id = escapeHtml(String(item.id || item._id || item.nom || item.name || ''));
            const name = escapeHtml(item.name || item.nom || item.label || 'Sans nom');
            const subtitle = escapeHtml(item.locality || item.region || (item.categories && item.categories[0]) || item.ville || '');
            const img = escapeHtml((item.image && /^https?:/.test(item.image)) ? item.image : (item.cover || item.photo || '/static/img/no-image.jpg'));
            const match = escapeHtml(item.match || '');
            const distance = escapeHtml(item.distance || '');
            const desc = escapeHtml(item.description || item.desc || item.summary || '').trim();
            const website = escapeHtml(item.website || item.url || item.site || '');
            const websiteHref = website ? (website.startsWith('http') ? website : `https://${website}`) : '';
            const hasDetail = desc || websiteHref;
            return `
                <div class="rm-card-item${hasDetail ? ' has-detail' : ''}" data-id="${id}">
                    <div class="rm-card-visual">
                        <img src="${img}" alt="${name}" onerror="this.src='/static/img/no-image.jpg'" />
                    </div>
                    <div class="rm-card-body">
                        <div class="rm-card-title">${name}</div>
                        <div class="rm-card-meta">${subtitle}</div>
                        <div class="rm-card-stats"><span>${match}</span> <span>${distance}</span></div>
                        ${hasDetail ? `<div class="rm-card-detail">
                            ${desc ? `<div class="rm-card-desc">${desc}</div>` : ''}
                            ${websiteHref ? `<a href="${websiteHref}" target="_blank" rel="noopener noreferrer" class="rm-card-link">Voir le site</a>` : ''}
                        </div>` : ''}
                    </div>
                    <div class="rm-card-actions">
                        <button type="button" class="rm-pick">Choisir</button>
                    </div>
                </div>`;
        }).join('');

        container.querySelectorAll('.rm-card-item').forEach(card => {
            const pick = () => {
                const id = card.dataset.id;
                const item = items.find(x => String(x.id || x._id || x.nom || x.name) === id);
                if (!item) return;
                applyReplacement(item);
            };
            const detail = card.querySelector('.rm-card-detail');
            if (detail) {
                card.addEventListener('click', function (e) {
                    if (e.target.closest('.rm-pick')) return;
                    card.classList.toggle('expanded');
                });
            }
            card.querySelector('.rm-pick')?.addEventListener('click', e => { e.stopPropagation(); pick(); });
        });
    }

    function renderModalResults(items, container) {
        if (!items || !items.length) {
            container.innerHTML = '<p style="color:#6b5f57;font-style:italic;text-align:center;padding:20px 0;">Aucune activité disponible. Essayez un autre terme.</p>';
            return;
        }
        container.innerHTML = items.map(it => {
            const id = it.id || it._id || '';
            const name = it.name || it.nom || 'Sans nom';
            const sub  = it.locality || it.region || (it.types && it.types[0]) || '';
            const img  = (it.image && /^https?:/.test(it.image)) ? it.image : '/static/img/no-image.jpg';
            return `
                <div class="rm-item" data-id="${escapeHtml(id)}"
                     style="display:flex;gap:12px;align-items:center;padding:12px;border:1px solid #D4C3B3;border-radius:10px;background:#fff;cursor:pointer;transition:border-color .15s,transform .15s;">
                    <img src="${escapeHtml(img)}" alt="" onerror="this.src='/static/img/no-image.jpg'"
                         style="width:60px;height:60px;border-radius:10px;object-fit:cover;flex-shrink:0;">
                    <div style="flex:1;min-width:0;">
                        <div style="font-family:'Cormorant Garamond',serif;font-size:16px;font-weight:600;color:#1C1C1C;line-height:1.2;">${escapeHtml(name)}</div>
                        <div style="font-size:12px;color:#6b5f57;">${escapeHtml(sub)}</div>
                    </div>
                    <button type="button" class="rm-pick" style="flex-shrink:0;background:#FF6F61;color:#fff;border:none;padding:10px 16px;border-radius:999px;cursor:pointer;font-weight:600;font-family:Montserrat,sans-serif;font-size:12px;text-transform:uppercase;letter-spacing:0.5px;">Choisir</button>
                </div>`;
        }).join('');

        container.querySelectorAll('.rm-item').forEach(row => {
            const pick = () => {
                const id = row.dataset.id;
                const item = items.find(x => String(x.id || x._id) === String(id));
                if (!item) return;
                applyReplacement(item);
            };
            row.addEventListener('click', pick);
            row.querySelector('.rm-pick')?.addEventListener('click', e => { e.stopPropagation(); pick(); });
            row.addEventListener('mouseenter', () => { row.style.borderColor = '#FF6F61'; row.style.transform = 'translateX(2px)'; });
            row.addEventListener('mouseleave', () => { row.style.borderColor = '#D4C3B3'; row.style.transform = 'none'; });
        });
    }

    function mongoToActivity(item) {
        // Reformate un objet "Mongo public" vers le schéma utilisé par l'algo
        return {
            nom: item.name || item.nom || 'Activité',
            ville: item.locality || item.ville || '',
            code_postal: item.code_postal || '',
            categories: item.categories || item.types || [],
            description: item.description || item.desc || item.summary || '',
            website: item.website || item.url || item.site || '',
            image: item.image || '',
            id: item.id || item._id || ''
        };
    }

    function applyReplacement(item) {
        if (!_currentTarget) return;
        const { jourIdx, moment, slotIdx } = _currentTarget;
        const list = asArray(dataVoyage[jourIdx][moment]);
        list[slotIdx] = mongoToActivity(item);
        dataVoyage[jourIdx][moment] = list;
        sessionStorage.setItem('algorithmRes', JSON.stringify({ data: dataVoyage }));
        closeModal();
        renderItineraire();
    }

    // ─── Render initial ─────────────────────────────────────
    renderItineraire();

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
