/* ===== INJECTION DES ANIMATIONS CSS ===== */
if (!document.getElementById('explora-animations')) {
    const style = document.createElement('style');
    style.id = 'explora-animations';
    style.innerHTML = `@keyframes spin { 100% { transform: rotate(360deg); } }`;
    document.head.appendChild(style);
}

document.addEventListener('DOMContentLoaded', function () {
    const conteneur = document.getElementById('daysList');
    const subtitle = document.getElementById('voyage-subtitle');
    const saveBtn = document.getElementById('saveVoyageBtn');

    let _currentTarget = null;
    let _searchTimer = null;
    let _openDayIndex = null; 
    
    const DAY_FALLBACKS = ['/static/img/roussillon.jpg', '/static/img/canoe_occitanie.jpg', '/static/img/provence.jpg', '/static/img/semur_en_auxois.jpg', '/static/img/menton.jpg'];

    const escapeHtml = (s) => String(s ?? '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;').replace(/'/g, '&#39;');
    const asArray = (v) => Array.isArray(v) ? v : (v && typeof v === 'object' ? Object.values(v) : []);

    const renderEmpty = (msg) => {
        conteneur.innerHTML = `<div class="voyage-empty"><h2>Aucun itinéraire à afficher</h2><p>${escapeHtml(msg || "Nous n'avons pas trouvé de voyage correspondant à vos critères.")}</p><a href="/makejourney" class="btn-explorer" style="display:inline-block; text-decoration:none;">Créer un nouveau voyage</a></div>`;
    };

    const raw = sessionStorage.getItem('algorithmRes');
    if (!raw) { renderEmpty("Aucune donnée trouvée — revenez créer un voyage."); return; }

    let parsed;
    try { parsed = JSON.parse(raw); } catch (e) { renderEmpty("Les données du voyage sont invalides."); return; }

    let dataVoyage = Array.isArray(parsed) ? parsed : asArray(parsed.data);
    if (!dataVoyage.length) { renderEmpty(parsed && parsed.message); return; }

    const criteria = (() => { try { return JSON.parse(sessionStorage.getItem('criteriaFormPayload') || 'null'); } catch { return null; } })();

    if (criteria && criteria.ville) {
        const jours = criteria.jours || dataVoyage.length;
        subtitle.textContent = `${jours} jour${jours > 1 ? 's' : ''} autour de ${criteria.ville} — vous pouvez retirer ou remplacer une activité.`;
    }

    function getDayImage(jourData, idx) {
        for (const key of ['matin', 'midi', 'aprem', 'soir']) {
            for (const act of asArray(jourData[key])) {
                let img = window.getActivityImage(act);
                if (img && !img.includes('default')) return img;
            }
        }
        return DAY_FALLBACKS[idx % DAY_FALLBACKS.length];
    }

    const formatCategories = (catsRaw) => {
        let arr = [];
        if (Array.isArray(catsRaw)) arr = catsRaw.map(c => typeof c === 'string' ? c : (c.name || ""));
        else if (typeof catsRaw === 'string') arr = catsRaw.split(',').map(s => s.trim());
        arr = arr.filter(Boolean);
        return arr.length ? arr.join(' · ') : '';
    };

    // --- LE CŒUR DE LA PRÉSENTATION ---
    function renderItineraire() {
        conteneur.innerHTML = '';
        const slotsLabels = { matin: 'Matinée', midi: 'Midi', aprem: 'Après-midi', soir: 'Soirée' };

        dataVoyage.forEach((jourData, idx) => {
            const numero = jourData.jour ?? (idx + 1);
            const trajets = jourData.trajets || {}; 
            
            const dayContainer = document.createElement('div');
            dayContainer.className = 'day-column';

            const item = document.createElement('div');
            item.className = 'day-item';
            
            const left = document.createElement('div'); left.className = 'day-left';
            const thumb = document.createElement('div'); thumb.className = 'day-thumb';
            thumb.style.backgroundImage = `url('${getDayImage(jourData, idx)}')`;

            const meta = document.createElement('div'); meta.className = 'day-meta';
            const name = document.createElement('div'); name.className = 'day-name';
            name.textContent = 'Journée ' + numero;

            let activitiesCount = 0;
            let presentSlots = [];
            let firstNames = [];
            
            for (const key of ['matin', 'midi', 'aprem', 'soir']) {
                const arr = asArray(jourData[key]);
                if (arr.length > 0) {
                    presentSlots.push(slotsLabels[key]);
                    activitiesCount += arr.length;
                    arr.forEach(a => firstNames.push(a.nom || a.name || 'Activité'));
                }
            }

            const desc = document.createElement('div'); desc.className = 'day-desc';
            if (activitiesCount > 0) {
                desc.textContent = firstNames.slice(0, 3).join(' · ') + (activitiesCount > 3 ? ` +${activitiesCount - 3}` : '');
            } else {
                desc.textContent = 'Journée libre'; desc.classList.add('day-desc-empty');
            }

            meta.appendChild(name); meta.appendChild(desc);

            if (activitiesCount > 0) {
                const chips = document.createElement('div'); chips.className = 'day-chips';
                const cnt = document.createElement('span'); cnt.className = 'day-chip day-chip-count';
                cnt.textContent = `${activitiesCount} activité${activitiesCount > 1 ? 's' : ''}`; chips.appendChild(cnt);
                presentSlots.forEach(s => { const c = document.createElement('span'); c.className = 'day-chip'; c.textContent = s; chips.appendChild(c); });
                meta.appendChild(chips);
            }

            left.appendChild(thumb); left.appendChild(meta); item.appendChild(left);

            const rightWrap = document.createElement('div'); rightWrap.style.marginTop = '10px';
            const detailBtn = document.createElement('button'); detailBtn.className = 'btn-voir-detail'; detailBtn.textContent = 'Voir le détail';
            rightWrap.appendChild(detailBtn); item.appendChild(rightWrap);

            // L'ACCORDÉON
            const detailsInline = document.createElement('div');
            detailsInline.className = 'day-details-inline';

            ['matin', 'midi', 'aprem', 'soir'].forEach(slotKey => {
                const arr = asArray(jourData[slotKey]);
                if (arr.length > 0) {
                    const sec = document.createElement('div');
                    sec.className = 'inline-slot-sec';
                    sec.innerHTML = `<div class="inline-slot-title">${slotsLabels[slotKey]}</div>`;

                    arr.forEach((act, actIdx) => {
                        const actCard = document.createElement('div');
                        actCard.className = 'inline-act-card';

                        const img = escapeHtml(window.getActivityImage(act));
                        const actName = escapeHtml(act.nom || act.name || 'Activité');
                        const actCity = escapeHtml(act.ville || act.locality || '');
                        const cp = escapeHtml(act.code_postal || '');
                        const cats = escapeHtml(formatCategories(act.categories || act.types || act.category));
                        const actDesc = escapeHtml(act.description || act.desc || "Aucune description détaillée n'est disponible.").trim();

                        // STRUCTURE PLIÉE PAR DÉFAUT
                        actCard.innerHTML = `
                            <button type="button" class="btn-activite">
                                <img src="${img}" class="inline-act-thumb" onerror="this.style.display='none'">
                                <div class="activite-info-header">
                                    <span class="activite-nom">${actName}</span>
                                    <span class="activite-meta-header">${actCity}</span>
                                </div>
                                <span class="icone-fleche">▶</span>
                            </button>
                            <div class="details-activite">
                                <p class="categories-text">${cats}</p>
                                <p><strong>📍 Lieu :</strong> ${actCity} ${cp ? `(${cp})` : ''}</p>
                                <p>${actDesc}</p>
                                <div class="activite-actions">
                                    <button type="button" class="btn-act-replace">↔ Remplacer</button>
                                    <button type="button" class="btn-act-remove">✕ Retirer</button>
                                </div>
                            </div>
                        `;

                        // PLIER / DÉPLIER L'ACTIVITÉ
                        actCard.querySelector('.btn-activite').addEventListener('click', function() {
                            const detailsDiv = this.nextElementSibling;
                            detailsDiv.classList.toggle('visible');
                            this.classList.toggle('ouvert');
                        });

                        // BOUTONS
                        actCard.querySelector('.btn-act-remove').addEventListener('click', (e) => {
                            e.stopPropagation();
                            if (!confirm('Retirer cette activité ?')) return;
                            const currentList = asArray(dataVoyage[idx][slotKey]);
                            currentList.splice(actIdx, 1);
                            dataVoyage[idx][slotKey] = currentList;
                            sessionStorage.setItem('algorithmRes', JSON.stringify({ data: dataVoyage }));
                            _openDayIndex = idx;
                            renderItineraire();
                        });

                        actCard.querySelector('.btn-act-replace').addEventListener('click', (e) => {
                            e.stopPropagation();
                            _openDayIndex = idx;
                            openReplaceModal({ jourIdx: idx, moment: slotKey, slotIdx: actIdx });
                        });

                        sec.appendChild(actCard);

                        if (slotKey === 'matin' && actIdx === 0 && arr.length > 1 && trajets.m1_m2) {
                            sec.insertAdjacentHTML('beforeend', `<div class="trajet-info">${escapeHtml(trajets.m1_m2)}</div>`);
                        }
                        if (slotKey === 'aprem' && actIdx === 0 && arr.length > 1 && trajets.a1_a2) {
                            sec.insertAdjacentHTML('beforeend', `<div class="trajet-info">${escapeHtml(trajets.a1_a2)}</div>`);
                        }
                    });
                    
                    detailsInline.appendChild(sec);
                    
                    if (slotKey === 'matin' && trajets.midi) {
                        detailsInline.insertAdjacentHTML('beforeend', `<div class="trajet-info">🍴 Pause déjeuner — ${escapeHtml(trajets.midi)}</div>`);
                    }
                }
            });

            const toggleAction = () => {
                if (activitiesCount === 0) return;
                if (detailsInline.classList.contains('show')) {
                    detailsInline.classList.remove('show');
                    item.classList.remove('is-open');
                    detailBtn.textContent = 'Voir le détail';
                    if (_openDayIndex === idx) _openDayIndex = null;
                } else {
                    detailsInline.classList.add('show');
                    item.classList.add('is-open');
                    detailBtn.textContent = 'Fermer';
                    _openDayIndex = idx;
                }
            };

            item.addEventListener('click', toggleAction);

            if (_openDayIndex === idx && activitiesCount > 0) {
                detailsInline.classList.add('show');
                item.classList.add('is-open');
                detailBtn.textContent = 'Fermer';
            }

            dayContainer.appendChild(item);
            dayContainer.appendChild(detailsInline);
            conteneur.appendChild(dayContainer);
        });
    }

    // ─── TOAST NOTIFICATION (Sobre) ────────────────────────────────
    function showToast(message) {
        let toast = document.getElementById('toast-notification');
        if (!toast) {
            toast = document.createElement('div');
            toast.id = 'toast-notification';
            document.body.appendChild(toast);
        }
        toast.textContent = message;
        toast.classList.add('show');
        setTimeout(() => toast.classList.remove('show'), 3000);
    }

    // ─── Modal de remplacement (Avec Changer de Style) ────────────────────────────────
    function ensureModal() {
        let modal = document.getElementById('replaceModal');
        if (modal) return modal;
        modal = document.createElement('div');
        modal.id = 'replaceModal';
        modal.style.cssText = 'position:fixed;inset:0;background:rgba(28,28,28,0.55);display:none;align-items:center;justify-content:center;z-index:9999;padding:20px;';
        modal.innerHTML = `
        <div class="rm-card" style="background:#FAF8F5;width:100%;max-width:850px;max-height:85vh;overflow:hidden;border-radius:16px;box-shadow:0 20px 60px rgba(0,0,0,0.3);font-family:Lora,serif; display:flex; flex-direction:column;">
            <div style="padding: 24px 24px 0 24px;">
                <div class="rm-header" style="display:flex;justify-content:space-between;align-items:flex-start;gap:16px;margin-bottom:18px;">
                    <div>
                        <h3 style="font-family:'Cormorant Garamond',serif;font-size:24px;margin:0;color:#1C1C1C;">Remplacer l'activité</h3>
                        <p class="rm-helper" style="margin:8px 0 0;color:#6b5f57;font-size:14px;">Choisissez une alternative selon vos envies.</p>
                    </div>
                    <button id="rmClose" type="button" style="background:none;border:none;font-size:26px;cursor:pointer;color:#6b5f57;padding:0;">×</button>
                </div>
                
                <div style="display:flex; gap:10px; margin-bottom:20px;">
                    <button id="btnModeSimilaire" style="flex:1; padding:10px; border-radius:8px; border:1px solid #FF6F61; background:#FF6F61; color:white; cursor:pointer; font-weight:600;">Esprit similaire</button>
                    <button id="btnModeDifferent" style="flex:1; padding:10px; border-radius:8px; border:1px solid #D4C3B3; background:white; color:#6b5f57; cursor:pointer; font-weight:600;">Autre type d'activité</button>
                </div>
                
                <input id="rmSearch" type="text" placeholder="Rechercher manuellement…" style="width:100%;padding:12px 16px;border:1px solid #D4C3B3;border-radius:10px;margin-bottom:15px;outline:none;">
            </div>
            
            <div style="padding: 0 24px 20px 24px; overflow-y:auto; flex:1;">
                <div id="rmCarouselTitle" class="rm-rec-title" style="font-family: 'Montserrat', sans-serif; font-size: 11px; font-weight: 700; text-transform: uppercase; letter-spacing: 1.5px; color: var(--text-soft); margin-bottom:10px;">Suggestions dans le même esprit</div>
                <div id="rmCarousel" class="rm-carousel"></div>
            </div>
        </div>`;
        document.body.appendChild(modal);

        modal.querySelector('#rmSearch').addEventListener('focus', function() { this.style.borderColor = '#FF6F61'; });
        modal.querySelector('#rmSearch').addEventListener('blur', function() { this.style.borderColor = '#D4C3B3'; });
        
        modal.addEventListener('click', e => { if (e.target === modal) closeModal(); });
        modal.querySelector('#rmClose').addEventListener('click', closeModal);
        return modal;
    }

    function closeModal() {
        const m = document.getElementById('replaceModal');
        if (m) m.style.display = 'none';
    }

    function getSpinnerHtml(text) {
        return `<div style="display:flex; flex-direction:column; align-items:center; justify-content:center; padding:40px 0; width:100%; color:#6b5f57; grid-column: 1 / -1;">
            <svg width="34" height="34" viewBox="0 0 50 50" style="animation: spin 1s linear infinite; margin-bottom: 12px;"><circle cx="25" cy="25" r="20" fill="none" stroke="#FF6F61" stroke-width="5" stroke-dasharray="31.4 31.4" stroke-linecap="round"></circle></svg>
            <span style="font-weight:600; font-family:'Montserrat',sans-serif; font-size:12px;">${text}</span>
        </div>`;
    }

    function openReplaceModal(target) {
        _currentTarget = target;
        const modal = ensureModal();
        modal.style.display = 'flex';
        
        const input = modal.querySelector('#rmSearch');
        const carousel = modal.querySelector('#rmCarousel');
        const title = modal.querySelector('#rmCarouselTitle');
        const btnSim = modal.querySelector('#btnModeSimilaire');
        const btnDiff = modal.querySelector('#btnModeDifferent');

        input.value = '';

        const currentAct = dataVoyage[target.jourIdx]?.[target.moment]?.[target.slotIdx];
        const currentName = currentAct?.nom || currentAct?.name || currentAct?.label || '';

        const refreshRecommendations = async (mode) => {
            carousel.innerHTML = getSpinnerHtml("Analyse par l'IA en cours...");
            
            if (mode === 'similaire') {
                btnSim.style.background = '#FF6F61'; btnSim.style.color = 'white'; btnSim.style.borderColor = '#FF6F61';
                btnDiff.style.background = 'transparent'; btnDiff.style.color = '#6b5f57'; btnDiff.style.borderColor = '#D4C3B3';
                title.textContent = "Suggestions dans le même esprit";
            } else {
                btnDiff.style.background = '#FF6F61'; btnDiff.style.color = 'white'; btnDiff.style.borderColor = '#FF6F61';
                btnSim.style.background = 'transparent'; btnSim.style.color = '#6b5f57'; btnSim.style.borderColor = '#D4C3B3';
                title.textContent = "Suggestions pour changer de style";
            }

            if (currentName) {
                try {
                    const items = await loadRecommendations(currentName, mode);
                    renderGridCards(items, carousel);
                } catch (err) {
                    console.error(err);
                    carousel.innerHTML = '<p style="color:#C9473A; text-align:center; padding:20px; grid-column: 1 / -1;">Impossible de charger les suggestions. Essayez la recherche manuelle.</p>';
                }
            } else {
                carousel.innerHTML = '<p style="width:100%; text-align:center; grid-column: 1 / -1;">Activité de référence manquante.</p>';
            }
        };

        btnSim.onclick = () => refreshRecommendations('similaire');
        btnDiff.onclick = () => refreshRecommendations('different');
        refreshRecommendations('similaire');

        input.oninput = () => {
            clearTimeout(_searchTimer);
            if (!input.value.trim()) {
                refreshRecommendations('similaire'); 
                return;
            }
            
            title.textContent = "Résultats de recherche";
            carousel.innerHTML = getSpinnerHtml("Recherche en cours...");
            
            _searchTimer = setTimeout(() => {
                const val = input.value;
                loadSuggestions(val).then(items => {
                    if (!items || !items.length) {
                        carousel.innerHTML = `<p style="color:#6b5f57;font-style:italic;text-align:center;padding:20px 0; grid-column: 1 / -1;">Aucun résultat pour "<b>${escapeHtml(val)}</b>".</p>`;
                    } else {
                        renderGridCards(items, carousel); 
                    }
                });
            }, 400); 
        };
    }

    async function loadSuggestions(q) {
        const region = criteria?.region || '';
        const slug = (region && region !== 'all') ? region : '';
        const query = (q || '').trim();

        if (slug) {
            try { const r = await fetch(`/regions/${encodeURIComponent(slug)}/cards?limit=24` + (query ? `&q=${encodeURIComponent(query)}` : '')); if (r.ok) { const data = await r.json(); if (Array.isArray(data) && data.length) return data; } } catch (_) {}
        }
        if (query.length >= 2) {
            try { const r = await fetch(`/search?query=${encodeURIComponent(query)}&limit=24`); if (r.ok) { const data = await r.json(); if (Array.isArray(data)) return data; } } catch (_) {}
        }
        return [];
    }

    async function loadRecommendations(activityName, mode = 'similaire') {
        const r = await fetch(`/suggestions?activity=${encodeURIComponent(activityName)}&mode=${mode}`);
        if (!r.ok) throw new Error("Erreur réseau");
        const data = await r.json();
        return Array.isArray(data) ? data : [];
    }

    // AFFICHE LES RÉSULTATS (IA OU RECHERCHE) EN GRILLE TYPE "CATALOGUE"
    function renderGridCards(items, container) {
        const slicedItems = items.slice(0, 16);
        if (!slicedItems || !slicedItems.length) {
            container.innerHTML = '<div class="rm-empty" style="padding:20px; text-align:center; width:100%; grid-column: 1 / -1;">Aucun résultat trouvé.</div>';
            return;
        }

        container.innerHTML = slicedItems.map(item => {
            const id = escapeHtml(String(item.id || item._id || item.nom || ''));
            const name = escapeHtml(item.name || item.nom || 'Sans nom');
            const cats = escapeHtml(formatCategories(item.categories || item.types || item.category || ''));
            const img = escapeHtml(window.getActivityImage(item));
            
            // Distance brute en dessous de la catégorie (ex: 11.2 km)
            let distHtml = '';
            if (item.distance) {
                distHtml = `<div class="rm-card-dist">${escapeHtml(String(item.distance).replace(/km/i, 'km').trim())}</div>`;
            }

            return `
                <div class="rm-card-item" data-id="${id}">
                    <div class="rm-card-visual"><img src="${img}"></div>
                    <div class="rm-card-body">
                        <div class="rm-card-title">${name}</div>
                        <div class="rm-card-meta">${cats}</div>
                        ${distHtml}
                    </div>
                    <div class="rm-card-actions"><button type="button" class="rm-pick">Choisir</button></div>
                </div>`;
        }).join('');

        container.querySelectorAll('.rm-card-item').forEach(card => {
            card.querySelector('.rm-pick').onclick = (e) => {
                e.stopPropagation();
                const id = card.dataset.id;
                const selectedItem = slicedItems.find(x => String(x.id || x._id || x.nom) === id);
                if (selectedItem) applyReplacement(selectedItem);
            };
        });
    }

    function mongoToActivity(item) {
        return {
            nom: item.name || item.nom || 'Activité',
            ville: item.locality || item.ville || '',
            code_postal: item.code_postal || '',
            categories: item.categories || item.types || [],
            description: item.description || item.desc || item.summary || '',
            website: item.website || item.url || item.site || '',
            image: window.getActivityImage(item), 
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
        showToast('Activité remplacée.'); // Petit message discret
    }

    // ON LANCE LA MACHINE !
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

    function ensureSaveNameModal() {
        let modal = document.getElementById('saveNameModal');
        if (modal) return modal;
        modal = document.createElement('div');
        modal.id = 'saveNameModal';
        modal.style.cssText = 'position:fixed;inset:0;background:rgba(28,28,28,0.55);display:none;align-items:center;justify-content:center;z-index:9999;padding:20px;';
        modal.innerHTML = `
            <div class="save-name-card" style="background:#FAF8F5;max-width:500px;width:100%;border-radius:16px;padding:32px;box-shadow:0 20px 60px rgba(0,0,0,0.3);font-family:Lora,serif;">
                <h3 style="font-family:'Cormorant Garamond',serif;font-size:28px;margin:0 0 12px;color:#1C1C1C;">Nommer ce voyage</h3>
                <p style="margin:0 0 24px;color:#6b5f57;font-size:14px;line-height:1.6;">Donnez un nom à votre voyage pour mieux le retrouver.</p>
                <input id="saveNameInput" type="text" placeholder="Ex: Weekend à Paris, Escapade en montagne..."
                       style="width:100%;padding:12px 16px;border:1px solid #D4C3B3;border-radius:10px;background:#fff;font-family:Lora,serif;font-size:14px;box-sizing:border-box;margin-bottom:24px;outline:none;">
                <div style="display:flex;gap:12px;justify-content:flex-end;">
                    <button id="saveNameCancel" type="button" style="background:none;border:1px solid #D4C3B3;color:#6b5f57;padding:10px 20px;border-radius:6px;cursor:pointer;font-weight:600;font-family:Montserrat,sans-serif;font-size:12px;">Annuler</button>
                    <button id="saveNameConfirm" type="button" style="background:#FF6F61;border:none;color:#fff;padding:10px 24px;border-radius:6px;cursor:pointer;font-weight:600;font-family:Montserrat,sans-serif;font-size:12px;text-transform:uppercase;letter-spacing:0.5px;">Sauvegarder</button>
                </div>
            </div>`;
        document.body.appendChild(modal);
        modal.querySelector('#saveNameInput').addEventListener('focus', function() { this.style.borderColor = '#FF6F61'; });
        modal.querySelector('#saveNameInput').addEventListener('blur', function() { this.style.borderColor = '#D4C3B3'; });
        return modal;
    }

    function openSaveNameModal(defaultName) {
        return new Promise((resolve) => {
            const modal = ensureSaveNameModal();
            const input = modal.querySelector('#saveNameInput');
            input.value = defaultName || '';
            modal.style.display = 'flex';

            const onConfirm = () => { const name = (input.value || '').trim(); cleanup(); resolve(name || defaultName); };
            const onCancel = () => { cleanup(); resolve(null); };

            const cleanup = () => {
                modal.querySelector('#saveNameConfirm').removeEventListener('click', onConfirm);
                modal.querySelector('#saveNameCancel').removeEventListener('click', onCancel);
                input.removeEventListener('keypress', onKeypress);
                modal.style.display = 'none';
            };

            const onKeypress = (e) => { if (e.key === 'Enter') onConfirm(); if (e.key === 'Escape') onCancel(); };

            modal.querySelector('#saveNameConfirm').addEventListener('click', onConfirm);
            modal.querySelector('#saveNameCancel').addEventListener('click', onCancel);
            input.addEventListener('keypress', onKeypress);
            input.focus();
        });
    }

    if (saveBtn) {
        saveBtn.addEventListener('click', async () => {
            const locationLabel = criteria?.ville || '';
            const defaultName = locationLabel ? `Voyage à ${locationLabel} — ${new Date().toLocaleDateString('fr-FR')}` : `Voyage du ${new Date().toLocaleDateString('fr-FR')}`;

            const customName = await openSaveNameModal(defaultName);
            if (customName === null) return; 

            const finalName = (customName || defaultName);
            const nowIso = new Date().toISOString();
            
            let coverImg = '/static/img/travel.jpg';
            outerLoop: for (const day of dataVoyage) {
                for (const key of ['matin', 'midi', 'aprem', 'soir']) {
                    for (const act of asArray(day[key])) {
                        const img = window.getActivityImage(act);
                        if (img && !img.includes('default')) { coverImg = img; break outerLoop; }
                    }
                }
            }

            const payload = { name: finalName, location: locationLabel, cover: coverImg, createdAt: nowIso, updatedAt: nowIso, criteria, plan: dataVoyage };

            saveBtn.disabled = true;
            const originalText = saveBtn.textContent;
            saveBtn.innerHTML = `<svg width="16" height="16" viewBox="0 0 50 50" style="animation: spin 1s linear infinite; vertical-align: middle; margin-right: 8px; display: inline-block;"><circle cx="25" cy="25" r="20" fill="none" stroke="#fff" stroke-width="6" stroke-dasharray="31.4 31.4" stroke-linecap="round"></circle></svg> Sauvegarde...`;
            
            try {
                const res = await fetch('/journeys', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload) });
                let serverResp = null;
                try { serverResp = await res.json(); } catch (_) { }
                if (!res.ok) throw new Error((serverResp && (serverResp.message || serverResp.detail)) || `HTTP ${res.status}`);

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