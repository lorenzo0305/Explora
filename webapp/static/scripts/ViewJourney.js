const DEFAULT_COVER = "/static/img/no-image.jpg";
const JOURNEY_KEYS = ["wish_journeys_v1", "journeys"];
const LAST_ID_KEY = "wish_last_journey_id";

let _currentTarget = null;
let _searchTimer = null;
let _openDayIndex = null; // Mémorise quel jour est ouvert pour le laisser ouvert après une modification

// --- RÉSERVE D'IMAGES POUR LES JOURS ---
const DAY_FALLBACKS = [
    '/static/img/roussillon.jpg',
    '/static/img/canoe_occitanie.jpg',
    '/static/img/provence.jpg',
    '/static/img/semur_en_auxois.jpg',
    '/static/img/menton.jpg',
    '/static/img/autoir.jpg'
];

const THEMES = {
    "Nature": { icones: ['/static/img/nature1.jpg', '/static/img/nature2.jpg', '/static/img/nature3.jpg', '/static/img/nature4.jpg', '/static/img/nature5.jpg', '/static/img/nature6.jpg'] },
    "Gastronomie": { icones: ['/static/img/food1.jpg', '/static/img/food2.jpg', '/static/img/food3.jpg', '/static/img/food4.jpg', '/static/img/food5.jpg', '/static/img/food6.jpg'] },
    "Culture": { icones: ['/static/img/culture1.jpg', '/static/img/culture2.jpg', '/static/img/culture3.jpg', '/static/img/culture4.jpg', '/static/img/culture5.jpg', '/static/img/culture6.jpg'] },
    "Sport": { icones: ['/static/img/sport1.jpg', '/static/img/sport2.jpg', '/static/img/sport3.jpg', '/static/img/sport4.jpg', '/static/img/sport5.png', '/static/img/sport6.jpg'] },   
    "Détente": { icones: ['/static/img/detente1.jpg', '/static/img/detente2.jpg', '/static/img/detente3.jpg', '/static/img/detente4.jpg', '/static/img/detente55.jpg', '/static/img/detente6.jpg'] },
    "Shopping": { icones: ['/static/img/shopping1.jpg', '/static/img/shopping2.jpg', '/static/img/shopping3.jpg', '/static/img/shopping4.jpg', '/static/img/shopping5.jpg', '/static/img/shopping6.jpg'] }
};
const MIX = [0, 1, 2, 3, 4, 5, 2, 4, 0, 5, 1, 3, 4, 2, 5, 0, 3, 1, 5, 3, 1, 4, 2];

const escapeHtml = (s) => String(s ?? '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;').replace(/'/g, '&#39;');

function getCategoryFromActivity(activity) {
    let typeStr = activity?.type || activity?.category || (Array.isArray(activity?.categories) ? activity.categories[0] : null) || (Array.isArray(activity?.types) ? activity.types[0] : null) || "";
    typeStr = String(typeStr).toLowerCase();
    if (typeStr.includes('nature') || typeStr.includes('parc') || typeStr.includes('jardin')) return "Nature";
    if (typeStr.includes('gastronomie') || typeStr.includes('restaurant') || typeStr.includes('food')) return "Gastronomie";
    if (typeStr.includes('culture') || typeStr.includes('musée') || typeStr.includes('patrimoine')) return "Culture";
    if (typeStr.includes('sport') || typeStr.includes('loisir') || typeStr.includes('aventure')) return "Sport";
    if (typeStr.includes('détente') || typeStr.includes('spa') || typeStr.includes('bien-être')) return "Détente";
    if (typeStr.includes('shopping') || typeStr.includes('boutique') || typeStr.includes('magasin')) return "Shopping";
    return "Nature";
}

function stringToHash(str) {
    let hash = 0;
    for (let i = 0; i < str.length; i++) { hash = str.charCodeAt(i) + ((hash << 5) - hash); }
    return Math.abs(hash);
}
function getShuffledFallbacks(journeyId) {
    let seed = stringToHash(String(journeyId || "default"));
    let arr = [...DAY_FALLBACKS];
    for (let i = arr.length - 1; i > 0; i--) {
        seed = (seed * 9301 + 49297) % 233280;
        let rand = seed / 233280;
        let j = Math.floor(rand * (i + 1));
        [arr[i], arr[j]] = [arr[j], arr[i]];
    }
    return arr;
}

function loadAllLocalJourneys() {
    for (const k of JOURNEY_KEYS) { try { const a = JSON.parse(localStorage.getItem(k) || "[]"); if (a && a.length) return a; } catch { } }
    try { return JSON.parse(localStorage.getItem(JOURNEY_KEYS[0]) || "[]"); } catch { return []; }
}
function byLocalId(id) { return loadAllLocalJourneys().find(j => String(j.id) === String(id)) || null }
function getJourneyId() {
    const m = location.pathname.match(/\/journeys\/view\/([^\/\?#]+)/);
    if (m) return m[1];
    const q = new URLSearchParams(location.search).get("id");
    if (q) return q;
    const last = localStorage.getItem(LAST_ID_KEY);
    if (last) return last;
    const first = (loadAllLocalJourneys()[0] || {}).id;
    return first || "";
}

const A = (a) => Array.isArray(a) ? a : (a && typeof a === "object") ? Object.values(a) : [];
const SLOT_FR_EN = { matin: 'morning', midi: 'noon', aprem: 'afternoon', soir: 'evening' };

function slotsArrayToObj(slotsArr) {
    const obj = { morning: [], noon: [], afternoon: [], evening: [] };
    (Array.isArray(slotsArr) ? slotsArr : []).forEach(s => {
        const raw = String(s?.key || "").toLowerCase();
        const k = SLOT_FR_EN[raw] || raw;
        if (obj[k]) obj[k] = A(s.items);
    });
    return obj;
}
function normalizeSlots(s) {
    s = s || {};
    return {
        morning:   A(s.morning   ?? s.matin),
        noon:      A(s.noon      ?? s.midi),
        afternoon: A(s.afternoon ?? s.aprem),
        evening:   A(s.evening   ?? s.soir),
    };
}
function normalizeDayAny(d, i) {
    if (!d || typeof d !== "object") return { day: (i || 0) + 1, slots: { morning: [], noon: [], afternoon: [], evening: [] } };
    const day = (d.day != null) ? Number(d.day) : (d.jour != null ? Number(d.jour) : (i || 0) + 1);
    if (Array.isArray(d.slots)) return { day, slots: slotsArrayToObj(d.slots) };
    const src = (d.slots && typeof d.slots === "object" && !Array.isArray(d.slots)) ? d.slots : { morning: d.morning ?? d.matin, noon: d.noon ?? d.midi, afternoon: d.afternoon ?? d.aprem, evening: d.evening ?? d.soir };
    return { day, slots: normalizeSlots(src) };
}
function deriveDays(j) {
    if (Array.isArray(j?.plan)) return j.plan.map((d, i) => normalizeDayAny(d, i));
    let days = j?.days ?? j?.days_json; if (typeof days === "string") { try { days = JSON.parse(days); } catch { days = null; } }
    if (Array.isArray(days)) return days.map((d, i) => normalizeDayAny(d, i));
    const s = normalizeSlots(j?.slots || {}); const total = s.morning.length + s.noon.length + s.afternoon.length + s.evening.length;
    return total ? [{ day: 1, slots: s }] : [];
}

const pickFirstStr = (...vals) => vals.find(v => typeof v === "string" && v.trim()) || "";
const safeImg = (u) => u && (/^data:image\//i.test(u) || /^https?:\/\//i.test(u) || u.startsWith("/")) ? u : "";

function activityImage(it) {
    const u = pickFirstStr(it?.image, it?.photo, it?.picture, it?.thumbnail, it?.cover);
    return safeImg(u);
}

function randomDayImage(d, j) {
    const order = ['morning', 'noon', 'afternoon', 'evening']; const pool = [];
    for (const k of order) { 
        for (const it of (d.slots?.[k] || [])) { 
            const u = activityImage(it); 
            if (u && !u.includes('no-image') && !u.includes('appareil_photo')) pool.push(u); 
        } 
    }
    if (pool.length) return pool[0];
    const shuffled = getShuffledFallbacks(j?.id);
    const dayNumber = d.day || 1;
    return shuffled[(dayNumber - 1) % shuffled.length];
}

function coverFrom(j) {
    return '/static/img/travel.jpg';
}

function countActivities(j) {
    const days = deriveDays(j || {}); let n = 0;
    for (const d of days) { n += (d.slots?.morning?.length || 0) + (d.slots?.noon?.length || 0) + (d.slots?.afternoon?.length || 0) + (d.slots?.evening?.length || 0); }
    return n;
}

async function fetchServerJourney(id) { try { const r = await fetch('/journeys/' + encodeURIComponent(id)); if (r.ok) return await r.json(); } catch { } return null }
function mergeJourneys(a = {}, b = {}) {
    const newer = (new Date(a?.updatedAt || 0) >= new Date(b?.updatedAt || 0)) ? a : b; const older = (newer === a) ? b : a;
    const m = { ...older, ...newer }; const nd = deriveDays(newer), od = deriveDays(older);
    if (!nd.length && od.length) { if (older.plan) m.plan = older.plan; if (older.days) m.days = older.days; if (older.days_json) m.days_json = older.days_json; if (!m.slots && older.slots) m.slots = older.slots; }
    return m;
}

function setImgWithFallback(imgEl, url, j) {
    const fallbackImg = '/static/img/travel.jpg';
    const finalUrl = url || fallbackImg;
    imgEl.onerror = function () { if (imgEl.dataset.fallback !== "1") { imgEl.dataset.fallback = "1"; imgEl.src = fallbackImg; } };
    imgEl.src = finalUrl;
}

// ─── SAUVEGARDE MUTATION ────────────────────────────────────
function saveAndRerender(journey) {
    journey.updatedAt = new Date().toISOString();
    
    let localJourneys = loadAllLocalJourneys();
    const localIdx = localJourneys.findIndex(x => String(x.id) === String(journey.id));
    if (localIdx >= 0) localJourneys[localIdx] = journey;
    else localJourneys.unshift(journey);
    
    try { localStorage.setItem(JOURNEY_KEYS[0], JSON.stringify(localJourneys)); } catch(e){}

    fetch(`/journeys/${journey.id}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(journey)
    }).catch(e => console.warn("Erreur synchro serveur:", e));

    render(journey); // Recharge l'interface instantanément
}

// ─── RENDU PRINCIPAL ────────────────────────────────────────
// ─── RENDU PRINCIPAL ────────────────────────────────────────
function render(j) {
    localStorage.setItem(LAST_ID_KEY, String(j.id));
    document.getElementById('journeyTitle').textContent = String(j?.name || 'Voyage').replace(/\s*<br\s*\/?>\s*/gi, ' ').trim();
    
    const locText = j?.location || '';
    const locEl = document.getElementById('journeyLocation');
    if (!locText || locText === 'Mon Carnet Magazine' || locText === 'Ville, lieux...') { locEl.style.display = 'none'; } 
    else { locEl.textContent = locText; locEl.style.display = 'block'; }
    
    setImgWithFallback(document.getElementById('cover'), coverFrom(j), j);

    const days = deriveDays(j || {}); const aCount = countActivities(j || {});
    
    document.getElementById('summaryLine').innerHTML =
        `<span><b id="daysLabel">${days.length}</b> jour${days.length > 1 ? 's' : ''}</span>
         <span style="margin: 0 10px;">•</span>
         <span><b>${aCount}</b> activité${aCount > 1 ? 's' : ''}</span>`;

    // ---> RÉINTÉGRATION DU DÉPLACEMENT DU BOUTON RETOUR <---
    const backBtn = document.getElementById('backBtn');
    const sectionTitle = document.querySelector('.section-title');
    if (backBtn && sectionTitle && backBtn.parentNode !== sectionTitle) {
        sectionTitle.style.display = 'flex';
        sectionTitle.style.flexDirection = 'row';
        sectionTitle.style.justifyContent = 'space-between';
        sectionTitle.style.alignItems = 'flex-end';
        
        const titleWrap = document.createElement('div');
        titleWrap.style.display = 'flex';
        titleWrap.style.flexDirection = 'column'; 
        
        while (sectionTitle.childNodes.length > 0) {
            titleWrap.appendChild(sectionTitle.childNodes[0]);
        }
        sectionTitle.appendChild(titleWrap);
        sectionTitle.appendChild(backBtn);
    }
    // --------------------------------------------------------

    const list = document.getElementById('daysList'); list.innerHTML = '';
    let globalActivityIndex = 0; 
    
    for (let idx = 0; idx < days.length; idx++) {
        const d = days[idx];
        const dayContainer = document.createElement('div');
        dayContainer.className = 'day-column';
        
        const item = document.createElement('div'); item.className = 'day-item';
        const left = document.createElement('div'); left.className = 'day-left';
        const thumb = document.createElement('div'); thumb.className = 'day-thumb';
        thumb.style.backgroundImage = `url('${randomDayImage(d, j)}')`;

        const meta = document.createElement('div'); meta.className = 'day-meta';
        const name = document.createElement('div'); name.className = 'day-name'; name.textContent = 'Journée ' + d.day;

        const labels = [['morning', 'Matinée'], ['noon', 'Midi'], ['afternoon', 'Après-midi'], ['evening', 'Soirée']];
        const activities = [];
        const presentSlots = [];
        for (const [k, fr] of labels) {
            const arr = d.slots?.[k] || [];
            if (arr.length) presentSlots.push(fr);
            for (const a of arr) activities.push(a);
        }
        const total = activities.length;

        const desc = document.createElement('div'); desc.className = 'day-desc';
        if (total) {
            const firstNames = activities.slice(0, 3).map(a => (a?.name || a?.nom || a?.title || 'Activité').toString().trim()).filter(Boolean);
            const more = total - firstNames.length;
            desc.textContent = firstNames.join(' · ') + (more > 0 ? ` +${more}` : '');
        } else {
            desc.textContent = 'Journée libre — cliquez pour ajouter des activités';
            desc.classList.add('day-desc-empty');
        }

        meta.appendChild(name); meta.appendChild(desc);

        if (total || presentSlots.length) {
            const chips = document.createElement('div'); chips.className = 'day-chips';
            if (total) {
                const cnt = document.createElement('span'); cnt.className = 'day-chip day-chip-count'; cnt.textContent = `${total} activité${total > 1 ? 's' : ''}`; chips.appendChild(cnt);
            }
            presentSlots.forEach(s => { const c = document.createElement('span'); c.className = 'day-chip'; c.textContent = s; chips.appendChild(c); });
            meta.appendChild(chips);
        }

        left.appendChild(thumb); left.appendChild(meta);
        item.appendChild(left);

        const rightWrap = document.createElement('div');
        rightWrap.style.marginLeft = 'auto'; rightWrap.style.paddingLeft = '15px';
        const detailBtn = document.createElement('button'); detailBtn.className = 'btn-voir-detail'; detailBtn.textContent = 'Voir le détail';
        rightWrap.appendChild(detailBtn); item.appendChild(rightWrap);

        const detailsInline = document.createElement('div');
        detailsInline.className = 'day-details-inline';
        
        const slotsLabels = { morning: 'Matinée', noon: 'Midi', afternoon: 'Après-midi', evening: 'Soirée' };
        ['morning', 'noon', 'afternoon', 'evening'].forEach(slotKey => {
            const slotActivities = d.slots?.[slotKey] || [];
            if (slotActivities.length > 0) {
                const sec = document.createElement('div');
                sec.className = 'inline-slot-sec';
                sec.innerHTML = `<div class="inline-slot-title">${slotsLabels[slotKey]}</div>`;
                
                slotActivities.forEach((act, actIdx) => {
                    const actCard = document.createElement('div');
                    actCard.className = 'inline-act-card';
                    actCard.style.flexDirection = 'column'; 
                    actCard.style.alignItems = 'flex-start';

                    let img = activityImage(act) || '';
                    if (!img || img.includes('no-image') || img.includes('appareil_photo')) {
                        const category = getCategoryFromActivity(act);
                        const currentTheme = THEMES[category] || THEMES["Nature"];
                        img = currentTheme.icones[MIX[globalActivityIndex % MIX.length] % currentTheme.icones.length];
                    }
                    globalActivityIndex++;

                    let actDesc = act?.description || act?.shortDescription || "";
                    if (typeof actDesc === 'object') actDesc = actDesc.fr || actDesc.en || Object.values(actDesc)[0] || "";
                    if (!actDesc || actDesc.trim() === "") actDesc = "Aucune description détaillée n'est disponible pour cette activité.";

                    actCard.innerHTML = `
                        <div style="display: flex; align-items: center; gap: 12px; width: 100%;">
                            <img src="${img}" class="inline-act-thumb" onerror="this.src='/static/img/no-image.jpg'">
                            <div style="flex: 1;">
                                <div style="font-weight:700; font-size:14px; color: var(--text-main);">${act.name || act.nom || 'Activité'}</div>
                                <div style="font-size:12px; color: var(--text-soft);">${act.city || act.ville || act.locality || ''}</div>
                            </div>
                        </div>
                        <p style="font-size: 13px; color: var(--text-soft); line-height: 1.5; margin: 10px 0 0 0; width: 100%;">${actDesc}</p>
                        
                        <!-- LES NOUVEAUX BOUTONS D'ÉDITION -->
                        <div style="display:flex; gap:8px; margin-top:12px; width:100%; border-top: 1px dashed var(--border-light); padding-top: 10px;">
                            <button type="button" class="btn-act-replace" onmouseover="this.style.background='#F0EDE9'" onmouseout="this.style.background='transparent'" style="background:transparent; border:1px solid #D4C3B3; color:#6b5f57; padding:6px 12px; border-radius:4px; font-size:11px; font-family:'Montserrat', sans-serif; cursor:pointer; font-weight:600; flex:1; transition:all 0.2s;">↔ Remplacer</button>
                            <button type="button" class="btn-act-remove" onmouseover="this.style.background='#FF6F61'; this.style.color='white'" onmouseout="this.style.background='transparent'; this.style.color='#FF6F61'" style="background:transparent; border:1px solid #FF6F61; color:#FF6F61; padding:6px 12px; border-radius:4px; font-size:11px; font-family:'Montserrat', sans-serif; cursor:pointer; font-weight:600; flex:1; transition:all 0.2s;">✕ Retirer</button>
                        </div>
                    `;

                    actCard.querySelector('.btn-act-remove').addEventListener('click', (e) => {
                        e.stopPropagation();
                        if (!confirm('Retirer cette activité du programme ?')) return;
                        days[idx].slots[slotKey].splice(actIdx, 1);
                        j.plan = days; 
                        _openDayIndex = idx;
                        saveAndRerender(j);
                    });

                    actCard.querySelector('.btn-act-replace').addEventListener('click', (e) => {
                        e.stopPropagation();
                        _openDayIndex = idx;
                        openReplaceModal({ jourIdx: idx, moment: slotKey, slotIdx: actIdx, journey: j, allDays: days });
                    });

                    sec.appendChild(actCard);
                });
                detailsInline.appendChild(sec);
            }
        });

        const toggleAction = () => {
            if (total === 0) return; 
            const isOpen = detailsInline.classList.contains('show');
            if (isOpen) {
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
        
        if (_openDayIndex === idx && total > 0) {
            detailsInline.classList.add('show');
            item.classList.add('is-open');
            detailBtn.textContent = 'Fermer';
        }
        
        dayContainer.appendChild(item);
        dayContainer.appendChild(detailsInline);
        list.appendChild(dayContainer);
    }

    const notesKey = 'journey_notes_' + j.id;
    const ta = document.getElementById('privateNotes');
    ta.value = localStorage.getItem(notesKey) || '';

    ta.addEventListener('input', () => {
        localStorage.setItem(notesKey, ta.value);
        const btn = document.getElementById('saveNotesBtn');
        if (btn && btn.textContent === 'Enregistré') {
            btn.textContent = 'Sauvegarder les notes';
        }
    });

    if (!document.getElementById('saveNotesBtn')) {
        const btn = document.createElement('button');
        btn.id = 'saveNotesBtn';
        btn.className = 'save-notes-btn';
        btn.textContent = 'Sauvegarder les notes';
        btn.addEventListener('click', () => {
            localStorage.setItem(notesKey, ta.value);
            ta.blur();
            btn.textContent = 'Enregistré'; 
        });
        ta.parentNode.insertBefore(btn, ta.nextSibling);
    }
}

// ─── LE CERVEAU IA (MODAL DE REMPLACEMENT) ─────────────────
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
            <h3 style="font-family:'Cormorant Garamond',serif;font-size:24px;margin:0;color:#1C1C1C;">Remplacer l'activité</h3>
            <p class="rm-helper" style="margin:8px 0 0;color:#6b5f57;font-size:14px;">Choisissez une alternative selon vos envies.</p>
        </div>
        <button id="rmClose" type="button" style="background:none;border:none;font-size:26px;cursor:pointer;color:#6b5f57;">×</button>
    </div>
    <div style="display:flex; gap:10px; margin-bottom:20px;">
        <button id="btnModeSimilaire" style="flex:1; padding:10px; border-radius:8px; border:1px solid #FF6F61; background:#FF6F61; color:white; cursor:pointer; font-weight:600;">Esprit similaire</button>
        <button id="btnModeDifferent" style="flex:1; padding:10px; border-radius:8px; border:1px solid #D4C3B3; background:white; color:#6b5f57; cursor:pointer; font-weight:600;">Changer de style</button>
    </div>
    <input id="rmSearch" type="text" placeholder="Rechercher manuellement…"
           style="width:100%;padding:12px 16px;border:1px solid #D4C3B3;border-radius:10px;margin-bottom:22px;">
    <div class="rm-recommendations">
        <div id="rmCarouselTitle" class="rm-rec-title" style="font-family: 'Montserrat', sans-serif; font-size: 11px; font-weight: 700; text-transform: uppercase; letter-spacing: 1.5px; color: var(--text-soft); margin-bottom:10px;">Suggestions similaires</div>
        <div id="rmCarousel" class="rm-carousel"></div>
    </div>
    <div class="rm-search-results" style="margin-top:30px;">
        <div class="rm-rec-title" style="font-family: 'Montserrat', sans-serif; font-size: 11px; font-weight: 700; text-transform: uppercase; letter-spacing: 1.5px; color: var(--text-soft); margin-bottom:10px;">Tous les résultats</div>
        <div id="rmResults" class="rm-results"></div>
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

function openReplaceModal(target) {
    _currentTarget = target;
    const modal = ensureModal();
    modal.style.display = 'flex';
    
    const input = modal.querySelector('#rmSearch');
    const results = modal.querySelector('#rmResults');
    const carousel = modal.querySelector('#rmCarousel');
    const title = modal.querySelector('#rmCarouselTitle');
    const btnSim = modal.querySelector('#btnModeSimilaire');
    const btnDiff = modal.querySelector('#btnModeDifferent');

    input.value = '';
    results.innerHTML = ''; 

    const currentAct = target.allDays[target.jourIdx].slots[target.moment][target.slotIdx];
    const currentName = currentAct?.nom || currentAct?.name || currentAct?.label || '';

    const refreshRecommendations = async (mode) => {
        carousel.innerHTML = '<p style="color:#6b5f57;font-style:italic;text-align:center;padding:20px 0;width:100%;">Analyse de vos préférences…</p>';
        
        if (mode === 'similaire') {
            btnSim.style.background = '#FF6F61'; btnSim.style.color = 'white';
            btnDiff.style.background = 'transparent'; btnDiff.style.color = '#6b5f57';
            title.textContent = "Suggestions dans le même esprit";
        } else {
            btnDiff.style.background = '#FF6F61'; btnDiff.style.color = 'white';
            btnSim.style.background = 'transparent'; btnSim.style.color = '#6b5f57';
            title.textContent = "Suggestions différentes";
        }

        if (currentName) {
            const items = await loadRecommendations(currentName, mode);
            renderRecommendationCards(items, carousel);
        } else {
            carousel.innerHTML = '<p style="width:100%; text-align:center;">Activité de référence manquante.</p>';
        }
    };

    btnSim.onclick = () => refreshRecommendations('similaire');
    btnDiff.onclick = () => refreshRecommendations('different');

    refreshRecommendations('similaire');
    loadSuggestions('').then(items => renderModalResults(items, results));

    input.oninput = () => {
        clearTimeout(_searchTimer);
        _searchTimer = setTimeout(() => {
            loadSuggestions(input.value).then(items => renderModalResults(items, results));
        }, 280);
    };
}

async function loadSuggestions(q) {
    // Si c'est un voyage IA, on cherche par la "location" de base. Sinon fallback classique
    const critRegion = _currentTarget?.journey?.criteria?.region || _currentTarget?.journey?.location || '';
    const slug = (critRegion && critRegion !== 'all' && critRegion !== 'Mon Carnet Magazine') ? critRegion : '';
    const query = (q || '').trim();

    if (slug) {
        try {
            const url = `/regions/${encodeURIComponent(slug)}/cards?limit=24` + (query ? `&q=${encodeURIComponent(query)}` : '');
            const r = await fetch(url);
            if (r.ok) {
                const data = await r.json();
                if (Array.isArray(data) && data.length) return data;
            }
        } catch (_) {}
    }

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

async function loadRecommendations(activityName, mode = 'similaire') {
    try {
        const url = `/suggestions?activity=${encodeURIComponent(activityName)}&mode=${mode}`;
        const r = await fetch(url);
        if (r.ok) {
            const data = await r.json();
            return Array.isArray(data) ? data : [];
        }
    } catch (err) {
        console.error("Erreur de recommandations :", err);
    }
    return [];
}

function renderRecommendationCards(items, container) {
    if (!items || !items.length) {
        container.innerHTML = '<div class="rm-empty" style="padding:20px; text-align:center; width:100%; font-style:italic;">Aucune recommandation disponible.</div>';
        return;
    }

    container.style.display = 'grid';
    container.style.gridTemplateColumns = 'repeat(auto-fill, minmax(160px, 1fr))';
    container.style.gap = '16px';
    container.style.padding = '10px 0';

    container.innerHTML = items.map(item => {
        const id = escapeHtml(String(item.id || item._id || item.nom || ''));
        const name = escapeHtml(item.name || item.nom || 'Sans nom');
        const subtitle = escapeHtml(item.locality || item.region || (item.categories && item.categories[0]) || '');
        const img = escapeHtml((item.image && /^https?:/.test(item.image)) ? item.image : '/static/img/no-image.jpg');
        const stat = escapeHtml(item.match || item.type || '');
        const distance = escapeHtml(item.distance || '');

        return `
            <div class="rm-card-item" data-id="${id}" 
                 style="background:#fff; border:1px solid #D4C3B3; border-radius:12px; overflow:hidden; display:flex; flex-direction:column; transition: all 0.3s ease; box-shadow:0 4px 10px rgba(0,0,0,0.03);">
                <div class="rm-card-visual" style="height:120px; overflow:hidden;">
                    <img src="${img}" alt="${name}" style="width:100%; height:100%; object-fit:cover;" onerror="this.src='/static/img/no-image.jpg'" />
                </div>
                <div class="rm-card-body" style="padding:12px; flex:1; display:flex; flex-direction:column; gap:4px;">
                    <div class="rm-card-title" style="font-family:'Cormorant Garamond',serif; font-size:16px; font-weight:700; color:#1C1C1C; line-height:1.2;">${name}</div>
                    <div class="rm-card-meta" style="font-size:12px; color:#6b5f57;">${subtitle}</div>
                    <div class="rm-card-stats" style="font-size:11px; font-weight:600; color:#FF6F61; margin-top:4px;">
                        <span>${stat}</span> <span style="margin-left:8px; color:#6b5f57;">${distance}</span>
                    </div>
                </div>
                <div class="rm-card-actions" style="padding:10px; border-top:1px solid #F0EDE9;">
                    <button type="button" class="rm-pick" style="width:100%; background:#FF6F61; color:#fff; border:none; padding:8px; border-radius:20px; cursor:pointer; font-weight:700; font-family:'Montserrat', sans-serif; font-size:11px; text-transform:uppercase;">Choisir</button>
                </div>
            </div>`;
    }).join('');

    container.querySelectorAll('.rm-card-item').forEach(card => {
        card.querySelector('.rm-pick').onclick = (e) => {
            e.stopPropagation();
            const id = card.dataset.id;
            const selectedItem = items.find(x => String(x.id || x._id || x.nom) === id);
            if (selectedItem) applyReplacement(selectedItem);
        };
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
                 style="display:flex;gap:12px;align-items:center;padding:12px;border:1px solid #D4C3B3;border-radius:10px;background:#fff;cursor:pointer;transition:border-color .15s,transform .15s; margin-bottom:8px;">
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
    return {
        nom: item.name || item.nom || 'Activité',
        name: item.name || item.nom || 'Activité',
        ville: item.locality || item.ville || '',
        locality: item.locality || item.ville || '',
        categories: item.categories || item.types || [],
        description: item.description || item.desc || item.summary || '',
        image: item.image || '',
        id: item.id || item._id || ''
    };
}

function applyReplacement(item) {
    if (!_currentTarget) return;
    const { jourIdx, moment, slotIdx, journey, allDays } = _currentTarget;
    
    // Remplace la donnée par la nouvelle activité formatée
    allDays[jourIdx].slots[moment][slotIdx] = mongoToActivity(item);
    
    // Met à jour l'objet voyage
    journey.plan = allDays;
    
    closeModal();
    saveAndRerender(journey);
}

document.getElementById('backBtn').addEventListener('click', () => history.back());

(async function init() {
    const id = getJourneyId();
    if (!id) { document.getElementById('journeyTitle').textContent = 'Voyage introuvable'; return; }
    const local = byLocalId(id); const server = await fetchServerJourney(id);
    const journey = (local && server) ? mergeJourneys(local, server) : (server || local);
    if (!journey) { document.getElementById('journeyTitle').textContent = 'Voyage introuvable'; return; }
    render(journey);
})();