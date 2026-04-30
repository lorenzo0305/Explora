const DEFAULT_COVER = "/static/img/no-image.jpg";
const JOURNEY_KEYS = ["wish_journeys_v1", "journeys"];
const LAST_ID_KEY = "wish_last_journey_id";

// --- RÉSERVE D'IMAGES POUR LES JOURS ---
const DAY_FALLBACKS = [
    '/static/img/roussillon.jpg',
    '/static/img/canoe_occitanie.jpg',
    '/static/img/provence.jpg',
    '/static/img/semur_en_auxois.jpg',
    '/static/img/menton.jpg',
    '/static/img/autoir.jpg'
];

// --- BIBLIOTHÈQUE PREMIUM POUR LES ACTIVITÉS (DICTIONNAIRES) ---
const THEMES = {
    "Nature": { icones: ['/static/img/nature1.jpg', '/static/img/nature2.jpg', '/static/img/nature3.jpg', '/static/img/nature4.jpg', '/static/img/nature5.jpg', '/static/img/nature6.jpg'] },
    "Gastronomie": { icones: ['/static/img/food1.jpg', '/static/img/food2.jpg', '/static/img/food3.jpg', '/static/img/food4.jpg', '/static/img/food5.jpg', '/static/img/food6.jpg'] },
    "Culture": { icones: ['/static/img/culture1.jpg', '/static/img/culture2.jpg', '/static/img/culture3.jpg', '/static/img/culture4.jpg', '/static/img/culture5.jpg', '/static/img/culture6.jpg'] },
    "Sport": { icones: ['/static/img/sport1.jpg', '/static/img/sport2.jpg', '/static/img/sport3.jpg', '/static/img/sport4.jpg', '/static/img/sport5.png', '/static/img/sport6.jpg'] },   
    "Détente": { icones: ['/static/img/detente1.jpg', '/static/img/detente2.jpg', '/static/img/detente3.jpg', '/static/img/detente4.jpg', '/static/img/detente55.jpg', '/static/img/detente6.jpg'] },
    "Shopping": { icones: ['/static/img/shopping1.jpg', '/static/img/shopping2.jpg', '/static/img/shopping3.jpg', '/static/img/shopping4.jpg', '/static/img/shopping5.jpg', '/static/img/shopping6.jpg'] }
};
const MIX = [0, 1, 2, 3, 4, 5, 2, 4, 0, 5, 1, 3, 4, 2, 5, 0, 3, 1, 5, 3, 1, 4, 2];

function getCategoryFromActivity(activity) {
    let typeStr = activity?.type || activity?.category || (Array.isArray(activity?.categories) ? activity.categories[0] : null) || (Array.isArray(activity?.types) ? activity.types[0] : null) || "";
    typeStr = String(typeStr).toLowerCase();
    if (typeStr.includes('nature') || typeStr.includes('parc') || typeStr.includes('jardin')) return "Nature";
    if (typeStr.includes('gastronomie') || typeStr.includes('restaurant') || typeStr.includes('food')) return "Gastronomie";
    if (typeStr.includes('culture') || typeStr.includes('musée') || typeStr.includes('patrimoine')) return "Culture";
    if (typeStr.includes('sport') || typeStr.includes('loisir') || typeStr.includes('aventure')) return "Sport";
    if (typeStr.includes('détente') || typeStr.includes('spa') || typeStr.includes('bien-être')) return "Détente";
    if (typeStr.includes('shopping') || typeStr.includes('boutique') || typeStr.includes('magasin')) return "Shopping";
    return "Nature"; // Par défaut
}

// Outils de hachage et mélange
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
    const src = (d.slots && typeof d.slots === "object" && !Array.isArray(d.slots))
        ? d.slots
        : { morning: d.morning ?? d.matin, noon: d.noon ?? d.midi, afternoon: d.afternoon ?? d.aprem, evening: d.evening ?? d.soir };
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
    if (j?.cover && !j.cover.includes('no-image')) return safeImg(j.cover);
    const days = deriveDays(j || {});
    for (const d of days) {
        for (const k of ['morning', 'noon', 'afternoon', 'evening']) {
            const u = activityImage((d.slots?.[k] || [])[0]); 
            if (u && !u.includes('no-image') && !u.includes('appareil_photo')) return u;
        }
    }
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

function render(j) {
    localStorage.setItem(LAST_ID_KEY, String(j.id));

    // Titre propre
    document.getElementById('journeyTitle').textContent = String(j?.name || 'Voyage').replace(/\s*<br\s*\/?>\s*/gi, ' ').trim();
    
    const locText = j?.location || '';
    const locEl = document.getElementById('journeyLocation');
    if (!locText || locText === 'Mon Carnet Magazine' || locText === 'Ville, lieux...') {
        locEl.style.display = 'none';
    } else {
        locEl.textContent = locText;
        locEl.style.display = 'block';
    }
    
    setImgWithFallback(document.getElementById('cover'), coverFrom(j), j);

    const days = deriveDays(j || {}); const aCount = countActivities(j || {});
    
    document.getElementById('summaryLine').innerHTML =
        `<span><b id="daysLabel">${days.length}</b> jour${days.length > 1 ? 's' : ''}</span>
         <span style="margin: 0 10px;">•</span>
         <span><b>${aCount}</b> activité${aCount > 1 ? 's' : ''}</span>`;

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

    const list = document.getElementById('daysList'); list.innerHTML = '';
    
    // Le fameux compteur pour mélanger les images Premium
    let globalActivityIndex = 0; 
    
    for (let idx = 0; idx < days.length; idx++) {
        const d = days[idx];
        
        const dayContainer = document.createElement('div');
        dayContainer.className = 'day-column'; // Kanban Layout !
        
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
            const firstNames = activities
                .slice(0, 3)
                .map(a => (a?.name || a?.nom || a?.title || 'Activité').toString().trim())
                .filter(Boolean);
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
                const cnt = document.createElement('span');
                cnt.className = 'day-chip day-chip-count';
                cnt.textContent = `${total} activité${total > 1 ? 's' : ''}`;
                chips.appendChild(cnt);
            }
            presentSlots.forEach(s => {
                const c = document.createElement('span');
                c.className = 'day-chip';
                c.textContent = s;
                chips.appendChild(c);
            });
            meta.appendChild(chips);
        }

        left.appendChild(thumb); left.appendChild(meta);
        item.appendChild(left);

        const rightWrap = document.createElement('div');
        rightWrap.style.marginLeft = 'auto'; 
        rightWrap.style.paddingLeft = '15px';
        
        const detailBtn = document.createElement('button');
        detailBtn.className = 'btn-voir-detail';
        detailBtn.textContent = 'Voir le détail';
        
        rightWrap.appendChild(detailBtn);
        item.appendChild(rightWrap);

        const detailsInline = document.createElement('div');
        detailsInline.className = 'day-details-inline';
        
        const slotsLabels = { morning: 'Matinée', noon: 'Midi', afternoon: 'Après-midi', evening: 'Soirée' };
        ['morning', 'noon', 'afternoon', 'evening'].forEach(slotKey => {
            const slotActivities = d.slots?.[slotKey] || [];
            if (slotActivities.length > 0) {
                const sec = document.createElement('div');
                sec.className = 'inline-slot-sec';
                sec.innerHTML = `<div class="inline-slot-title">${slotsLabels[slotKey]}</div>`;
                
                slotActivities.forEach(act => {
                    const actCard = document.createElement('div');
                    actCard.className = 'inline-act-card';
                    actCard.style.flexDirection = 'column'; 
                    actCard.style.alignItems = 'flex-start';

                    let img = activityImage(act) || '';
                    
                    // --- LA MAGIE DES IMAGES PREMIUM APPLIQUÉE AUX ACTIVITÉS ---
                    if (!img || img.includes('no-image') || img.includes('appareil_photo')) {
                        const category = getCategoryFromActivity(act);
                        const currentTheme = THEMES[category] || THEMES["Nature"];
                        const variantIndex = MIX[globalActivityIndex % MIX.length] % currentTheme.icones.length;
                        img = currentTheme.icones[variantIndex];
                    }
                    globalActivityIndex++; // On avance dans la séquence
                    // -----------------------------------------------------------

                    let desc = act?.description || act?.shortDescription || "";
                    if (typeof desc === 'object') desc = desc.fr || desc.en || Object.values(desc)[0] || "";
                    if (!desc || desc.trim() === "") desc = "Aucune description détaillée n'est disponible pour cette activité.";

                    actCard.innerHTML = `
                        <div style="display: flex; align-items: center; gap: 12px; width: 100%;">
                            <img src="${img}" class="inline-act-thumb" onerror="this.src='/static/img/no-image.jpg'">
                            <div style="flex: 1;">
                                <div style="font-weight:700; font-size:14px; color: var(--text-main);">${act.name || act.nom || 'Activité'}</div>
                                <div style="font-size:12px; color: var(--text-soft);">${act.city || act.ville || act.locality || ''}</div>
                            </div>
                        </div>
                        <p style="font-size: 13px; color: var(--text-soft); line-height: 1.5; margin: 10px 0 0 0; width: 100%;">
                            ${desc}
                        </p>
                    `;

                    sec.appendChild(actCard);
                });
                detailsInline.appendChild(sec);
            }
        });

        // Gestion Indépendante de l'ouverture
        const toggleAction = () => {
            if (total === 0) return; 
            
            const isOpen = detailsInline.classList.contains('show');
            
            if (isOpen) {
                detailsInline.classList.remove('show');
                item.classList.remove('is-open');
                detailBtn.textContent = 'Voir le détail';
            } else {
                detailsInline.classList.add('show');
                item.classList.add('is-open');
                detailBtn.textContent = 'Fermer';
            }
        };

        item.addEventListener('click', toggleAction);
        
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

document.getElementById('backBtn').addEventListener('click', () => history.back());

(async function init() {
    const id = getJourneyId();
    if (!id) { document.getElementById('journeyTitle').textContent = 'Voyage introuvable'; return; }
    const local = byLocalId(id); const server = await fetchServerJourney(id);
    const journey = (local && server) ? mergeJourneys(local, server) : (server || local);
    if (!journey) { document.getElementById('journeyTitle').textContent = 'Voyage introuvable'; return; }
    render(journey);
})();