// --- /static/scripts/VoyagesSauvegarde.js ---

if (!document.getElementById('explora-animations')) {
    const style = document.createElement('style');
    style.id = 'explora-animations';
    style.innerHTML = `@keyframes spin { 100% { transform: rotate(360deg); } }`;
    document.head.appendChild(style);
}

const DEFAULT_COVER = "/static/img/travel.jpg";
const JOURNEY_KEYS = ["wish_journeys_v1", "journeys"];
const LAST_ID_KEY = "wish_last_journey_id";

// --- Variables globales de l'état du voyage ---
let _currentTarget = null;
let _searchTimer = null;
let _openDayIndex = null; 
let _hasUnsavedChanges = false;
let _currentJourney = null;

const DAY_FALLBACKS = [
    '/static/img/roussillon.jpg', '/static/img/canoe_occitanie.jpg', 
    '/static/img/provence.jpg', '/static/img/semur_en_auxois.jpg', 
    '/static/img/menton.jpg', '/static/img/autoir.jpg'
];

function getSafeImage(item) {
    if (!item) return DEFAULT_COVER;
    if (typeof window !== 'undefined' && typeof window.getActivityImage === 'function') {
        try { return window.getActivityImage(item); } catch(e) { console.warn("Erreur dictionnaire:", e); }
    }
    return item.image || item.photo || item.cover || DEFAULT_COVER;
}

const escapeHtml = (s) => String(s ?? '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;').replace(/'/g, '&#39;');

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
    const src = (d.slots && typeof d.slots === "object" && !Array.isArray(d.slots)) ? d.slots : d;
    return { day, slots: normalizeSlots(src) };
}

function deriveDays(j) {
    if (Array.isArray(j?.plan)) return j.plan.map((d, i) => normalizeDayAny(d, i));
    let days = j?.days ?? j?.days_json; if (typeof days === "string") { try { days = JSON.parse(days); } catch { days = null; } }
    if (Array.isArray(days)) return days.map((d, i) => normalizeDayAny(d, i));
    const s = normalizeSlots(j?.slots || {}); const total = s.morning.length + s.noon.length + s.afternoon.length + s.evening.length;
    return total ? [{ day: 1, slots: s }] : [];
}

function randomDayImage(d, j) {
    const order = ['morning', 'noon', 'afternoon', 'evening']; const pool = [];
    for (const k of order) { 
        for (const it of (d.slots?.[k] || [])) { 
            const u = getSafeImage(it); 
            if (u && !u.includes('no-image') && !u.includes('appareil_photo')) pool.push(u); 
        } 
    }
    if (pool.length) return pool[0];
    const shuffled = getShuffledFallbacks(j?.id);
    const dayNumber = d.day || 1;
    return shuffled[(dayNumber - 1) % shuffled.length];
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

// ─── GESTION DES SAUVEGARDES MANUELLES ────────────────────────────────
function saveJourneyToDB(journey) {
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
    
    _hasUnsavedChanges = false;
    updateFloatingSaveBtn();
}

function applyLocalChange(journey) {
    _hasUnsavedChanges = true;
    _currentJourney = journey;
    render(journey);
    updateFloatingSaveBtn();
}

function updateFloatingSaveBtn() {
    let fab = document.getElementById('floatingSaveBtn');
    if (!fab) {
        fab = document.createElement('button');
        fab.id = 'floatingSaveBtn';
        fab.textContent = 'Sauvegarder les modifications';
        fab.style.cssText = 'position:fixed;bottom:30px;right:30px;background:#1C1C1C;color:#fff;border:none;padding:15px 25px;border-radius:50px;font-family:Montserrat,sans-serif;font-size:12px;font-weight:700;text-transform:uppercase;box-shadow:0 10px 30px rgba(0,0,0,0.2);cursor:pointer;z-index:9000;transition:all 0.3s;display:none;letter-spacing:1px;';
        fab.onmouseover = () => { fab.style.background = '#FF6F61'; fab.style.transform = 'translateY(-3px)'; };
        fab.onmouseout = () => { fab.style.background = '#1C1C1C'; fab.style.transform = 'translateY(0)'; };
        
        fab.addEventListener('click', () => {
            saveJourneyToDB(_currentJourney);
            showToast("Modifications sauvegardées avec succès !");
        });
        document.body.appendChild(fab);
    }
    fab.style.display = _hasUnsavedChanges ? 'block' : 'none';
}

function showToast(message) {
    let toast = document.getElementById('toast-notification');
    if (!toast) {
        toast = document.createElement('div');
        toast.id = 'toast-notification';
        document.body.appendChild(toast);
    }
    toast.textContent = message;
    toast.classList.add('show');
    setTimeout(() => toast.classList.remove('show'), 3500);
}

// ─── BOÎTES DE DIALOGUES (Custom) ────────────────────────────────
function showCustomConfirm(title, message, onConfirm) {
    let modal = document.getElementById('customConfirmModal');
    if (!modal) {
        modal = document.createElement('div');
        modal.id = 'customConfirmModal';
        modal.style.cssText = 'position:fixed;inset:0;background:rgba(28,28,28,0.6);display:none;align-items:center;justify-content:center;z-index:9999;padding:20px;';
        modal.innerHTML = `
            <div style="background:#FAF8F5;max-width:400px;width:100%;border-radius:16px;padding:32px;box-shadow:0 20px 60px rgba(0,0,0,0.3);text-align:center;position:relative;">
                <button id="ccClose" type="button" style="position:absolute;top:16px;right:16px;background:none;border:none;font-size:24px;cursor:pointer;color:#6b5f57;">×</button>
                <div style="width:50px;height:50px;border-radius:50%;background:rgba(255,111,97,0.1);color:#FF6F61;display:flex;align-items:center;justify-content:center;margin:0 auto 16px;">
                    <svg viewBox="0 0 24 24" width="24" height="24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="3 6 5 6 21 6"></polyline><path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6m3 0V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2"></path></svg>
                </div>
                <h3 id="ccTitle" style="font-family:'Cormorant Garamond',serif;font-size:24px;margin:0 0 8px;color:#1C1C1C;">Titre</h3>
                <p id="ccMessage" style="margin:0 0 24px;color:#6b5f57;font-size:15px;line-height:1.5;font-family:'Lora',serif;">Message</p>
                <div style="display:flex;gap:12px;justify-content:center;">
                    <button id="ccCancel" type="button" style="background:none;border:1px solid #D4C3B3;color:#6b5f57;padding:10px 24px;border-radius:8px;cursor:pointer;font-weight:700;font-family:'Montserrat',sans-serif;font-size:11px;text-transform:uppercase;letter-spacing:0.5px;transition:background 0.2s;">Annuler</button>
                    <button id="ccConfirm" type="button" style="background:#FF6F61;border:none;color:#fff;padding:10px 24px;border-radius:8px;cursor:pointer;font-weight:700;font-family:'Montserrat',sans-serif;font-size:11px;text-transform:uppercase;letter-spacing:0.5px;transition:background 0.2s;">Retirer</button>
                </div>
            </div>
        `;
        document.body.appendChild(modal);

        modal.querySelector('#ccCancel').addEventListener('mouseover', function() { this.style.background = '#F0EDE9'; });
        modal.querySelector('#ccCancel').addEventListener('mouseout', function() { this.style.background = 'none'; });
        modal.querySelector('#ccConfirm').addEventListener('mouseover', function() { this.style.background = '#E85A4D'; });
        modal.querySelector('#ccConfirm').addEventListener('mouseout', function() { this.style.background = '#FF6F61'; });
    }

    modal.querySelector('#ccTitle').textContent = title;
    modal.querySelector('#ccMessage').textContent = message;
    modal.style.display = 'flex';

    const closeIt = () => { modal.style.display = 'none'; cleanup(); };
    const confirmIt = () => { closeIt(); if(onConfirm) onConfirm(); };

    const btnClose = modal.querySelector('#ccClose');
    const btnCancel = modal.querySelector('#ccCancel');
    const btnConfirm = modal.querySelector('#ccConfirm');

    const cleanup = () => {
        btnClose.removeEventListener('click', closeIt);
        btnCancel.removeEventListener('click', closeIt);
        btnConfirm.removeEventListener('click', confirmIt);
    };

    btnClose.addEventListener('click', closeIt);
    btnCancel.addEventListener('click', closeIt);
    btnConfirm.addEventListener('click', confirmIt);
}

function showUnsavedConfirm(onSave, onDiscard) {
    let modal = document.getElementById('unsavedConfirmModal');
    if (!modal) {
        modal = document.createElement('div');
        modal.id = 'unsavedConfirmModal';
        modal.style.cssText = 'position:fixed;inset:0;background:rgba(28,28,28,0.6);display:none;align-items:center;justify-content:center;z-index:9999;padding:20px;';
        modal.innerHTML = `
            <div style="background:#FAF8F5;max-width:450px;width:100%;border-radius:16px;padding:32px;box-shadow:0 20px 60px rgba(0,0,0,0.3);text-align:center;position:relative;">
                <button id="ucClose" type="button" style="position:absolute;top:16px;right:16px;background:none;border:none;font-size:24px;cursor:pointer;color:#6b5f57;">×</button>
                <div style="width:50px;height:50px;border-radius:50%;background:rgba(255,111,97,0.1);color:#FF6F61;display:flex;align-items:center;justify-content:center;margin:0 auto 16px;">
                    <svg viewBox="0 0 24 24" width="24" height="24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M19 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h11l5 5v11a2 2 0 0 1-2 2z"></path><polyline points="17 21 17 13 7 13 7 21"></polyline><polyline points="7 3 7 8 15 8"></polyline></svg>
                </div>
                <h3 style="font-family:'Cormorant Garamond',serif;font-size:24px;margin:0 0 8px;color:#1C1C1C;">Sauvegarder les modifications ?</h3>
                <p style="margin:0 0 24px;color:#6b5f57;font-size:15px;line-height:1.5;font-family:'Lora',serif;">Vous avez effectué des changements sur ce voyage. Voulez-vous les conserver avant de quitter ?</p>
                <div style="display:flex;gap:12px;justify-content:center;flex-wrap:wrap;">
                    <button id="ucDiscard" type="button" style="background:none;border:1px solid #D4C3B3;color:#6b5f57;padding:10px 20px;border-radius:8px;cursor:pointer;font-weight:700;font-family:'Montserrat',sans-serif;font-size:11px;text-transform:uppercase;letter-spacing:0.5px;transition:all 0.2s;">Quitter sans sauvegarder</button>
                    <button id="ucSave" type="button" style="background:#FF6F61;border:none;color:#fff;padding:10px 20px;border-radius:8px;cursor:pointer;font-weight:700;font-family:'Montserrat',sans-serif;font-size:11px;text-transform:uppercase;letter-spacing:0.5px;transition:all 0.2s;">Oui, sauvegarder</button>
                </div>
            </div>
        `;
        document.body.appendChild(modal);

        modal.querySelector('#ucDiscard').addEventListener('mouseover', function() { this.style.background = '#F0EDE9'; });
        modal.querySelector('#ucDiscard').addEventListener('mouseout', function() { this.style.background = 'none'; });
        modal.querySelector('#ucSave').addEventListener('mouseover', function() { this.style.background = '#E85A4D'; });
        modal.querySelector('#ucSave').addEventListener('mouseout', function() { this.style.background = '#FF6F61'; });
    }

    modal.style.display = 'flex';

    const closeIt = () => { modal.style.display = 'none'; cleanup(); };
    const saveIt = () => { closeIt(); if(onSave) onSave(); };
    const discardIt = () => { closeIt(); if(onDiscard) onDiscard(); };

    const btnClose = modal.querySelector('#ucClose');
    const btnSave = modal.querySelector('#ucSave');
    const btnDiscard = modal.querySelector('#ucDiscard');

    const cleanup = () => {
        btnClose.removeEventListener('click', closeIt);
        btnSave.removeEventListener('click', saveIt);
        btnDiscard.removeEventListener('click', discardIt);
    };

    btnClose.addEventListener('click', closeIt);
    btnSave.addEventListener('click', saveIt);
    btnDiscard.addEventListener('click', discardIt);
}


function render(j) {
    localStorage.setItem(LAST_ID_KEY, String(j.id));
    
    document.getElementById('journeyTitle').textContent = String(j?.name || 'Mon Voyage').replace(/\s*<br\s*\/?>\s*/gi, ' ').trim();
    
    const locText = j?.location || '';
    const locEl = document.getElementById('journeyLocation');
    if (!locText || locText === 'Mon Carnet Magazine' || locText === 'Ville, lieux...') { 
        locEl.textContent = 'Votre aventure'; 
    } else { 
        locEl.textContent = locText; 
    }
    
    const days = deriveDays(j || {}); 
    const aCount = countActivities(j || {});
    
    document.getElementById('summaryLine').innerHTML =
        `<span>${days.length} jour${days.length > 1 ? 's' : ''}</span> <span style="margin: 0 10px;">•</span> <span>${aCount} activité${aCount > 1 ? 's' : ''}</span>`;

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
        
        backBtn.style.display = 'inline-flex'; 
        sectionTitle.appendChild(backBtn);
    }

    const list = document.getElementById('daysList'); list.innerHTML = '';
    
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
            desc.textContent = 'Journée libre';
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
        const detailBtn = document.createElement('button'); 
        detailBtn.className = 'btn-voir-detail'; 
        detailBtn.textContent = 'Voir le détail';
        
        if (total === 0) {
            detailBtn.style.opacity = '0.5';
            detailBtn.style.cursor = 'default';
        }

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

                    let img = escapeHtml(getSafeImage(act));

                    let actDesc = act?.description || act?.shortDescription || "";
                    if (typeof actDesc === 'object') actDesc = actDesc.fr || actDesc.en || Object.values(actDesc)[0] || "";
                    if (!actDesc || actDesc.trim() === "") actDesc = "Aucune description détaillée n'est disponible pour cette activité.";

                    actCard.innerHTML = `
                        <div style="display: flex; align-items: center; gap: 12px; width: 100%;">
                            <img src="${img}" class="inline-act-thumb" onerror="this.src='/static/img/travel.jpg'">
                            <div style="flex: 1;">
                                <div style="font-weight:700; font-size:14px; color: var(--text-main);">${act.name || act.nom || 'Activité'}</div>
                                <div style="font-size:12px; color: var(--text-soft);">${act.city || act.ville || act.locality || ''}</div>
                            </div>
                        </div>
                        <p style="font-size: 13px; color: var(--text-soft); line-height: 1.5; margin: 10px 0 0 0; width: 100%;">${actDesc}</p>
                        
                        <div style="display:flex; gap:8px; margin-top:12px; width:100%; border-top: 1px dashed var(--border-light); padding-top: 10px;">
                            <button type="button" class="btn-act-replace" onmouseover="this.style.background='#F0EDE9'" onmouseout="this.style.background='transparent'" style="background:transparent; border:1px solid #D4C3B3; color:#6b5f57; padding:6px 12px; border-radius:4px; font-size:11px; font-family:'Montserrat', sans-serif; cursor:pointer; font-weight:600; flex:1; transition:all 0.2s;">↔ Remplacer</button>
                            <button type="button" class="btn-act-remove" onmouseover="this.style.background='#FF6F61'; this.style.color='white'" onmouseout="this.style.background='transparent'; this.style.color='#FF6F61'" style="background:transparent; border:1px solid #FF6F61; color:#FF6F61; padding:6px 12px; border-radius:4px; font-size:11px; font-family:'Montserrat', sans-serif; cursor:pointer; font-weight:600; flex:1; transition:all 0.2s;">✕ Retirer</button>
                        </div>
                    `;

                    actCard.querySelector('.btn-act-remove').addEventListener('click', (e) => {
                        e.stopPropagation();
                        
                        // --- SÉCURITÉ : Ne pas supprimer si c'est la dernière activité de la journée ---
                        if (total <= 1) {
                            showToast("Impossible : votre journée doit contenir au moins une activité.");
                            return;
                        }

                        showCustomConfirm(
                            "Retirer cette activité ?", 
                            `Voulez-vous vraiment retirer "${act.name || act.nom || 'cette activité'}" de votre programme ?`, 
                            () => {
                                days[idx].slots[slotKey].splice(actIdx, 1);
                                j.plan = days; 
                                _openDayIndex = idx;
                                applyLocalChange(j);
                            }
                        );
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
            if (total === 0) return; // Empêche l'ouverture si le jour est vide
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

function ensureModal() {
    let modal = document.getElementById('replaceModal');
    if (modal) return modal;
    modal = document.createElement('div');
    modal.id = 'replaceModal';
    modal.style.cssText = 'position:fixed;inset:0;background:rgba(28,28,28,0.55);display:none;align-items:center;justify-content:center;z-index:9999;padding:20px;';
    
    modal.innerHTML = `
<div class="rm-card" style="background:#FAF8F5;max-width:850px;width:100%;max-height:85vh;overflow:auto;border-radius:16px;padding:24px;box-shadow:0 20px 60px rgba(0,0,0,0.3);font-family:Lora,serif;">
    <div class="rm-header" style="display:flex;justify-content:space-between;align-items:flex-start;gap:16px;margin-bottom:18px;">
        <div>
            <h3 style="font-family:'Cormorant Garamond',serif;font-size:24px;margin:0;color:#1C1C1C;">Remplacer l'activité</h3>
            <p class="rm-helper" style="margin:8px 0 0;color:#6b5f57;font-size:14px;">Choisissez une alternative selon vos envies.</p>
        </div>
        <button id="rmClose" type="button" style="background:none;border:none;font-size:26px;cursor:pointer;color:#6b5f57;">×</button>
    </div>
    
    <div style="display:flex; gap:10px; margin-bottom:20px;">
        <button id="btnModeSimilaire" style="flex:1; padding:10px; border-radius:8px; border:1px solid #FF6F61; background:#FF6F61; color:white; cursor:pointer; font-weight:600;">Esprit similaire</button>
        <button id="btnModeDifferent" style="flex:1; padding:10px; border-radius:8px; border:1px solid #D4C3B3; background:white; color:#6b5f57; cursor:pointer; font-weight:600;">Autre type d'activité</button>
    </div>
    
    <input id="rmSearch" type="text" autocomplete="off" placeholder="Rechercher manuellement…" style="width:100%;padding:12px 16px;border:1px solid #D4C3B3;border-radius:10px;margin-bottom:22px;outline:none;background-color:white;">
    
    <div class="rm-recommendations">
        <div id="rmCarouselTitle" class="rm-rec-title" style="font-family: 'Montserrat', sans-serif; font-size: 11px; font-weight: 700; text-transform: uppercase; letter-spacing: 1.5px; color: var(--text-soft); margin-bottom:10px;">Suggestions dans le même esprit</div>
        <div id="rmCarousel" class="rm-carousel"></div>
    </div>
</div>`;
    document.body.appendChild(modal);

    const searchInput = modal.querySelector('#rmSearch');
    
    searchInput.addEventListener('focus', function() { this.style.borderColor = '#FF6F61'; });
    searchInput.addEventListener('blur', function() { this.style.borderColor = '#D4C3B3'; });
    searchInput.addEventListener('keydown', function(e) { if (e.key === 'Enter') this.blur(); });
    
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

    const currentAct = target.allDays[target.jourIdx].slots[target.moment][target.slotIdx];
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
            title.textContent = "Autres types d'activités";
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
        if (!input.value.trim()) { refreshRecommendations('similaire'); return; }
        
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
    const critRegion = _currentTarget?.journey?.criteria?.region || _currentTarget?.journey?.location || '';
    const slug = (critRegion && critRegion !== 'all' && critRegion !== 'Mon Carnet Magazine') ? critRegion : '';
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

function formatCategoriesString(rawCats) {
    let catsArr = [];
    if (Array.isArray(rawCats)) catsArr = rawCats.map(c => typeof c === 'string' ? c : (c.name || ""));
    else if (typeof rawCats === 'string') catsArr = rawCats.split(',').map(s => s.trim());
    return catsArr.filter(Boolean).join(' · ');
}

function renderGridCards(items, container) {
    const slicedItems = items.slice(0, 16);
    if (!slicedItems || !slicedItems.length) {
        container.innerHTML = '<div class="rm-empty" style="padding:20px; text-align:center; width:100%; grid-column: 1 / -1;">Aucun résultat trouvé.</div>';
        return;
    }

    container.innerHTML = slicedItems.map(item => {
        const id = escapeHtml(String(item.id || item._id || item.nom || ''));
        const name = escapeHtml(item.name || item.nom || 'Sans nom');
        const cats = escapeHtml(formatCategoriesString(item.categories || item.types || item.category || ''));
        const img = escapeHtml(getSafeImage(item));
        let distHtml = item.distance ? `<div class="rm-card-dist">${escapeHtml(String(item.distance).replace(/km/i, 'km').trim())}</div>` : '';

        return `
            <div class="rm-card-item" data-id="${id}">
                <div class="rm-card-visual"><img src="${img}" onerror="this.src='/static/img/travel.jpg'"></div>
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
        name: item.name || item.nom || 'Activité',
        ville: item.locality || item.ville || '',
        locality: item.locality || item.ville || '',
        categories: item.categories || item.types || [],
        description: item.description || item.desc || item.summary || '',
        image: getSafeImage(item),
        id: item.id || item._id || ''
    };
}

function applyReplacement(item) {
    if (!_currentTarget) return;
    const { jourIdx, moment, slotIdx, journey, allDays } = _currentTarget;
    allDays[jourIdx].slots[moment][slotIdx] = mongoToActivity(item);
    journey.plan = allDays;
    
    closeModal();
    applyLocalChange(journey);
    showToast("Activité remplacée avec succès !");
}

(async function init() {
    const id = getJourneyId();
    if (!id) { document.getElementById('journeyTitle').textContent = 'Voyage introuvable'; return; }
    const local = byLocalId(id); const server = await fetchServerJourney(id);
    const journey = (local && server) ? mergeJourneys(local, server) : (server || local);
    if (!journey) { document.getElementById('journeyTitle').textContent = 'Voyage introuvable'; return; }
    
    _currentJourney = journey;
    render(_currentJourney);

    const mainBackBtn = document.getElementById('backBtn');
    if (mainBackBtn) {
        const clonedBtn = mainBackBtn.cloneNode(true);
        mainBackBtn.parentNode.replaceChild(clonedBtn, mainBackBtn);
        clonedBtn.addEventListener('click', (e) => {
            e.preventDefault();
            if (_hasUnsavedChanges) {
                showUnsavedConfirm(
                    () => { saveJourneyToDB(_currentJourney); window.location.href = '/mesvoyages'; },
                    () => { window.location.href = '/mesvoyages'; }
                );
            } else {
                window.location.href = '/mesvoyages';
            }
        });
    }
})();