/* ===== LA JOLIE MODALE D'ALERTE ===== */
function showCustomAlert(title, message) {
    let modal = document.getElementById('customAlertModal');
    if (!modal) {
        modal = document.createElement('div');
        modal.id = 'customAlertModal';
        modal.style.cssText = 'position:fixed;inset:0;background:rgba(28,28,28,0.6);display:none;align-items:center;justify-content:center;z-index:9999;padding:20px;';
        modal.innerHTML = `
            <div style="background:#FAF8F5;max-width:400px;width:100%;border-radius:16px;padding:32px;box-shadow:0 20px 60px rgba(0,0,0,0.3);text-align:center;position:relative;">
                <button id="caClose" style="position:absolute;top:16px;right:16px;background:none;border:none;font-size:24px;cursor:pointer;color:#6b5f57;">×</button>
                <div style="width:50px;height:50px;border-radius:50%;background:rgba(255,111,97,0.1);color:#FF6F61;display:flex;align-items:center;justify-content:center;margin:0 auto 16px;">
                    <svg viewBox="0 0 24 24" width="24" height="24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"></circle><line x1="12" y1="8" x2="12" y2="12"></line><line x1="12" y1="16" x2="12.01" y2="16"></line></svg>
                </div>
                <h3 id="caTitle" style="font-family:'Cormorant Garamond',serif;font-size:24px;margin:0 0 8px;color:#1C1C1C;">Titre</h3>
                <p id="caMessage" style="margin:0 0 24px;color:#6b5f57;font-size:15px;line-height:1.5;font-family:'Lora',serif;">Message</p>
                <button id="caOk" type="button" style="background:#FF6F61;border:none;color:#fff;padding:10px 24px;border-radius:8px;cursor:pointer;font-weight:700;font-family:'Montserrat',sans-serif;font-size:11px;text-transform:uppercase;letter-spacing:0.5px;transition:background 0.2s;">OK</button>
            </div>
        `;
        document.body.appendChild(modal);

        modal.querySelector('#caOk').addEventListener('mouseover', function() { this.style.background = '#E85A4D'; });
        modal.querySelector('#caOk').addEventListener('mouseout', function() { this.style.background = '#FF6F61'; });
    }

    modal.querySelector('#caTitle').textContent = title;
    modal.querySelector('#caMessage').textContent = message;
    modal.style.display = 'flex';

    const closeIt = () => { modal.style.display = 'none'; };
    modal.querySelector('#caClose').onclick = closeIt;
    modal.querySelector('#caOk').onclick = closeIt;
}


function updateVal(id) {
    document.getElementById('val-' + id).textContent = document.getElementById(id).value;
}

const criteriaForm = document.getElementById('criteriaForm');
if (criteriaForm) {
    criteriaForm.addEventListener('submit', async function (e) {
        e.preventDefault();

        const btn = document.getElementById('submitBtn');
        const originalBtnText = btn.dataset.originalText || btn.textContent;
        btn.dataset.originalText = originalBtnText;

        const regionSelect = document.getElementById('region');
        const regionValue = regionSelect ? regionSelect.value : '';
        const regionLabel = regionSelect ? regionSelect.options[regionSelect.selectedIndex].text : '';

        let ville = document.getElementById('ville').value.trim();

        if (!ville) {
            if (regionValue) {
                ville = regionLabel;
            } else {
                showCustomAlert("Localisation manquante", "Veuillez cliquer sur la carte pour sélectionner un lieu, ou choisir une région précise.");
                return;
            }
        }

        const locationData = {
            ville: ville,
            region: regionValue,
            rayon: parseInt(document.getElementById('rayon').value, 10),
            jours: parseInt(document.getElementById('jours').value, 10) || 1
        };

        const preferencesData = {
            detente: document.getElementById('detente').value,
            nature: document.getElementById('nature').value,
            sport: document.getElementById('sport').value,
            gastronomie: document.getElementById('gastronomie').value,
            culture: document.getElementById('culture').value
        };

        const formData = { ...locationData, ...preferencesData };
        console.log("[AVANT SOUMISSION] Critères : ", formData);

        btn.disabled = true;
        btn.textContent = 'Soumission en cours...';

        try {
            const res = await fetch('/algorithm', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(formData)
            });

            let data = null;
            try { data = await res.json(); } catch (_) { }

            if (!res.ok) {
                const serverMsg = (data && (data.message || data.detail)) || `Erreur HTTP ${res.status}`;
                throw new Error(serverMsg);
            }

            if (data && data.status === 'empty') {
                showCustomAlert("Aucun résultat", data.message || "Aucun itinéraire trouvé avec ces critères. Essayez un autre lieu ou augmentez le rayon.");
                return;
            }

            sessionStorage.setItem('algorithmRes', JSON.stringify(data));
            sessionStorage.setItem('criteriaFormPayload', JSON.stringify(formData));
            window.location.href = '/Voyage.html';

        } catch (error) {
            console.error("Erreur lors de la soumission :", error);
            showCustomAlert("Erreur de connexion", "Une erreur est survenue lors de la soumission. Veuillez réessayer.");
        } finally {
            btn.disabled = false;
            btn.textContent = originalBtnText;
        }
    });
}

document.addEventListener('DOMContentLoaded', () => {
    if (document.getElementById('map')) {
        const map = L.map('map').setView([46.603354, 1.888334], 5);
        L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
            attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OSM</a>'
        }).addTo(map);

        let marker;
        map.on('click', async function (e) {
            const lat = e.latlng.lat;
            const lng = e.latlng.lng;

            if (marker) map.removeLayer(marker);
            marker = L.marker([lat, lng]).addTo(map);

            document.getElementById('selected-ville-name').textContent = "Recherche...";

            try {
                const response = await fetch(`https://nominatim.openstreetmap.org/reverse?format=json&lat=${lat}&lon=${lng}&addressdetails=1`);
                const data = await response.json();

                if (!data || !data.address) throw new Error("Lieu introuvable");

                // --- VÉRIFICATION DE LA RÉGION ---
                const state = data.address.state || '';
                const stateLower = state.toLowerCase();
                
                const isHDF = stateLower.includes("hauts-de-france") || stateLower.includes("nord-pas-de-calais") || stateLower.includes("picardie");
                const isARA = stateLower.includes("auvergne") || stateLower.includes("rhône") || stateLower.includes("rhone") || stateLower.includes("alpes");

                if (!isHDF && !isARA) {
                    showCustomAlert(
                        "Zone non couverte", 
                        "Pour le moment, nous ne proposons des voyages que dans les régions Hauts-de-France et Auvergne-Rhône-Alpes. Veuillez sélectionner une ville dans ces zones !"
                    );
                    document.getElementById('selected-ville-name').textContent = "(Cliquez sur la carte)";
                    document.getElementById('ville').value = "";
                    if (marker) map.removeLayer(marker);
                    return; 
                }

                // --- SI LA RÉGION EST BONNE ---
                const ville = data.address.city || data.address.town || data.address.village || data.address.municipality || 'Lieu inconnu';
                document.getElementById('ville').value = ville;
                document.getElementById('selected-ville-name').textContent = ville;

                if (isHDF) document.getElementById('region').value = "hauts-de-france";
                if (isARA) document.getElementById('region').value = "auvergne-rhone-alpes";

            } catch (err) {
                console.error(err);
                document.getElementById('ville').value = "";
                document.getElementById('selected-ville-name').textContent = "Lieu inconnu";
                if (marker) map.removeLayer(marker);
            }
        });
    }
});

/* ========= Clés & helpers ========= */
const JOURNEYS_KEY = 'wish_journeys_v1';
const $ = (id) => document.getElementById(id);
const q = (sel, root = document) => root.querySelector(sel);
const qa = (sel, root = document) => Array.from(root.querySelectorAll(sel));

const urlParams = new URLSearchParams(location.search);
const IS_EDIT = urlParams.get('edit') === '1';
const EDIT_ID = IS_EDIT ? (urlParams.get('id') || sessionStorage.getItem('wish_edit_id') || '') : '';

/* ========= Editor Logic ========= */
let draggingEl = null;

function updateDropHints() {
    qa('.slot').forEach(s => {
        const hint = q('.drop-hint', s);
        if (hint) hint.style.display = s.querySelector('.activity') ? 'none' : 'block';
    });
}

function createActivityElement(item) {
    const el = document.createElement('div'); el.className = 'activity'; el.draggable = true;
    el.dataset.id = String(item.id); el.dataset.name = item.name || ''; el.dataset.image = item.image || item.photo || '';
    const imgSrc = item.image || item.photo || ('https://picsum.photos/seed/' + encodeURIComponent(item.id) + '/80/80');
    el.innerHTML = `<img alt="" src="${imgSrc}"><span>${item.name != null ? item.name : ('#' + item.id)}</span><button class="remove" title="Supprimer">✕</button>`;
    el.addEventListener('dragstart', (e) => { draggingEl = el; try { e.dataTransfer.setData('text/x-activity', 'move'); e.dataTransfer.effectAllowed = 'move'; } catch { } });
    el.addEventListener('dragend', () => { draggingEl = null; });
    q('.remove', el).addEventListener('click', () => { el.parentElement?.removeChild(el); updateDropHints(); });
    return el;
}

function wireSlot(slot) {
    slot.addEventListener('dragenter', (e) => { e.preventDefault(); slot.classList.add('drag-over'); });
    slot.addEventListener('dragleave', () => { slot.classList.remove('drag-over'); });
}
document.addEventListener('dragover', (e) => { if (e.target?.closest?.('.slot')) { e.preventDefault(); e.target.closest('.slot').classList.add('drag-over'); } });
document.addEventListener('drop', (e) => {
    const slot = e.target?.closest?.('.slot');
    if (!slot) return;
    e.preventDefault(); e.stopPropagation();
    qa('.slot.drag-over').forEach(s => s.classList.remove('drag-over'));
    if (draggingEl) { slot.appendChild(draggingEl); updateDropHints(); return; }
    try {
        const data = e.dataTransfer.getData('application/x-basket-item');
        if (data) { slot.appendChild(createActivityElement(JSON.parse(data))); updateDropHints(); }
    } catch { }
});

/* ========= Gestion des Jours ========= */
function createDaySection(dayNumber) {
    const sec = document.createElement('section');
    sec.className = 'day-section';
    sec.dataset.day = String(dayNumber);
    sec.innerHTML = `
        <div class="slots">
          <div class="slot" data-key="morning"><h5>Matinée</h5><div class="drop-hint">Déposez vos activités ici</div></div>
          <div class="slot" data-key="noon"><h5>Midi</h5><div class="drop-hint">Déposez vos activités ici</div></div>
          <div class="slot" data-key="afternoon"><h5>Après-midi</h5><div class="drop-hint">Déposez vos activités ici</div></div>
          <div class="slot" data-key="evening"><h5>Soirée</h5><div class="drop-hint">Déposez vos activités ici</div></div>
        </div>`;
    qa('.slot', sec).forEach(wireSlot);
    return sec;
}

function clearAllDays() {
    qa('.day-section').forEach(s => s.remove());
    qa('.day-title').forEach(t => t.remove());
}

function resetEditorToEmpty() {
    clearAllDays();
    const editor = q('.editor');
    if (!editor) return;
    const first = createDaySection(1);
    const h = document.createElement('h3'); h.className = 'day-title'; h.textContent = 'Journée 1';
    editor.insertBefore(h, q('#addDayBtn'));
    editor.insertBefore(first, q('#addDayBtn'));
    if($('journeyName')) $('journeyName').value = ''; 
    if($('journeyLocation')) $('journeyLocation').value = ''; 
    if($('createJourneyForm')) $('createJourneyForm').dataset.id = '';
    updateDropHints();
}

function addDay() {
    const editor = q('.editor');
    if (!editor) return;
    const n = qa('.day-section').length + 1;
    const sec = createDaySection(n);
    const h = document.createElement('h3'); h.className = 'day-title'; h.textContent = 'Journée ' + n;
    editor.insertBefore(h, q('#addDayBtn'));
    editor.insertBefore(sec, q('#addDayBtn'));
    updateDropHints();
}

/* ========= Save & Load ========= */
function loadJourneys() { try { return JSON.parse(localStorage.getItem(JOURNEYS_KEY) || '[]'); } catch { return []; } }
function saveJourneys(arr) { localStorage.setItem(JOURNEYS_KEY, JSON.stringify(arr)); }
function uid() { return 'j_' + Date.now().toString(36) + '_' + Math.random().toString(36).slice(2, 7); }

function collectPlan() {
    return qa('.day-section').map((sec, i) => ({
        day: i + 1,
        slots: qa('.slot', sec).map(s => ({
            key: s.getAttribute('data-key'),
            items: qa('.activity', s).map(a => ({ id: a.dataset.id, name: a.dataset.name, image: a.dataset.image || '' }))
        }))
    }));
}

function upsertJourney(j) {
    const all = loadJourneys();
    const idx = all.findIndex(x => x.id === j.id);
    if (idx >= 0) all[idx] = j; else all.unshift(j);
    saveJourneys(all);
    window.dispatchEvent(new StorageEvent('storage', { key: JOURNEYS_KEY }));
    showCustomAlert('Succès !', 'Voyage sauvegardé ✔️');
}

/* ========= Normalisation jours (pour édition) ========= */
function deriveDays(obj) {
    const A = (a) => Array.isArray(a) ? a : (a && typeof a === 'object') ? Object.values(a) : [];
    function slotsArrayToObj(slotsArr) {
        const o = { morning: [], noon: [], afternoon: [], evening: [] };
        (Array.isArray(slotsArr) ? slotsArr : []).forEach(s => { const k = String(s?.key || '').toLowerCase(); if (o[k]) o[k] = A(s.items); });
        return o;
    }
    if (Array.isArray(obj?.plan)) return obj.plan.map((d, i) => ({ day: (d.day != null ? Number(d.day) : i + 1), slots: slotsArrayToObj(d.slots) }));
    return [];
}

function renderForEdit(j) {
    if($('journeyName')) $('journeyName').value = j?.name || '';
    if($('journeyLocation')) $('journeyLocation').value = j?.location || '';
    clearAllDays();
    const editor = q('.editor');
    if (!editor) return;
    const days = deriveDays(j);
    if (!days.length) {
        const first = createDaySection(1);
        const h = document.createElement('h3'); h.className = 'day-title'; h.textContent = 'Journée 1';
        editor.insertBefore(h, q('#addDayBtn'));
        editor.insertBefore(first, q('#addDayBtn'));
    } else {
        days.forEach((d, i) => {
            const sec = createDaySection(i + 1);
            const h = document.createElement('h3'); h.className = 'day-title'; h.textContent = 'Journée ' + (i + 1);
            editor.insertBefore(h, q('#addDayBtn'));
            editor.insertBefore(sec, q('#addDayBtn'));
            ['morning', 'noon', 'afternoon', 'evening'].forEach(key => {
                const slot = qa(`.slot[data-key="${key}"]`, sec)[0];
                if (!slot) return;
                (d.slots?.[key] || []).forEach(item => { slot.appendChild(createActivityElement(item)); });
            });
        });
    }
    updateDropHints();
    if($('createJourneyForm')) $('createJourneyForm').dataset.id = String(j.id || '');
}

function loadForEditMaybe() {
    if (!IS_EDIT) return;
    let obj = null;
    if (EDIT_ID) try { obj = (JSON.parse(localStorage.getItem(JOURNEYS_KEY) || '[]')).find(x => String(x.id) === String(EDIT_ID)) || null; } catch { }
    if (!obj) try { obj = JSON.parse(sessionStorage.getItem('wish_edit_payload') || 'null'); } catch { }
    if (obj) renderForEdit(obj);
}

document.addEventListener('click', (e) => {
    if (e.target?.id === 'saveJourneyBtn') {
        const form = $('createJourneyForm');
        const existingId = form?.dataset?.id || null;
        const journey = {
            id: existingId || uid(),
            name: ($('journeyName') ? $('journeyName').value : '').trim() || 'Sans titre',
            location: ($('journeyLocation') ? $('journeyLocation').value : '').trim(),
            cover: q('.activity img') ? q('.activity img').src : '/static/img/no-image.jpg',
            createdAt: new Date().toISOString(), updatedAt: new Date().toISOString(),
            plan: collectPlan()
        };
        upsertJourney(journey);
        if (!existingId && form) { form.dataset.id = journey.id; }
    }
    if (e.target?.id === 'addDayBtn') { addDay(); }
});

/* ========= Photos démo ========= */
const addPhotos = $('addPhotos'); const photosInput = $('photosInput'); const photosPreview = $('photosPreview');
if (addPhotos) addPhotos.addEventListener('click', () => photosInput?.click());
if (photosInput) photosInput.addEventListener('change', () => {
    photosPreview.innerHTML = '';
    Array.from(photosInput.files || []).slice(0, 12).forEach(f => {
        const url = URL.createObjectURL(f);
        const img = document.createElement('img'); img.src = url; photosPreview.appendChild(img);
    });
});

document.addEventListener('DOMContentLoaded', () => {
    if (!IS_EDIT) { sessionStorage.removeItem('wish_edit_id'); sessionStorage.removeItem('wish_edit_payload'); resetEditorToEmpty(); } else { loadForEditMaybe(); }
});