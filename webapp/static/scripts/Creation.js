function updateVal(id) {
    document.getElementById('val-' + id).textContent = document.getElementById(id).value;
}

document.getElementById('criteriaForm').addEventListener('submit', async function (e) {
    e.preventDefault();

    // ==========================================
    // DONNÉES DE LOCALISATION (Ville & Zone Géo)
    // ==========================================
    const locationData = {
        ville: document.getElementById('ville').value.trim(),
        rayon: parseInt(document.getElementById('rayon').value, 10),
        jours: parseInt(document.getElementById('jours').value, 10) || 1
    };

    // ==========================================
    // CRITÈRES DE PRÉFÉRENCES (1 à 10)
    // ==========================================
    const preferencesData = {
        detente: document.getElementById('detente').value,
        nature: document.getElementById('nature').value,
        sport: document.getElementById('sport').value,
        gastronomie: document.getElementById('gastronomie').value,
        culture: document.getElementById('culture').value
    };

    // ==========================================
    // FUSION DES DONNÉES POUR SOUMISSION
    // ==========================================
    const formData = {
        ...locationData,
        ...preferencesData
    };

    console.log("Critères soumis :", formData);

    const btn = document.getElementById('submitBtn');
    btn.disabled = true;
    btn.textContent = 'Soumission en cours...';
    console.log("[AVANT SOUMISSION] Critères : ", formData);
    try {
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

        // Pour garder les données même après la redirectoin
        sessionStorage.setItem('algorithmResponse', JSON.stringify(data));
        sessionStorage.setItem('criteriaFormPayload', JSON.stringify(formData));
        window.location.href = '/Voyage.html';

    } catch (error) {
        console.error("Erreur lors de la soumission :", error);
        alert("Une erreur est survenue lors de la soumission. Veuillez réessayer.");
    } finally {
        btn.disabled = false;
        btn.textContent = 'Soumettre';
    }
});

// ==========================================
// INITIALISATION DE LA CARTE (LEAFLET)
// ==========================================
document.addEventListener('DOMContentLoaded', () => {
    // Si la carte existe sur la page
    if (document.getElementById('map')) {
        // Coordonnées par défaut (centre de la France)
        const map = L.map('map').setView([46.603354, 1.888334], 5);

        // Ajout du fond de carte OpenStreetMap
        L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
            attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OSM</a>'
        }).addTo(map);

        let marker;

        // Gérer le clic sur la carte
        map.on('click', async function (e) {
            const lat = e.latlng.lat;
            const lng = e.latlng.lng;

            // Placer le marqueur
            if (marker) map.removeLayer(marker);
            marker = L.marker([lat, lng]).addTo(map);

            // Mettre à jour l'affichage pendant la recherche
            document.getElementById('selected-ville-name').textContent = "Recherche...";

            // Appel API Nominatim pour trouver la ville (Reverse Geocoding)
            try {
                const response = await fetch(`https://nominatim.openstreetmap.org/reverse?format=json&lat=${lat}&lon=${lng}`);
                const data = await response.json();

                const ville = data.address.city || data.address.town || data.address.village || data.address.municipality || 'Lieu inconnu';

                document.getElementById('ville').value = ville;
                document.getElementById('selected-ville-name').textContent = ville;
            } catch (err) {
                console.error(err);
                document.getElementById('ville').value = `${lat.toFixed(3)}, ${lng.toFixed(3)}`;
                document.getElementById('selected-ville-name').textContent = "Lieu sélectionné";
            }
        });
    }
});

/* ========= Clés & helpers =========
const BASKET_KEY = 'wish_basket_v1';
const LIKES_KEY = 'wish_likes_v1';
const JOURNEYS_KEY = 'wish_journeys_v1';

const $ = (id) => document.getElementById(id);
const q = (sel, root = document) => root.querySelector(sel);
const qa = (sel, root = document) => Array.from(root.querySelectorAll(sel));

// Mode
const urlParams = new URLSearchParams(location.search);
const IS_EDIT = urlParams.get('edit') === '1';
const EDIT_ID = IS_EDIT ? (urlParams.get('id') || sessionStorage.getItem('wish_edit_id') || '') : '';

/* ========= Panier & Favoris Logic =========
const basketIcon = $('basketIcon');
const basketCount = $('basketCount');
const floatingBasket = $('floatingBasket');
const likesIcon = $('likesIcon');
const likesCount = $('likesCount');
const floatingLikes = $('floatingLikes');

const loadData = (k) => { try { return JSON.parse(localStorage.getItem(k) || '[]'); } catch { return []; } };
const saveData = (k, d) => { localStorage.setItem(k, JSON.stringify(d)); updateCounts(); renderPanels(); };

function updateCounts() {
    const b = loadData(BASKET_KEY).length;
    basketCount.textContent = b; basketCount.hidden = b === 0;
    const l = loadData(LIKES_KEY).length;
    likesCount.textContent = l; likesCount.hidden = l === 0;
}

function renderPanel(key, container, title, emptyMsg, draggable = false) {
    const items = loadData(key);
    if (!items.length) {
        container.innerHTML = `<h4>${title}</h4><p class="panel-empty">${emptyMsg}</p>`;
        return;
    }
    const esc = (s) => String(s || '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/"/g, '&quot;');
    container.innerHTML = `<h4>${title}</h4>` + items.map(x => (
        '<div class="panel-item"' + (draggable ? ' draggable="true"' : '') +
        ' data-id="' + esc(x.id) + '" data-name="' + esc(x.name) + '" data-image="' + esc(x.image || '') + '">' +
        '<img src="' + (x.image || '/static/img/no-image.jpg') + '" alt="">' +
        '<div class="pi-name">' + (esc(x.name) || 'Sans nom') + '</div>' +
        '<button class="pi-remove" onclick="removeItem(\'' + key + '\', \'' + x.id + '\')">✕</button>' +
        '</div>'
    )).join('');

    // Wire drag events for basket
    if (draggable) {
        qa('.panel-item', container).forEach(row => {
            row.addEventListener('dragstart', e => {
                const data = { id: row.dataset.id, name: row.dataset.name, image: row.dataset.image };
                try { e.dataTransfer.setData('application/x-basket-item', JSON.stringify(data)); e.dataTransfer.effectAllowed = 'copy'; } catch { }
            });
        });
    }
}

function renderPanels() {
    renderPanel(BASKET_KEY, floatingBasket, 'Votre panier', 'Votre panier est vide.', true); // Panier draggable
    renderPanel(LIKES_KEY, floatingLikes, 'Mes Favoris', 'Aucun favori.', false); // Likes pas draggable (selon besoin)
}

window.removeItem = function (key, id) {
    const data = loadData(key).filter(x => String(x.id) !== String(id));
    saveData(key, data);
};

basketIcon.addEventListener('click', (e) => {
    e.stopPropagation();
    floatingLikes.style.display = 'none';
    floatingBasket.style.display = floatingBasket.style.display === 'block' ? 'none' : 'block';
});
likesIcon.addEventListener('click', (e) => {
    e.stopPropagation();
    floatingBasket.style.display = 'none';
    floatingLikes.style.display = floatingLikes.style.display === 'block' ? 'none' : 'block';
});
document.addEventListener('click', (e) => {
    if (!floatingBasket.contains(e.target) && e.target !== basketIcon) floatingBasket.style.display = 'none';
    if (!floatingLikes.contains(e.target) && e.target !== likesIcon) floatingLikes.style.display = 'none';
});
window.addEventListener('storage', (e) => { if (e.key === BASKET_KEY || e.key === LIKES_KEY) { updateCounts(); renderPanels(); } });


/* ========= Editor Logic =========
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

/* ========= Gestion des Jours (CORRECTION DOUBLON TITRE) =========
function createDaySection(dayNumber) {
    const sec = document.createElement('section');
    sec.className = 'day-section';
    sec.dataset.day = String(dayNumber);
    // CORRECTION: On NE met PAS le <h3> dans le innerHTML, car il est ajouté avant la section par la fonction appelante
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
    const first = createDaySection(1);
    const h = document.createElement('h3'); h.className = 'day-title'; h.textContent = 'Journée 1';
    editor.insertBefore(h, q('#addDayBtn'));
    editor.insertBefore(first, q('#addDayBtn'));
    $('journeyName').value = ''; $('journeyLocation').value = ''; $('createJourneyForm').dataset.id = '';
    updateDropHints();
}

function addDay() {
    const editor = q('.editor');
    const n = qa('.day-section').length + 1;
    const sec = createDaySection(n);
    const h = document.createElement('h3'); h.className = 'day-title'; h.textContent = 'Journée ' + n;
    editor.insertBefore(h, q('#addDayBtn'));
    editor.insertBefore(sec, q('#addDayBtn'));
    updateDropHints();
}

/* ========= Save & Load =========
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
    alert('Voyage sauvegardé ✔️');
}

/* ========= Normalisation jours (pour édition) =========
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
    $('journeyName').value = j?.name || '';
    $('journeyLocation').value = j?.location || '';
    clearAllDays();
    const editor = q('.editor');
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
    $('createJourneyForm').dataset.id = String(j.id || '');
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
            name: ($('journeyName').value || '').trim() || 'Sans titre',
            location: ($('journeyLocation').value || '').trim(),
            cover: q('.activity img') ? q('.activity img').src : '/static/img/no-image.jpg',
            createdAt: new Date().toISOString(), updatedAt: new Date().toISOString(),
            plan: collectPlan()
        };
        upsertJourney(journey);
        if (!existingId && form) { form.dataset.id = journey.id; }
    }
    if (e.target?.id === 'addDayBtn') { addDay(); }
});

/* ========= Photos démo =========
const addPhotos = $('addPhotos'); const photosInput = $('photosInput'); const photosPreview = $('photosPreview');
addPhotos?.addEventListener('click', () => photosInput?.click());
photosInput?.addEventListener('change', () => {
    photosPreview.innerHTML = '';
    Array.from(photosInput.files || []).slice(0, 12).forEach(f => {
        const url = URL.createObjectURL(f);
        const img = document.createElement('img'); img.src = url; photosPreview.appendChild(img);
    });
});

document.addEventListener('DOMContentLoaded', () => {
    updateCounts(); renderPanels();
    if (!IS_EDIT) { sessionStorage.removeItem('wish_edit_id'); sessionStorage.removeItem('wish_edit_payload'); resetEditorToEmpty(); } else { loadForEditMaybe(); }
});
*/