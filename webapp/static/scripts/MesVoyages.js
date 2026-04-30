// 1. Liste des images de secours, disponible pour toutes les fonctions
const FALLBACK_IMAGES = [
    '/static/img/baie_de_somme2.jpg',
    '/static/img/auvergne.jpg',
    '/static/img/montagne_france2.jpg',
    '/static/img/boeuf_bourguignon.jpg',
    '/static/img/semur_en_auxois.jpg',
    '/static/img/vtt.jpg'
];

/* ===== Logic Panier & Favoris ===== */
const BASKET_KEY = 'wish_basket_v1';
const LIKES_KEY = 'wish_likes_v1';

const basketIcon = document.getElementById('basketIcon');
const basketCount = document.getElementById('basketCount');
const floatingBasket = document.getElementById('floatingBasket');
const likesIcon = document.getElementById('likesIcon');
const likesCount = document.getElementById('likesCount');
const floatingLikes = document.getElementById('floatingLikes');

const loadData = (k) => { try { return JSON.parse(localStorage.getItem(k) || '[]'); } catch { return []; } };
const saveData = (k, d) => { localStorage.setItem(k, JSON.stringify(d)); updateCounts(); renderPanels(); };

function updateCounts() {
    const b = loadData(BASKET_KEY).length;
    basketCount.textContent = b; basketCount.hidden = b === 0;
    const l = loadData(LIKES_KEY).length;
    likesCount.textContent = l; likesCount.hidden = l === 0;
}

function renderPanel(key, container, title, emptyMsg, isLike = false) {
    const items = loadData(key);
    if (!items.length) {
        container.innerHTML = `<h4>${title}</h4><p class="panel-empty">${emptyMsg}</p>` + (isLike ? '' : `<div class="panel-footer"><a href="/makejourney">Aller à la création</a></div>`);
        return;
    }
    
    // On génère le HTML d'abord avec l'anti "no-image"
    const html = items.map((x, idx) => {
        const thumb = (x.image && !x.image.includes('no-image')) 
            ? x.image 
            : FALLBACK_IMAGES[idx % FALLBACK_IMAGES.length];
            
        return `
            <div class="panel-item">
              <img src="${thumb}" alt="">
              <div class="pi-name">${x.name || 'Sans nom'}</div>
              <button class="pi-remove" onclick="removeItem('${key}', '${x.id}')">✕</button>
            </div>
        `;
    }).join('');

    // Puis on l'injecte proprement
    container.innerHTML = `<h4>${title}</h4>${html}` + (isLike ? '' : `<div class="panel-footer"><a href="/makejourney">Aller à la création</a></div>`);
}

function renderPanels() {
    renderPanel(BASKET_KEY, floatingBasket, 'Votre panier', 'Votre panier est vide.');
    renderPanel(LIKES_KEY, floatingLikes, 'Mes Favoris', 'Aucun favori.', true);
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

window.addEventListener("storage", (e) => {
    if (e.key === BASKET_KEY || e.key === LIKES_KEY) { updateCounts(); renderPanels(); }
});

/* ===== Logic Carnet (Liste Voyages) ===== */
const JOURNEYS_KEY = "wish_journeys_v1";

const lsGetJourneys = () => { try { return JSON.parse(localStorage.getItem(JOURNEYS_KEY) || "[]"); } catch { return []; } };
const lsSetJourneys = (arr) => { try { localStorage.setItem(JOURNEYS_KEY, JSON.stringify(arr)); } catch { } };

function normalizeSlotsAny(s) { s = s || {}; return { morning: s.morning || [], noon: s.noon || [], afternoon: s.afternoon || [], evening: s.evening || [] }; }

function activitiesCount(j) {
    if (!Array.isArray(j?.plan)) return 0;
    return j.plan.reduce((acc, d) => {
        if (Array.isArray(d?.matin) || Array.isArray(d?.aprem)) {
            return acc + (Array.isArray(d.matin) ? d.matin.length : 0)
                      + (Array.isArray(d.aprem) ? d.aprem.length : 0);
        }
        const s = d?.slots || {};
        const c = (arr) => Array.isArray(arr) ? arr.length : (arr ? Object.values(arr).length : 0);
        return acc + c(s.morning) + c(s.noon) + c(s.afternoon) + c(s.evening);
    }, 0);
}

function daysCount(j) { return Array.isArray(j?.plan) ? j.plan.length : 1; }

function metaText(j) {
    const d = daysCount(j);
    const a = activitiesCount(j);
    return (j.location ? j.location + " • " : "") + d + (d > 1 ? " jours" : " jour") + " • " + a + (a > 1 ? " activités" : " activité");
}

function pickCover(j, index) {
    // Si on a déjà une belle couverture, on la garde
    if (j?.cover && !j.cover.includes('no-image')) return j.cover;

    // Sinon, on cherche dans les activités du plan
    if (Array.isArray(j?.plan)) {
        for (const day of j.plan) {
            const slots = day.slots || day;
            const imgs = [slots.morning, slots.noon, slots.afternoon, slots.evening, slots.matin, slots.aprem];
            for (let slot of imgs) {
                if (Array.isArray(slot)) {
                    for (let act of slot) {
                        if (act?.image && !act.image.includes('no-image')) return act.image;
                    }
                }
            }
        }
    }

    // LA PIOCHE PARFAITE : On utilise la position de la carte (0, 1, 2, 3...)
    const safeIndex = typeof index === 'number' ? index : 0;
    return FALLBACK_IMAGES[safeIndex % FALLBACK_IMAGES.length];
}

function stashEditPayload(j) {
    try {
        sessionStorage.setItem('wish_edit_id', String(j.id));
        sessionStorage.setItem('wish_edit_payload', JSON.stringify(j));
    } catch { }
}

async function fetchServerJourneys() {
    try {
        const res = await fetch('/journeys', { headers: { 'Accept': 'application/json' } });
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const arr = await res.json();
        return Array.isArray(arr) ? arr : [];
    } catch (err) {
        console.warn('[MesVoyages] Serveur indisponible, fallback localStorage :', err.message);
        return null;
    }
}

async function deleteServerJourney(id) {
    try {
        const res = await fetch(`/journeys/${encodeURIComponent(id)}`, { method: 'DELETE' });
        return res.ok || res.status === 404;
    } catch (err) {
        console.warn('[MesVoyages] Suppression serveur échouée :', err.message);
        return false;
    }
}

function mergeJourneys(serverList, localList) {
    const map = new Map();
    const consider = (j) => {
        if (!j || !j.id) return;
        const existing = map.get(j.id);
        if (!existing) { map.set(j.id, j); return; }
        const aTs = new Date(existing.updatedAt || 0).getTime();
        const bTs = new Date(j.updatedAt || 0).getTime();
        if (bTs >= aTs) map.set(j.id, j);
    };
    (serverList || []).forEach(consider);
    (localList || []).forEach(consider);
    return Array.from(map.values())
        .sort((a, b) => new Date(b?.updatedAt || 0) - new Date(a?.updatedAt || 0));
}

function renderJourneys(journeys) {
    const listEl = document.getElementById("topicsList");
    const emptyEl = document.getElementById("emptyState");
    listEl.innerHTML = "";

    if (!journeys.length) {
        emptyEl.style.display = "block";
        return;
    }
    emptyEl.style.display = "none";

    const esc = (s) => String(s ?? '')
        .replace(/&/g, '&amp;').replace(/</g, '&lt;')
        .replace(/>/g, '&gt;').replace(/"/g, '&quot;');

    // On utilise forEach pour récupérer l'index (0, 1, 2...) de chaque voyage
    journeys.forEach((j, index) => {
        const item = document.createElement("div"); 
        item.className = "item";
        
        // Nettoie les anciens noms qui contiennent un <br> littéral injecté par l'ancien Voyage.js
        const cleanName = String(j.name || "Voyage sans titre").replace(/\s*<br\s*\/?>\s*/gi, ' ').trim();
        const formattedName = esc(cleanName);
        
        item.innerHTML = `
          <div class="left">
            <!-- On passe l'index à pickCover ici ! -->
            <div class="thumb" style="background-image:url('${esc(pickCover(j, index))}')"></div>
            <div class="meta">
              <div class="name">${formattedName}</div>
              <div class="desc">${esc(metaText(j))}</div>
            </div>
          </div>
          <div class="actions-right">
            <button class="icon-btn edit-btn" title="Modifier">
              <svg viewBox="0 0 24 24"><path d="M3 21l3.9-1 11.7-11.7a2.1 2.1 0 0 0 0-3l-1-1a2.1 2.1 0 0 0-3 0L3 16.1 3 21z"/><path d="M15 5l4 4"/></svg>
            </button>
            <button class="icon-btn delete-btn" title="Supprimer">
              <svg viewBox="0 0 24 24"><path d="M3 6h18"/><path d="M8 6v-2a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2"/><path d="M19 6l-1 14a2 2 0 0 1-2 2H8a2 2 0 0 1-2-2L5 6"/><path d="M10 11v6"/><path d="M14 11v6"/></svg>
            </button>
          </div>
        `;

        item.querySelector('.edit-btn').addEventListener('click', (e) => {
            e.stopPropagation(); stashEditPayload(j);
            window.location.href = `/makejourney?id=${encodeURIComponent(j.id)}&edit=1`;
        });

        item.querySelector('.delete-btn').addEventListener('click', async (e) => {
            e.stopPropagation();
            if (!confirm(`Supprimer « ${j.name || 'Voyage'} » ?`)) return;

            await deleteServerJourney(j.id);
            lsSetJourneys(lsGetJourneys().filter(x => String(x.id) !== String(j.id)));
            loadJourneys();
        });

        item.addEventListener("click", () => window.location.href = `/journeys/view/${encodeURIComponent(j.id)}`);
        listEl.appendChild(item);
    });
}

async function loadJourneys() {
    const emptyEl = document.getElementById("emptyState");
    emptyEl.style.display = "none";

    const cached = lsGetJourneys();
    renderJourneys(
        [...cached].sort((a, b) => new Date(b?.updatedAt || 0) - new Date(a?.updatedAt || 0))
    );

    const serverJourneys = await fetchServerJourneys();
    if (serverJourneys === null) return;

    const merged = mergeJourneys(serverJourneys, cached);
    lsSetJourneys(merged);
    renderJourneys(merged);
}

document.addEventListener("DOMContentLoaded", () => {
    loadJourneys(); updateCounts(); renderPanels();
});