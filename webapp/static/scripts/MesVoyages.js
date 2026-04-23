
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
    const html = items.map(x => `
        <div class="panel-item">
          <img src="${x.image || '/static/img/no-image.jpg'}" alt="">
          <div class="pi-name">${x.name || 'Sans nom'}</div>
          <button class="pi-remove" onclick="removeItem('${key}', '${x.id}')">✕</button>
        </div>
      `).join('');
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

/* ===== Logic Carnet (Liste Voyages) =====
 * Source de vérité : le serveur (GET /journeys).
 * localStorage sert de cache pour afficher tout de suite au chargement
 * et de fallback si le backend est injoignable.
 * Les voyages "locaux" (créés dans l'éditeur, jamais poussés) sont fusionnés
 * avec les voyages serveur dans l'affichage.
 */
const JOURNEYS_KEY = "wish_journeys_v1";

const lsGetJourneys = () => { try { return JSON.parse(localStorage.getItem(JOURNEYS_KEY) || "[]"); } catch { return []; } };
const lsSetJourneys = (arr) => { try { localStorage.setItem(JOURNEYS_KEY, JSON.stringify(arr)); } catch { } };

// ─── Helpers de parsing ────────────────────────────────────
function normalizeSlotsAny(s) { s = s || {}; return { morning: s.morning || [], noon: s.noon || [], afternoon: s.afternoon || [], evening: s.evening || [] }; }

/** Nombre d'activités dans un voyage, qu'il soit au format "éditeur" (slots) ou "algo" (matin/aprem). */
function activitiesCount(j) {
    if (!Array.isArray(j?.plan)) return 0;
    return j.plan.reduce((acc, d) => {
        // Format algorithme : { matin: [...], aprem: [...] }
        if (Array.isArray(d?.matin) || Array.isArray(d?.aprem)) {
            return acc + (Array.isArray(d.matin) ? d.matin.length : 0)
                      + (Array.isArray(d.aprem) ? d.aprem.length : 0);
        }
        // Format éditeur : { slots: { morning, noon, afternoon, evening } }
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

function pickCover(j) {
    if (j?.cover) return j.cover;
    // Dernier recours : regarder dans le plan
    for (const day of (j?.plan || [])) {
        for (const key of ['matin', 'aprem']) {
            for (const act of (day?.[key] || [])) {
                if (act?.image) return act.image;
            }
        }
    }
    return "/static/img/no-image.jpg";
}

function stashEditPayload(j) {
    try {
        sessionStorage.setItem('wish_edit_id', String(j.id));
        sessionStorage.setItem('wish_edit_payload', JSON.stringify(j));
    } catch { }
}

// ─── Communication serveur ─────────────────────────────────
async function fetchServerJourneys() {
    try {
        const res = await fetch('/journeys', { headers: { 'Accept': 'application/json' } });
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const arr = await res.json();
        return Array.isArray(arr) ? arr : [];
    } catch (err) {
        console.warn('[MesVoyages] Serveur indisponible, fallback localStorage :', err.message);
        return null; // null = on n'a pas pu parler au serveur
    }
}

async function deleteServerJourney(id) {
    try {
        const res = await fetch(`/journeys/${encodeURIComponent(id)}`, { method: 'DELETE' });
        // 404 = pas sur le serveur, c'est OK (voyage purement local)
        return res.ok || res.status === 404;
    } catch (err) {
        console.warn('[MesVoyages] Suppression serveur échouée :', err.message);
        return false;
    }
}

/**
 * Fusionne deux listes de voyages (serveur + local) par id.
 * Le plus récent (updatedAt) gagne.
 */
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

// ─── Rendu ─────────────────────────────────────────────────
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

    for (const j of journeys) {
        const item = document.createElement("div"); item.className = "item";
        item.innerHTML = `
          <div class="left">
            <div class="thumb" style="background-image:url('${esc(pickCover(j))}')"></div>
            <div class="meta">
              <div class="name">${esc(j.name || "Voyage sans titre")}</div>
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

            // 1) serveur d'abord
            await deleteServerJourney(j.id);
            // 2) cache local ensuite
            lsSetJourneys(lsGetJourneys().filter(x => String(x.id) !== String(j.id)));
            // 3) recharger la liste
            loadJourneys();
        });

        item.addEventListener("click", () => window.location.href = `/journeys/view/${encodeURIComponent(j.id)}`);
        listEl.appendChild(item);
    }
}

async function loadJourneys() {
    const emptyEl = document.getElementById("emptyState");
    emptyEl.style.display = "none";

    // 1. Rendu immédiat depuis le cache local (perçu instantané)
    const cached = lsGetJourneys();
    renderJourneys(
        [...cached].sort((a, b) => new Date(b?.updatedAt || 0) - new Date(a?.updatedAt || 0))
    );

    // 2. Fetch serveur en parallèle
    const serverJourneys = await fetchServerJourneys();
    if (serverJourneys === null) {
        // Serveur KO : on reste sur le cache
        return;
    }

    // 3. Fusion + rendu + mise à jour du cache
    const merged = mergeJourneys(serverJourneys, cached);
    lsSetJourneys(merged);
    renderJourneys(merged);
}

document.addEventListener("DOMContentLoaded", () => {
    loadJourneys(); updateCounts(); renderPanels();
});
