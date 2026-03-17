
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

/* ===== Logic Carnet (Liste Voyages) ===== */
const JOURNEYS_KEY = "wish_journeys_v1";

const lsGetJourneys = () => { try { return JSON.parse(localStorage.getItem(JOURNEYS_KEY) || "[]"); } catch { return []; } };
const lsSetJourneys = (arr) => { try { localStorage.setItem(JOURNEYS_KEY, JSON.stringify(arr)); } catch { } };

// ... (Helpers de parsing conservés) ...
function normalizeSlotsAny(s) { s = s || {}; return { morning: s.morning || [], noon: s.noon || [], afternoon: s.afternoon || [], evening: s.evening || [] }; }
function deriveDays(j) {
    if (Array.isArray(j?.plan)) return j.plan.map((d, i) => ({ slots: d.slots }));
    return [{ slots: normalizeSlotsAny(j?.slots) }]; // Fallback simple
}
function activitiesCount(j) {
    const days = deriveDays(j);
    return days.reduce((acc, d) => {
        const s = d.slots || {};
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
function pickCover(j) { return j?.cover || "/static/img/no-image.jpg"; }
function stashEditPayload(j) { try { sessionStorage.setItem('wish_edit_id', String(j.id)); } catch { } }

async function loadJourneys() {
    const listEl = document.getElementById("topicsList");
    const emptyEl = document.getElementById("emptyState");
    listEl.innerHTML = ""; emptyEl.style.display = "none";

    const journeys = lsGetJourneys().sort((a, b) => new Date(b?.updatedAt || 0) - new Date(a?.updatedAt || 0));
    if (!journeys.length) { emptyEl.style.display = "block"; return; }

    for (const j of journeys) {
        const item = document.createElement("div"); item.className = "item";
        item.innerHTML = `
          <div class="left">
            <div class="thumb" style="background-image:url('${pickCover(j)}')"></div>
            <div class="meta">
              <div class="name">${j.name || "Voyage sans titre"}</div>
              <div class="desc">${metaText(j)}</div>
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

        item.querySelector('.delete-btn').addEventListener('click', (e) => {
            e.stopPropagation();
            if (confirm(`Supprimer « ${j.name || 'Voyage'} » ?`)) {
                const arr = lsGetJourneys().filter(x => String(x.id) !== String(j.id));
                lsSetJourneys(arr);
                loadJourneys();
            }
        });

        item.addEventListener("click", () => window.location.href = `/journeys/view/${encodeURIComponent(j.id)}`);
        listEl.appendChild(item);
    }
}

document.addEventListener("DOMContentLoaded", () => {
    loadJourneys(); updateCounts(); renderPanels();
});
