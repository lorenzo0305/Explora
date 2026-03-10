/* ===== Logic Panier & Favoris ===== */
const BASKET_KEY = 'wish_basket_v1';
const LIKES_KEY = 'wish_likes_v1';

// Elements
const basketIcon = document.getElementById('basketIcon');
const basketCount = document.getElementById('basketCount');
const floatingBasket = document.getElementById('floatingBasket');
const likesIcon = document.getElementById('likesIcon');
const likesCount = document.getElementById('likesCount');
const floatingLikes = document.getElementById('floatingLikes');

// Loaders
const loadData = (k) => { try { return JSON.parse(localStorage.getItem(k) || '[]'); } catch { return []; } };
const saveData = (k, d) => { localStorage.setItem(k, JSON.stringify(d)); updateCounts(); renderPanels(); };

// Update Counts
function updateCounts() {
    const b = loadData(BASKET_KEY).length;
    basketCount.textContent = b; basketCount.hidden = b === 0;
    const l = loadData(LIKES_KEY).length;
    likesCount.textContent = l; likesCount.hidden = l === 0;
}

// Render Logic
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

// Global remove function
window.removeItem = function (key, id) {
    const data = loadData(key).filter(x => String(x.id) !== String(id));
    saveData(key, data);
};

// Toggles
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

/* ===== Données Voyages ===== */
const JOURNEYS_KEY = "wish_journeys_v1";
const DEFAULT_COVER = "/static/img/no-image.jpg";
const GRADIENTS = ["linear-gradient(180deg, #FF6000 0%, #993A00 100%)", "linear-gradient(180deg, #00999A 0%, #003434 100%)", "linear-gradient(180deg, #9747FF 0%, #33145C 100%)", "linear-gradient(180deg, #FDC43F 0%, #991B10 100%)", "linear-gradient(180deg, #C6E2E9 0%, #6F7F83 100%)"];

const lsGetJourneys = () => { try { return JSON.parse(localStorage.getItem(JOURNEYS_KEY) || "[]"); } catch { return []; } };
const normArrLike = (a) => Array.isArray(a) ? a : (a && typeof a === "object") ? Object.values(a) : [];
const isHttp = (u) => typeof u === "string" && /^https?:\/\//i.test(u);
const isNonDefaultImg = (u) => !!u && typeof u === "string" && u.indexOf("no-image") === -1;

function deriveDays(j) {
    if (Array.isArray(j?.plan)) return j.plan.map((d, i) => ({ day: (d?.day != null ? Number(d.day) : i + 1), slots: d?.slots }));
    return []; // Simplifié pour l'accueil
}

function metaText(j) {
    const d = Array.isArray(j?.plan) ? j.plan.length : 0;
    return (j.location ? j.location + " • " : "") + d + (d > 1 ? " jours" : " jour");
}

function pickCover(j) {
    if (j?.cover) return j.cover;
    // Fallback simple
    return DEFAULT_COVER;
}

function buildStars(active = 4) {
    const wrap = document.createElement("div"); wrap.className = "stars";
    for (let i = 0; i < 5; i++) { const st = document.createElement("div"); st.className = "star" + (i < active ? " active" : ""); wrap.appendChild(st); }
    return wrap;
}

function buildRecentCard(j) {
    const item = document.createElement("div"); item.className = "featured-item";
    item.addEventListener("click", () => window.location.href = `/journeys/view/${encodeURIComponent(j.id)}`);
    const th = document.createElement("div"); th.className = "thumb"; th.style.backgroundImage = `url('${pickCover(j)}')`;
    const nm = document.createElement("div"); nm.className = "name"; nm.textContent = j?.name || "Voyage sans titre";
    const com = document.createElement("div"); com.className = "comment"; com.textContent = metaText(j);
    item.append(th, nm, com, buildStars(4));
    return item;
}

function buildStyledPlaceholderCard(i = 0) {
    const item = document.createElement("div"); item.className = "featured-item placeholder";
    const th = document.createElement("div"); th.className = "thumb"; th.style.background = GRADIENTS[i % GRADIENTS.length];
    const nm = document.createElement("div"); nm.className = "name"; nm.textContent = "Nom du voyage";
    const com = document.createElement("div"); com.className = "comment"; com.textContent = "Bientôt disponible";
    item.append(th, nm, com, buildStars(0));
    return item;
}

function renderRecent() {
    const list = document.getElementById("recentList");
    const empty = document.getElementById("recentEmpty");
    list.innerHTML = ""; empty.style.display = "none";
    const journeys = lsGetJourneys().filter(j => j && j.id != null).sort((a, b) => new Date(b?.updatedAt || 0) - new Date(a?.updatedAt || 0)).slice(0, 5);
    if (!journeys.length) empty.style.display = "block";
    journeys.forEach(j => list.appendChild(buildRecentCard(j)));
    for (let i = journeys.length; i < 5; i++) list.appendChild(buildStyledPlaceholderCard(i));
}

/* Activités avec photo (Logique simplifiée pour affichage) */
function buildActivityCard(act) {
    const card = document.createElement("div"); card.className = "proposal-item";
    const th = document.createElement("div"); th.className = "thumb"; th.style.backgroundImage = `url('${act.image}')`;
    const nm = document.createElement("div"); nm.className = "name"; nm.textContent = act.name || "Activité";
    card.append(th, nm);
    if (act.id) { card.style.cursor = "pointer"; card.addEventListener("click", () => window.location.href = `/detail-act-perso/${encodeURIComponent(act.id)}`); }
    return card;
}
function buildPlaceholderActivity(i) {
    const card = document.createElement("div"); card.className = "proposal-item placeholder";
    const th = document.createElement("div"); th.className = "thumb"; th.style.background = GRADIENTS[i % GRADIENTS.length];
    const nm = document.createElement("div"); nm.className = "name"; nm.textContent = "Suggestion";
    card.append(th, nm); return card;
}

// Fonction mock pour l'exemple (normalement parcourt les voyages pour trouver des photos)
function renderActivities() {
    const grid = document.getElementById("activitiesGrid"); grid.innerHTML = "";
    // Ici on met juste des placeholders pour l'exemple visuel demandé
    for (let i = 0; i < 5; i++) grid.appendChild(buildPlaceholderActivity(i));
}

window.addEventListener("storage", (e) => {
    if (e.key === JOURNEYS_KEY || e.key === BASKET_KEY || e.key === LIKES_KEY) {
        renderRecent(); updateCounts(); renderPanels();
    }
});

document.addEventListener("DOMContentLoaded", () => {
    renderRecent(); renderActivities(); updateCounts(); renderPanels();
});