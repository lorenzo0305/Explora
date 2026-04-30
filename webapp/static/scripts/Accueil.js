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

const lsGetJourneys = () => { try { return JSON.parse(localStorage.getItem(JOURNEYS_KEY) || "[]"); } catch { return []; } };
const normArrLike = (a) => Array.isArray(a) ? a : (a && typeof a === "object") ? Object.values(a) : [];
const isHttp = (u) => typeof u === "string" && /^https?:\/\//i.test(u);
const isPath = (u) => typeof u === "string" && u.startsWith("/");
const isNonDefaultImg = (u) => !!u && typeof u === "string" && u.indexOf("no-image") === -1;

function deriveDays(j) {
    if (Array.isArray(j?.plan)) return j.plan.map((d, i) => ({ day: (d?.day != null ? Number(d.day) : i + 1), slots: d?.slots }));
    return [];
}

function metaText(j) {
    const d = Array.isArray(j?.plan) ? j.plan.length : 0;
    return (j.location ? j.location + " • " : "") + d + (d > 1 ? " jours" : " jour");
}

/* ----- Recherche récursive d'image dans le plan ----- */
function resolveImg(u) {
    if (!u || typeof u !== "string") return "";
    u = u.trim();
    if (!u) return "";
    if (u.startsWith("data:image/")) return u;
    if (u.startsWith("//")) return "https:" + u;
    if (isHttp(u) || isPath(u)) return u;
    if (/\.(jpe?g|png|webp|gif|tiff?|bmp)$/i.test(u)) return "/static/img/phototheque/" + u.replace(/^\/+/, "");
    return "";
}
function firstImageInActivity(a) {
    if (!a || typeof a !== "object") return "";
    const candidates = [a.image, a.photo, a.picture, a.thumbnail, a.cover, a.image_url, a.imageUrl];
    for (const c of candidates) {
        const u = resolveImg(typeof c === "string" ? c : (Array.isArray(c) ? c[0] : ""));
        if (u && isNonDefaultImg(u)) return u;
    }
    return "";
}
function firstImageInDay(d) {
    if (!d) return "";
    // d.slots peut être {morning:[],noon:[],afternoon:[],evening:[]} ou {matin,midi,aprem,soir} ou un tableau de slots
    let buckets = [];
    if (d.slots && typeof d.slots === "object") {
        if (Array.isArray(d.slots)) buckets = d.slots.map(s => s.items || []);
        else ["morning", "noon", "afternoon", "evening", "matin", "midi", "aprem", "soir"].forEach(k => {
            if (Array.isArray(d.slots[k])) buckets.push(d.slots[k]);
        });
    }
    // Compatibilité quand les slots sont posés directement sur le jour (cas algo IA)
    ["matin", "midi", "aprem", "soir", "morning", "noon", "afternoon", "evening"].forEach(k => {
        if (Array.isArray(d[k])) buckets.push(d[k]);
    });
    for (const bucket of buckets) {
        for (const a of (bucket || [])) {
            const u = firstImageInActivity(a);
            if (u) return u;
        }
    }
    return "";
}
function pickCover(j) {
    // 1) cover explicite
    const c = resolveImg(j?.cover);
    if (c && isNonDefaultImg(c)) return c;
    // 2) image directe sur le voyage
    const direct = firstImageInActivity(j);
    if (direct) return direct;
    // 3) creuse dans plan / days
    const days = Array.isArray(j?.plan) ? j.plan : (Array.isArray(j?.days) ? j.days : []);
    for (const d of days) {
        const u = firstImageInDay(d);
        if (u) return u;
    }
    return DEFAULT_COVER;
}

/* ----- Récupération côté serveur (best-effort) ----- */
async function fetchServerJourneys() {
    try {
        const r = await fetch("/journeys", { cache: "no-store" });
        if (!r.ok) return [];
        const data = await r.json();
        return Array.isArray(data) ? data : (Array.isArray(data?.items) ? data.items : []);
    } catch { return []; }
}
function mergeJourneys(local, server) {
    const map = new Map();
    [...local, ...server].forEach(j => {
        if (!j || j.id == null) return;
        const id = String(j.id);
        const prev = map.get(id);
        if (!prev) { map.set(id, j); return; }
        const a = new Date(j.updatedAt || 0).getTime();
        const b = new Date(prev.updatedAt || 0).getTime();
        map.set(id, a >= b ? { ...prev, ...j } : { ...j, ...prev });
    });
    return Array.from(map.values());
}

function buildStars(active = 4) {
    const wrap = document.createElement("div"); wrap.className = "stars";
    for (let i = 0; i < 5; i++) { const st = document.createElement("div"); st.className = "star" + (i < active ? " active" : ""); wrap.appendChild(st); }
    return wrap;
}

function buildRecentCard(j) {
    const item = document.createElement("div"); item.className = "featured-item";
    item.addEventListener("click", () => window.location.href = `/journeys/view/${encodeURIComponent(j.id)}`);
    const th = document.createElement("div"); th.className = "thumb";
    const cover = pickCover(j);
    if (cover && cover !== DEFAULT_COVER) {
        th.style.backgroundImage = `url('${cover}')`;
    } else {
        th.classList.add("no-cover");
    }
    const nm = document.createElement("div"); nm.className = "name";
    nm.textContent = String(j?.name || "Voyage sans titre").replace(/\s*<br\s*\/?>\s*/gi, ' ').trim();
    const com = document.createElement("div"); com.className = "comment"; com.textContent = metaText(j);
    item.append(th, nm, com, buildStars(4));
    return item;
}

function buildPlaceholderCard() {
    // Carte vide stylisée carnet, cliquable pour démarrer un voyage
    const item = document.createElement("a");
    item.className = "featured-item placeholder";
    item.href = "/makejourney";
    const th = document.createElement("div"); th.className = "thumb";
    const nm = document.createElement("div"); nm.className = "name"; nm.textContent = "Votre prochain voyage";
    const com = document.createElement("div"); com.className = "comment"; com.textContent = "Cliquez pour le créer";
    item.append(th, nm, com);
    return item;
}

async function renderRecent() {
    const list = document.getElementById("recentList");
    const empty = document.getElementById("recentEmpty");
    if (!list) return;
    list.innerHTML = ""; if (empty) empty.style.display = "none";

    // 1) Affichage instantané depuis localStorage
    let journeys = lsGetJourneys().filter(j => j && j.id != null);

    // 2) Fusion avec serveur en tâche de fond
    const server = await fetchServerJourneys();
    journeys = mergeJourneys(journeys, server);

    journeys = journeys
        .sort((a, b) => new Date(b?.updatedAt || 0) - new Date(a?.updatedAt || 0))
        .slice(0, 5);

    list.innerHTML = "";
    if (!journeys.length) {
        if (empty) empty.style.display = "block";
        // Une seule carte placeholder discrète qui invite à créer
        list.appendChild(buildPlaceholderCard());
        return;
    }
    journeys.forEach(j => list.appendChild(buildRecentCard(j)));
    // Compléter jusqu'à 5 cartes seulement si on en a déjà au moins 1
    for (let i = journeys.length; i < 5; i++) list.appendChild(buildPlaceholderCard());
}

/* Activités avec photo (parcourt les voyages pour proposer de vraies images) */
function buildActivityCard(act) {
    const card = document.createElement("div"); card.className = "proposal-item";
    const th = document.createElement("div"); th.className = "thumb";
    const img = firstImageInActivity(act);
    if (img) th.style.backgroundImage = `url('${img}')`;
    else th.classList.add("no-cover");
    const nm = document.createElement("div"); nm.className = "name"; nm.textContent = act.name || "Activité";
    card.append(th, nm);
    if (act.id) { card.style.cursor = "pointer"; card.addEventListener("click", () => window.location.href = `/detail-act-perso/${encodeURIComponent(act.id)}`); }
    return card;
}

function collectActivitiesFromJourneys(journeys, max = 5) {
    const seen = new Set();
    const out = [];
    for (const j of journeys) {
        const days = Array.isArray(j?.plan) ? j.plan : (Array.isArray(j?.days) ? j.days : []);
        for (const d of days) {
            const buckets = [];
            if (d?.slots && typeof d.slots === "object") {
                if (Array.isArray(d.slots)) d.slots.forEach(s => buckets.push(s.items || []));
                else ["morning", "noon", "afternoon", "evening", "matin", "midi", "aprem", "soir"]
                    .forEach(k => { if (Array.isArray(d.slots[k])) buckets.push(d.slots[k]); });
            }
            ["matin", "midi", "aprem", "soir", "morning", "noon", "afternoon", "evening"]
                .forEach(k => { if (Array.isArray(d[k])) buckets.push(d[k]); });
            for (const bucket of buckets) {
                for (const a of (bucket || [])) {
                    const id = a?.id || a?.objectId || a?._id || a?.nom || a?.name;
                    const img = firstImageInActivity(a);
                    if (!img || !id || seen.has(String(id))) continue;
                    seen.add(String(id));
                    out.push({ id, name: a.name || a.nom || "Activité", image: img });
                    if (out.length >= max) return out;
                }
            }
        }
    }
    return out;
}

function renderActivities() {
    const grid = document.getElementById("activitiesGrid");
    if (!grid) return;
    grid.innerHTML = "";
    const journeys = lsGetJourneys();
    const acts = collectActivitiesFromJourneys(journeys, 5);
    acts.forEach(a => grid.appendChild(buildActivityCard(a)));
    // S'il manque, on remplit avec des placeholders
    for (let i = acts.length; i < 5; i++) {
        const card = document.createElement("div"); card.className = "proposal-item placeholder";
        const th = document.createElement("div"); th.className = "thumb";
        const nm = document.createElement("div"); nm.className = "name"; nm.textContent = "Suggestion à venir";
        card.append(th, nm); grid.appendChild(card);
    }
}

window.addEventListener("storage", (e) => {
    if (e.key === JOURNEYS_KEY || e.key === BASKET_KEY || e.key === LIKES_KEY) {
        renderRecent(); renderActivities(); updateCounts(); renderPanels();
    }
});
window.addEventListener("wishbasket:change", () => {
    renderRecent(); renderActivities(); updateCounts(); renderPanels();
});

document.addEventListener("DOMContentLoaded", () => {
    renderRecent(); renderActivities(); updateCounts(); renderPanels();
});