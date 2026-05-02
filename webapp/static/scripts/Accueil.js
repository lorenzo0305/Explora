// --- /static/scripts/Accueil.js ---

const JOURNEYS_KEY = "wish_journeys_v1";

const FALLBACK_IMAGES = [
    '/static/img/baie_de_somme2.jpg',
    '/static/img/auvergne.jpg',
    '/static/img/montagne_france2.jpg',
    '/static/img/boeuf_bourguignon.jpg',
    '/static/img/semur_en_auxois.jpg',
    '/static/img/vtt.jpg'
];

function stringToHash(str) {
    let hash = 0;
    for (let i = 0; i < str.length; i++) { hash = str.charCodeAt(i) + ((hash << 5) - hash); }
    return Math.abs(hash);
}

function getShuffledFallbacks(journeyId) {
    let seed = stringToHash(String(journeyId || "default"));
    let arr = [...FALLBACK_IMAGES];
    for (let i = arr.length - 1; i > 0; i--) {
        seed = (seed * 9301 + 49297) % 233280;
        let rand = seed / 233280;
        let j = Math.floor(rand * (i + 1));
        [arr[i], arr[j]] = [arr[j], arr[i]];
    }
    return arr;
}

const lsGetJourneys = () => { try { return JSON.parse(localStorage.getItem(JOURNEYS_KEY) || "[]"); } catch { return []; } };

function metaText(j) {
    const d = Array.isArray(j?.plan) ? j.plan.length : 0;
    return (j.location ? j.location + " • " : "") + d + (d > 1 ? " jours" : " jour");
}

// LA CORRECTION : La fonction pickCover est désormais un copié-collé strict de celle de "MesVoyages.js"
function pickCover(j) {
    if (j?.cover && !j.cover.includes('no-image') && !j.cover.includes('appareil_photo') && !j.cover.includes('no-img')) {
        return j.cover;
    }
    return getShuffledFallbacks(j?.id)[0];
}

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
    th.style.backgroundImage = `url('${cover}')`;
    
    const nm = document.createElement("div"); nm.className = "name";
    nm.textContent = String(j?.name || "Voyage sans titre").replace(/\s*<br\s*\/?>\s*/gi, ' ').trim();
    const com = document.createElement("div"); com.className = "comment"; com.textContent = metaText(j);
    item.append(th, nm, com, buildStars(4));
    return item;
}

function buildPlaceholderCard() {
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

    let journeys = lsGetJourneys().filter(j => j && j.id != null);
    const server = await fetchServerJourneys();
    journeys = mergeJourneys(journeys, server);

    journeys = journeys
        .sort((a, b) => new Date(b?.updatedAt || 0) - new Date(a?.updatedAt || 0))
        .slice(0, 5);

    list.innerHTML = "";
    if (!journeys.length) {
        if (empty) empty.style.display = "block";
        list.appendChild(buildPlaceholderCard());
        return;
    }
    journeys.forEach(j => list.appendChild(buildRecentCard(j)));
    for (let i = journeys.length; i < 5; i++) list.appendChild(buildPlaceholderCard());
}

window.addEventListener("storage", (e) => {
    if (e.key === JOURNEYS_KEY) { renderRecent(); }
});
window.addEventListener("wishbasket:change", () => {
    renderRecent(); 
});

document.addEventListener("DOMContentLoaded", () => {
    renderRecent(); 
});