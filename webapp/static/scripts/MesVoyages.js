// --- TA BANQUE D'IMAGES (Synchronisée avec l'Accueil) ---
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
    const html = items.map((x, idx) => {
        const thumb = (x.image && !x.image.includes('no-image') && !x.image.includes('appareil_photo')) 
            ? x.image 
            : FALLBACK_IMAGES[idx % FALLBACK_IMAGES.length];
        return `
            <div class="panel-item">
              <img src="${thumb}" alt="">
              <div class="pi-name">${x.name || 'Sans nom'}</div>
              <button class="pi-remove" onclick="removeItem('${key}', '${x.id}')">✕</button>
            </div>`;
    }).join('');
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

/* ===== Logic Liste Voyages ===== */
const JOURNEYS_KEY = "wish_journeys_v1";
const lsGetJourneys = () => { try { return JSON.parse(localStorage.getItem(JOURNEYS_KEY) || "[]"); } catch { return []; } };
const lsSetJourneys = (arr) => { try { localStorage.setItem(JOURNEYS_KEY, JSON.stringify(arr)); } catch { } };

function activitiesCount(j) {
    if (!Array.isArray(j?.plan)) return 0;
    return j.plan.reduce((acc, d) => {
        if (Array.isArray(d?.matin) || Array.isArray(d?.aprem)) {
            return acc + (Array.isArray(d.matin) ? d.matin.length : 0) + (Array.isArray(d.aprem) ? d.aprem.length : 0);
        }
        const s = d?.slots || {};
        const c = (arr) => Array.isArray(arr) ? arr.length : (arr ? Object.values(arr).length : 0);
        return acc + c(s.morning) + c(s.noon) + c(s.afternoon) + c(s.evening);
    }, 0);
}

function metaText(j) {
    const d = Array.isArray(j?.plan) ? j.plan.length : 1;
    const a = activitiesCount(j);
    return (j.location ? j.location + " • " : "") + d + (d > 1 ? " jours" : " jour") + " • " + a + (a > 1 ? " activités" : " activité");
}

function pickCover(j) {
    if (j?.cover && !j.cover.includes('no-image') && !j.cover.includes('appareil_photo')) return j.cover;
    return getShuffledFallbacks(j?.id)[0];
}

function renderJourneys(journeys) {
    const listEl = document.getElementById("topicsList");
    const emptyEl = document.getElementById("emptyState");
    if (!listEl) return;
    listEl.innerHTML = "";

    if (!journeys.length) { emptyEl.style.display = "block"; return; }
    emptyEl.style.display = "none";

    const esc = (s) => String(s ?? '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');

    journeys.forEach((j) => {
        const item = document.createElement("div"); 
        item.className = "item";
        
        // C'EST ICI : On ajoute le curseur main !
        item.style.cursor = "pointer";
        
        const isEditor = j.source === 'editor' || j.source === 'Editor';
        const sourceText = isEditor ? "Voyage conçu manuellement" : "Voyage généré par IA";
        
        const safeName = esc(j.name || "Voyage");
        const formattedName = safeName.replace(/\s*[—\-]\s*/, '<br>');

        item.innerHTML = `
          <div class="left">
            <div class="thumb" style="background-image:url('${esc(pickCover(j))}')"></div>
            <div class="meta">
              <div class="name">${formattedName}</div>
              <div class="desc">${esc(metaText(j))}</div>
            </div>
          </div>
          <div class="actions-right">
            <button class="icon-btn delete-btn" title="Supprimer">
              <svg viewBox="0 0 24 24"><path d="M3 6h18"/><path d="M8 6v-2a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2"/><path d="M19 6l-1 14a2 2 0 0 1-2 2H8a2 2 0 0 1-2-2L5 6"/><path d="M10 11v6"/><path d="M14 11v6"/></svg>
            </button>
          </div>
          <!-- Le petit texte positionné tout en bas -->
          <div style="position: absolute; bottom: 12px; left: 0; width: 100%; text-align: center; pointer-events: none;">
            <span class="source-label" style="font-family: 'Montserrat', sans-serif; font-size: 7.5px; font-weight: 700; text-transform: uppercase; letter-spacing: 0.3px; color: #1C1C1C; opacity: 0.5;">
                ${sourceText}
            </span>
          </div>`;

        item.querySelector('.delete-btn').addEventListener('click', async (e) => {
            e.stopPropagation();
            if (!confirm("Supprimer ce voyage ?")) return;
            lsSetJourneys(lsGetJourneys().filter(x => String(x.id) !== String(j.id)));
            loadJourneys();
        });

        item.addEventListener("click", () => window.location.href = "/journeys/view/" + encodeURIComponent(j.id));
        listEl.appendChild(item);
    });
}

async function loadJourneys() {
    const cached = lsGetJourneys();
    renderJourneys(cached.sort((a, b) => new Date(b?.updatedAt || 0) - new Date(a?.updatedAt || 0)));
}

document.addEventListener("DOMContentLoaded", () => {
    loadJourneys(); updateCounts(); renderPanels();
});