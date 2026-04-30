// ------- Helpers -------
const $ = s => document.querySelector(s);
function debounce(fn, wait = 250) { let t; return (...a) => { clearTimeout(t); t = setTimeout(() => fn(...a), wait); }; }
function getSlugFromPath() {
    const parts = location.pathname.split('/').filter(Boolean);
    return decodeURIComponent(parts[parts.length - 1] || '');
}
const escAttr = s => String(s ?? '').replace(/"/g, '&quot;');

// -------- Normalisation région --------
const REGION_ALIASES = {
    "hauts-de-france": "Hauts-de-France",
    "hdf": "Hauts-de-France",
    "auvergne-rhone-alpes": "Auvergne-Rhône-Alpes",
    "ara": "Auvergne-Rhône-Alpes",
};
function toAsciiSlug(s) {
    return String(s || "").normalize('NFD').replace(/[\u0300-\u036f]/g, '').toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-+|-+$/g, '');
}
function resolveRegion(slugFromUrl) {
    const key = toAsciiSlug(slugFromUrl);
    const display = REGION_ALIASES[key] || slugFromUrl;
    const apiSlug = display;
    return { displayName: display, apiSlug };
}

/* ---------------- FAVORIS (LIKES) ---------------- */
const LIKES_KEY = 'wish_likes_v1';
const likesIcon = document.getElementById('likesIcon');
const likesCount = document.getElementById('likesCount');
const floatingLikes = document.getElementById('floatingLikes');

function loadLikes() {
    try { const raw = localStorage.getItem(LIKES_KEY); return raw ? JSON.parse(raw) : []; } catch (e) { return []; }
}
function saveLikes(items) {
    localStorage.setItem(LIKES_KEY, JSON.stringify(items));
    updateLikesCount(); renderLikesPanel();
    // Rafraichir les boutons visibles
    document.querySelectorAll('.fav-action').forEach(btn => {
        // Pour les cartes (qui ont l'id sur le parent)
        if (btn.parentElement.dataset.id) {
            if (isLiked(btn.parentElement.dataset.id)) btn.classList.add('active');
            else btn.classList.remove('active');
        }
        // Pour les résultats de recherche (qui ont l'id sur eux-mêmes)
        if (btn.dataset.id) {
            if (isLiked(btn.dataset.id)) btn.classList.add('active');
            else btn.classList.remove('active');
        }
    });
}
function updateLikesCount() {
    const n = loadLikes().length;
    likesCount.textContent = n; likesCount.hidden = n === 0;
}
function isLiked(id) {
    return loadLikes().some(x => String(x.id) === String(id));
}
function toggleLike(item) {
    let items = loadLikes();
    const idStr = String(item.id);
    if (items.some(x => String(x.id) === idStr)) {
        items = items.filter(x => String(x.id) !== idStr);
    } else {
        const img = (item.image && item.image !== '/static/img/no-image.jpg') ? item.image : '/static/img/no-image.jpg';
        items.push({ id: item.id, name: item.name, image: img, types: item.types || [] });
    }
    saveLikes(items);
    return isLiked(item.id);
}
function renderLikesPanel() {
    const items = loadLikes();
    if (!items.length) {
        floatingLikes.innerHTML = '<h4>Mes Favoris</h4><p class="likes-empty">Aucun coup de cœur pour l’instant.</p>';
        return;
    }
    const list = items.map(x => (
        `<div class="basket-item">
          <img src="${x.image || '/static/img/no-image.jpg'}" alt="">
          <div>
            <div class="bi-name">${x.name || 'Sans nom'}</div>
            <div class="bi-meta">${(x.types && x.types[0]) ? x.types[0] : ''}</div>
          </div>
          <button class="bi-like-remove" onclick="event.stopPropagation(); toggleLike({id:'${x.id}'})">💔</button>
        </div>`
    )).join('');
    floatingLikes.innerHTML = '<h4>Mes Favoris</h4>' + list;
}
likesIcon.addEventListener('click', (e) => {
    e.stopPropagation();
    const visible = floatingLikes.style.display === 'block';
    if (floatingBasket) floatingBasket.style.display = 'none';
    floatingLikes.style.display = visible ? 'none' : 'block';
    if (!visible) renderLikesPanel();
});
document.addEventListener('click', (e) => {
    if (!floatingLikes.contains(e.target) && e.target !== likesIcon) floatingLikes.style.display = 'none';
});
updateLikesCount();

// ---------- Panier ----------
const BASKET_KEY = 'wish_basket_v1';
const basketIcon = document.getElementById('basketIcon');
const basketCount = document.getElementById('basketCount');
const floatingBasket = document.getElementById('floatingBasket');

function loadBasket() {
    try { const raw = localStorage.getItem(BASKET_KEY); return raw ? JSON.parse(raw) : []; } catch (e) { return []; }
}
function saveBasket(items) {
    localStorage.setItem(BASKET_KEY, JSON.stringify(items));
    updateBasketCount(); renderBasketPanel();
}
function updateBasketCount() {
    const n = loadBasket().length;
    basketCount.textContent = n; basketCount.hidden = n === 0;
}
function addToBasket(item) {
    const items = loadBasket();
    if (!items.some(x => String(x.id) === String(item.id))) {
        items.push({ id: item.id, name: item.name, image: item.image || '', types: item.types || [] });
        saveBasket(items);
    }
}
function removeFromBasket(id) {
    const items = loadBasket().filter(x => String(x.id) !== String(id));
    saveBasket(items);
}
function renderBasketPanel() {
    const items = loadBasket();
    if (!items.length) {
        floatingBasket.innerHTML = '<h4>Votre panier</h4><p class="basket-empty">Aucun élément pour l\u2019instant.</p><div class="basket-footer"><a href="/creation">Aller à la création</a></div>';
        return;
    }
    const list = items.map(x => (
        `<div class="basket-item">
          <img src="${x.image || '/static/img/no-image.jpg'}" alt="">
          <div>
            <div class="bi-name">${x.name || 'Sans nom'}</div>
            <div class="bi-meta">${(x.types && x.types[0]) ? x.types[0] : ''}</div>
          </div>
          <button class="bi-remove" data-id="${x.id}">Retirer</button>
        </div>`
    )).join('');
    floatingBasket.innerHTML = '<h4>Votre panier</h4>' + list + '<div class="basket-footer"><a href="/creation">Aller à la création</a></div>';
    floatingBasket.querySelectorAll('.bi-remove').forEach(btn => {
        btn.addEventListener('click', (e) => { e.stopPropagation(); removeFromBasket(btn.getAttribute('data-id')); });
    });
}
basketIcon.addEventListener('click', (e) => {
    e.stopPropagation();
    const visible = floatingBasket.style.display === 'block';
    if (floatingLikes) floatingLikes.style.display = 'none';
    floatingBasket.style.display = visible ? 'none' : 'block';
    if (!visible) renderBasketPanel();
});
document.addEventListener('click', (e) => {
    if (!floatingBasket.contains(e.target) && e.target !== basketIcon) floatingBasket.style.display = 'none';
});
updateBasketCount();

// ------- Cards -------
function cardHTML(item) {
    const img = item.image || '/static/img/no-image.jpg';
    const loc = item.locality ? `<div class="meta">${item.locality}</div>` : '';
    const name = item.name || 'Sans nom';
    const types = Array.isArray(item.types) ? item.types.join('|') : '';
    const likedClass = isLiked(item.id) ? 'active' : '';

    return `<div class="card" data-id="${escAttr(item.id)}" data-name="${escAttr(name)}" data-image="${escAttr(img)}" data-types="${escAttr(types)}">
        <button class="fav-action ${likedClass}" 
                style="position:absolute; top:8px; left:8px; z-index:2; background:rgba(0,0,0,0.5);"
                onclick="event.stopPropagation(); var p=this.parentElement; toggleLike({id:p.dataset.id, name:p.dataset.name, image:p.dataset.image, types:p.dataset.types.split('|')});"
                title="Mettre en favoris">❤</button>
        
        <button class="add-btn" title="Ajouter au panier" aria-label="Ajouter au panier">+</button>
        <div class="thumb" style="background-image:url('${img}')"></div>
        <div class="name">${name}</div>
        ${loc}
      </div>`;
}
function bindCards(container) {
    container.querySelectorAll('.card').forEach(c => {
        c.addEventListener('click', () => {
            const id = c.getAttribute('data-id');
            if (id) location.href = `/object/${encodeURIComponent(id)}`;
        });
        const add = c.querySelector('.add-btn');
        if (add) {
            add.addEventListener('click', (e) => {
                e.stopPropagation();
                const id = c.dataset.id;
                const name = c.dataset.name || 'Sans nom';
                const image = c.dataset.image || '';
                const types = (c.dataset.types || '').split('|').filter(Boolean);
                addToBasket({ id, name, image, types });
            });
        }
    });
}

async function loadCategory(apiSlug, typ, limit = 24) {
    const row = document.getElementById(`row-${typ}`);
    if (!row) return;
    row.innerHTML = '';
    try {
        const r = await fetch(`/regions/${encodeURIComponent(apiSlug)}/cards?type=${encodeURIComponent(typ)}&limit=${limit}`);
        const items = r.ok ? await r.json() : [];
        row.innerHTML = items.map(cardHTML).join('') || '<div style="opacity:.7">Aucun élément</div>';
        bindCards(row);
    } catch (e) {
        row.innerHTML = '<div style="color:#f88">Erreur de chargement</div>';
    }
}

// ------- Recherche locale -------
const resultsBox = document.getElementById('results');
const searchInput = document.getElementById('search');
function clearResults() { resultsBox.innerHTML = ''; resultsBox.style.display = 'none'; }
function renderResults(items) {
    resultsBox.innerHTML = '';
    if (!items || !items.length) {
        resultsBox.innerHTML = '<div class="no-res">Aucun résultat</div>';
    } else {
        items.forEach(item => {
            const row = document.createElement('div');
            row.className = 'result-item';
            row.addEventListener('click', () => { location.href = `/object/${encodeURIComponent(item.id)}`; });

            const left = document.createElement('div'); left.className = 'result-left';
            const img = document.createElement('img'); img.className = 'result-thumb'; img.src = item.image || '/static/img/no-image.jpg'; img.alt = item.name || 'Résultat';
            const meta = document.createElement('div');
            const name = document.createElement('div'); name.className = 'result-name'; name.textContent = item.name || 'Sans nom';
            const type = document.createElement('div'); type.style.fontSize = '12px'; type.style.opacity = '.75'; type.textContent = (item.types && item.types[0]) ? item.types[0] : '';
            meta.append(name, type); left.append(img, meta);

            // Container Actions (Like + Add)
            const right = document.createElement('div');
            right.className = 'result-add';
            right.style.display = 'flex'; right.style.gap = '8px'; right.style.alignItems = 'center';

            // Like
            const heartBtn = document.createElement('button');
            heartBtn.className = isLiked(item.id) ? 'fav-action active' : 'fav-action';
            heartBtn.textContent = '❤';
            heartBtn.dataset.id = item.id;
            heartBtn.style.background = 'transparent'; heartBtn.style.border = '1px solid #eee';
            heartBtn.style.color = heartBtn.classList.contains('active') ? '#ff4081' : '#ccc';
            heartBtn.addEventListener('click', (e) => {
                e.stopPropagation();
                const liked = toggleLike(item);
                heartBtn.className = liked ? 'fav-action active' : 'fav-action';
                heartBtn.style.color = liked ? '#ff4081' : '#ccc';
            });

            // Add
            const addBtn = document.createElement('button');
            addBtn.className = 'add'; addBtn.textContent = '+';
            addBtn.addEventListener('click', (e) => { e.stopPropagation(); addToBasket(item); });

            right.appendChild(heartBtn);
            right.appendChild(addBtn);

            row.append(left, right);
            resultsBox.append(row);
        });
    }
    resultsBox.style.display = 'block';
}

let searchSeq = 0;
const doSearch = debounce(async function (q, apiSlug) {
    const query = (q || '').trim();
    if (query.length < 2) { clearResults(); return; }
    const mySeq = ++searchSeq;
    resultsBox.innerHTML = '<div class="results-loader">Recherche…</div>';
    resultsBox.style.display = 'block';
    try {
        const r = await fetch(`/regions/${encodeURIComponent(apiSlug)}/cards?q=${encodeURIComponent(query)}&limit=60`, { cache: 'no-store' });
        if (mySeq !== searchSeq) return;
        if (!r.ok) { clearResults(); return; }
        const data = await r.json();
        renderResults(data);
    } catch (e) {
        if (mySeq !== searchSeq) return;
        clearResults();
    }
}, 250);

// ------- Init -------
document.addEventListener('DOMContentLoaded', () => {
    const rawSlug = getSlugFromPath();
    const { displayName, apiSlug } = resolveRegion(rawSlug);
    document.getElementById('regionName').textContent = displayName || 'Région';
    loadCategory(apiSlug, 'CulturalSite', 24);
    loadCategory(apiSlug, 'FoodEstablishment', 24);
    loadCategory(apiSlug, 'PlaceOfInterest', 24);
    searchInput.addEventListener('input', e => doSearch(e.target.value, apiSlug));
    document.addEventListener('click', (e) => {
        if (!resultsBox.contains(e.target) && e.target !== searchInput) resultsBox.style.display = 'none';
    });
});