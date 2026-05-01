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

/* --- JOLIE NOTIFICATION FLOTTANTE --- */
function showToast(message) {
    const toast = document.createElement('div');
    toast.textContent = message;
    toast.style.cssText = "position:fixed; bottom:30px; left:50%; transform:translateX(-50%); background:#1C1C1C; color:white; padding:12px 24px; border-radius:30px; z-index:10000; font-family:'Montserrat', sans-serif; font-size:12px; font-weight:700; text-transform:uppercase; letter-spacing:0.5px; box-shadow:0 4px 15px rgba(0,0,0,0.2); opacity:0; transition:opacity 0.3s;";
    document.body.appendChild(toast);
    setTimeout(() => toast.style.opacity = '1', 10);
    setTimeout(() => { toast.style.opacity = '0'; setTimeout(() => toast.remove(), 300); }, 2500);
}

// ------- Cards -------
function cardHTML(item) {
    const img = item.image || '/static/img/no-image.jpg';
    const loc = item.locality ? `<div class="meta">${item.locality}</div>` : '';
    const name = item.name || 'Sans nom';
    const types = Array.isArray(item.types) ? item.types.join('|') : '';
    const isFav = window.WishLikes ? window.WishLikes.has(item.id) : false;
    const likedClass = isFav ? 'active' : '';

    return `<div class="card" data-id="${escAttr(item.id)}" data-name="${escAttr(name)}" data-image="${escAttr(img)}" data-types="${escAttr(types)}">
        <button class="fav-action ${likedClass}" 
                style="position:absolute; top:8px; left:8px; z-index:2; background:rgba(0,0,0,0.5);"
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
        
        // Coeur
        const fav = c.querySelector('.fav-action');
        if (fav) {
            fav.addEventListener('click', (e) => {
                e.stopPropagation();
                if (window.WishLikes) {
                    const id = c.dataset.id;
                    const types = (c.dataset.types || '').split('|').filter(Boolean);
                    window.WishLikes.toggle({ id, name: c.dataset.name, image: c.dataset.image, types });
                    window.WishLikes.refresh();
                    fav.classList.toggle('active', window.WishLikes.has(id));
                }
            });
        }

        // Panier
        const add = c.querySelector('.add-btn');
        if (add) {
            add.addEventListener('click', (e) => {
                e.stopPropagation();
                if (window.WishBasket) {
                    const id = c.dataset.id;
                    const name = c.dataset.name || 'Sans nom';
                    const image = c.dataset.image || '';
                    const types = (c.dataset.types || '').split('|').filter(Boolean);
                    window.WishBasket.add({ id, name, image, types });
                    window.WishBasket.refresh();
                    showToast('Ajouté au panier !');
                }
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
            const isFav = window.WishLikes ? window.WishLikes.has(item.id) : false;
            heartBtn.className = isFav ? 'fav-action active' : 'fav-action';
            heartBtn.textContent = '❤';
            heartBtn.dataset.id = item.id;
            heartBtn.style.background = 'transparent'; heartBtn.style.border = '1px solid #eee';
            heartBtn.style.color = heartBtn.classList.contains('active') ? '#ff4081' : '#ccc';
            heartBtn.addEventListener('click', (e) => {
                e.stopPropagation();
                if (window.WishLikes) {
                    window.WishLikes.toggle(item);
                    window.WishLikes.refresh();
                    const liked = window.WishLikes.has(item.id);
                    heartBtn.className = liked ? 'fav-action active' : 'fav-action';
                    heartBtn.style.color = liked ? '#ff4081' : '#ccc';
                }
            });

            // Add
            const addBtn = document.createElement('button');
            addBtn.className = 'add'; addBtn.textContent = '+';
            addBtn.addEventListener('click', (e) => {
                e.stopPropagation(); 
                if (window.WishBasket) {
                    window.WishBasket.add(item); 
                    window.WishBasket.refresh();
                    showToast('Ajouté au panier !');
                }
            });

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
    if(document.getElementById('regionName')) document.getElementById('regionName').textContent = displayName || 'Région';
    
    loadCategory(apiSlug, 'CulturalSite', 24);
    loadCategory(apiSlug, 'FoodEstablishment', 24);
    loadCategory(apiSlug, 'PlaceOfInterest', 24);
    
    if(searchInput) searchInput.addEventListener('input', e => doSearch(e.target.value, apiSlug));
    document.addEventListener('click', (e) => {
        if (resultsBox && !resultsBox.contains(e.target) && e.target !== searchInput) resultsBox.style.display = 'none';
    });
});