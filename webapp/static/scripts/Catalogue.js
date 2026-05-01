/* ================================
 * MODE DIAG
 * ================================ */
const DEBUG = new URLSearchParams(location.search).has('debug') || localStorage.getItem('wish_debug') === '1';
const NO_IMG = '/static/img/no-image.jpg';
function dbg(...args) { if (DEBUG) console.log('[EXP-Debug]', ...args); }

/* Horloge */
(function clock() {
    const el = document.getElementById('clock');
    if (!el) return;
    const tick = () => { const d = new Date(); el.textContent = d.toLocaleTimeString('fr-FR', { hour: '2-digit', minute: '2-digit' }); };
    tick(); setInterval(tick, 15000);
})();

/* ---------------- Helpers ---------------- */
function firstString() {
    for (let i = 0; i < arguments.length; i++) {
        const v = arguments[i];
        if (!v) continue;
        if (typeof v === 'string' && v.trim()) return v.trim();
        if (Array.isArray(v)) {
            for (const e of v) {
                if (typeof e === 'string' && e.trim()) return e.trim();
                if (e && typeof e === 'object') {
                    const s = e['@value'] || e.value || e.url || e['@id'];
                    if (typeof s === 'string' && s.trim()) return s.trim();
                }
            }
        }
        if (typeof v === 'object') {
            const s = v['@value'] || v.value || v.url || v['@id'];
            if (typeof s === 'string' && s.trim()) return s.trim();
        }
    }
    return '';
}
function anyToArray(x) { return !x ? [] : (Array.isArray(x) ? x : [x]); }

/* -------- Résolution d'image -------- */
function _resolveBestImage(obj) {
    if (!obj || typeof obj !== 'object') return { url: NO_IMG, how: 'none', key: null, note: 'obj invalide' };
    const flatKey = ['image', 'photo', 'thumbnail', 'picture', 'cover', 'https://schema.org/image', 'image_url', 'media', 'thumb']
        .find(k => {
            const v = firstString(obj[k]);
            return v && /^https?:\/\//i.test(v);
        });
    if (flatKey) return { url: firstString(obj[flatKey]), how: 'flat', key: flatKey, note: '' };

    for (const [k, val] of Object.entries(obj || {})) {
        const arr = anyToArray(val);
        for (const it of arr) {
            if (it && typeof it === 'object') {
                const u = firstString(it.contentUrl, it.url, it['@id']);
                if (u && /^https?:\/\/.+\.(jpg|jpeg|png|webp|gif)(\?|#|$)/i.test(u)) return { url: u, how: 'nested', key: k, note: '' };
            }
        }
    }
    return { url: NO_IMG, how: 'none', key: null, note: 'aucune correspondance' };
}
function getBestImage(obj) {
    const r = _resolveBestImage(obj);
    return r.url || NO_IMG;
}
function attachImgFallback(img) {
    img.addEventListener('error', () => {
        if (img.dataset.fbk) return;
        img.dataset.fbk = '1';
        img.src = NO_IMG;
    }, { once: true });
}

/* -------- Fetch debug -------- */
async function fetchJSONDebug(url, opts = {}) {
    try {
        const r = await fetch(url, opts);
        const txt = await r.text();
        let data = null;
        try { data = JSON.parse(txt); } catch (_) { }
        return { ok: r.ok, status: r.status, json: data };
    } catch (e) {
        if (e && e.name === 'AbortError') return { ok: false, aborted: true };
        return { ok: false, status: 0, error: e };
    }
}
function scheduleUpgrade(img, item) { }
function debounce(fn, wait = 350) {
    let t; return (...args) => { clearTimeout(t); t = setTimeout(() => fn(...args), wait); };
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

/* -------------- Recherche -------------- */
const resultsBox = document.getElementById('results');
const searchInput = document.getElementById('search');

const S = { q: '', loading: false, offset: 0, limit: 30, reachedEnd: false, next: null, ctrl: null, token: 0 };

function clearResults() {
    if(resultsBox) { resultsBox.innerHTML = ''; resultsBox.style.display = 'none'; }
    const sections = document.querySelector('.sections-container');
    if (sections) sections.style.display = 'flex';
    S.loading = false; S.offset = 0; S.reachedEnd = false; S.next = null;
}
function appendLoader() {
    const d = document.createElement('div'); d.className = 'results-loader'; d.textContent = 'Chargement...';
    if(resultsBox) resultsBox.appendChild(d);
}
function removeLoader() {
    const l = resultsBox?.querySelector('.results-loader'); if (l) l.remove();
}
function showEnd() {
    const e = document.createElement('div'); e.className = 'results-end'; e.textContent = 'Fin des résultats';
    if(resultsBox) resultsBox.appendChild(e);
}

function renderResults(items, append = false) {
    if (!resultsBox) return;
    if (!append) resultsBox.innerHTML = '';
    if (!items || !items.length) {
        if (!append) resultsBox.innerHTML = '<div class="no-res">Aucun résultat</div>';
        else showEnd();
    } else {
        const seen = new Set(Array.from(resultsBox.querySelectorAll('.result-item')).map(r => r.getAttribute('data-id')));
        items.forEach(item => {
            const id = item.id || item._id || item.identifier || item['@id'] || item.url || '';
            if (!id || seen.has(String(id))) return;

            const row = document.createElement('div');
            row.className = 'result-item';
            row.setAttribute('data-id', id);

            // HEADER (la partie toujours visible)
            const header = document.createElement('div');
            header.className = 'result-header';

            const left = document.createElement('div'); left.className = 'result-left';
            const img = document.createElement('img'); img.className = 'result-thumb';
            const resolved = getBestImage(item);
            img.src = resolved || NO_IMG;
            img.alt = item.name || 'Résultat';
            attachImgFallback(img);

            const meta = document.createElement('div');
            const name = document.createElement('div'); name.className = 'result-name'; name.textContent = item.name || 'Sans nom';
            const type = document.createElement('div'); type.style.fontSize = '12px'; type.style.opacity = '.75'; type.textContent = item.locality || item.region || '';
            meta.appendChild(name); meta.appendChild(type);
            left.appendChild(img); left.appendChild(meta);

            const actionsDiv = document.createElement('div');
            actionsDiv.style.display = 'flex'; actionsDiv.style.gap = '8px'; actionsDiv.style.alignItems = 'center';

            // Coeur
            const heartBtn = document.createElement('button');
            const isLiked = window.WishLikes ? window.WishLikes.has(id) : false;
            heartBtn.className = isLiked ? 'fav-action active' : 'fav-action';
            heartBtn.textContent = '❤';
            heartBtn.dataset.id = id;
            // Hover supprimé !
            heartBtn.addEventListener('click', (e) => {
                e.stopPropagation();
                if(window.WishLikes) {
                    window.WishLikes.toggle({ ...item, id, image: resolved });
                    window.WishLikes.refresh();
                    heartBtn.className = window.WishLikes.has(id) ? 'fav-action active' : 'fav-action';
                }
            });

            // Bouton + PANIER
            const addBtn = document.createElement('button');
            addBtn.className = 'result-add-btn';
            addBtn.textContent = '+ PANIER';
            addBtn.addEventListener('click', (e) => {
                e.stopPropagation(); 
                if(window.WishBasket) {
                    window.WishBasket.add({ ...item, id, image: resolved }); 
                    window.WishBasket.refresh();
                    showToast('Ajouté au panier !');
                }
            });

            // Chevron pour ouvrir/fermer la description
            const chevron = document.createElement('div');
            chevron.innerHTML = '❯';
            chevron.style.marginLeft = '10px'; chevron.style.transition = 'transform 0.3s'; chevron.style.color = '#ccc';

            actionsDiv.appendChild(heartBtn);
            actionsDiv.appendChild(addBtn);
            actionsDiv.appendChild(chevron);

            header.appendChild(left);
            header.appendChild(actionsDiv);

            // CORPS (La description cachée)
            const details = document.createElement('div');
            details.className = 'result-details';
            const desc = item.description || "Aucune description détaillée n'est disponible pour cette activité. Laissez-vous surprendre sur place !";
            details.innerHTML = `<p style="margin:0;">${desc}</p>`;

            // L'accordéon s'ouvre/se ferme quand on clique sur le header
            header.addEventListener('click', (e) => {
                if (e.target.closest('button')) return; // Ne s'ouvre pas si on clique sur un bouton
                const isOpen = details.style.display === 'block';
                details.style.display = isOpen ? 'none' : 'block';
                chevron.style.transform = isOpen ? 'rotate(0deg)' : 'rotate(90deg)';
            });

            row.appendChild(header);
            row.appendChild(details);
            resultsBox.appendChild(row);

            if (!resolved || resolved === NO_IMG) scheduleUpgrade(img, item);
        });
    }
    resultsBox.style.display = 'block';
}

async function fetchMore(myToken = S.token) {
    if (S.loading || S.reachedEnd || !S.q) return;
    S.loading = true; appendLoader();
    try {
        const url = S.next ? S.next : `/search?query=${encodeURIComponent(S.q)}&offset=${S.offset}&limit=${S.limit}`;
        const resp = await fetchJSONDebug(url, { signal: S.ctrl ? S.ctrl.signal : undefined });
        if (resp.aborted || myToken !== S.token) return;
        if (!resp.ok) { removeLoader(); return; }

        let batch = [];
        if (Array.isArray(resp.json)) {
            batch = resp.json; S.offset += batch.length; if (batch.length < S.limit) S.reachedEnd = true;
        } else if (resp.json && Array.isArray(resp.json.items)) {
            batch = resp.json.items;
            if (typeof resp.json.next_offset === 'number') S.offset = resp.json.next_offset;
            else if (batch.length < S.limit) S.reachedEnd = true; else S.offset += batch.length;
        } else {
            S.reachedEnd = true;
        }
        removeLoader();
        renderResults(batch, true);
        if (S.reachedEnd) showEnd();
    } finally {
        S.loading = false;
    }
}

async function startSearch(q) {
    const val = (q || '').trim();
    if (val.length < 2) {
        if (S.ctrl) S.ctrl.abort();
        clearResults(); S.q = ''; return;
    }
    if (S.ctrl) S.ctrl.abort();
    S.ctrl = new AbortController();
    S.token++; const myToken = S.token;
    S.q = val; S.offset = 0; S.reachedEnd = false; S.next = null;
    if(resultsBox) { resultsBox.innerHTML = ''; resultsBox.style.display = 'block'; }
    await fetchMore(myToken);
}

// Clics en dehors de la boîte de recherche pour la fermer
document.addEventListener('click', (e) => {
    if (resultsBox && !resultsBox.contains(e.target) && e.target !== searchInput) {
        resultsBox.style.display = 'none';
    }
});

if(resultsBox) {
    resultsBox.addEventListener('scroll', () => {
        const nearBottom = resultsBox.scrollTop + resultsBox.clientHeight >= resultsBox.scrollHeight - 80;
        if (nearBottom) fetchMore(S.token);
    });
}

if(searchInput) {
    const handleInput = debounce(e => startSearch(e.target.value), 350);
    searchInput.addEventListener('input', handleInput);

    // Quand on reclique sur la barre, on réaffiche les résultats sans avoir besoin de retaper
    searchInput.addEventListener('click', () => {
        if (searchInput.value.trim().length >= 2 && resultsBox.children.length > 0) {
            resultsBox.style.display = 'block';
        }
    });
}

/* -------------- Sections statiques -------------- */
function createRegionCard({ name, href, active, bg }) {
    const cardBox = document.createElement('div');
    cardBox.className = 'card-box' + (active ? '' : ' disabled');
    if (bg) cardBox.style.background = bg;

    const cardName = document.createElement('span');
    cardName.className = 'card-name';
    cardName.textContent = name;

    const cardThumb = document.createElement('div');
    cardThumb.className = 'card-thumb';

    cardBox.appendChild(cardName);
    cardBox.appendChild(cardThumb);

    if (active && href) {
        cardBox.addEventListener('click', () => location.href = href);
    }
    return cardBox;
}
function renderRegionsGrid() {
    const grid = document.getElementById('regions-grid');
    if (!grid) return;
    grid.innerHTML = '';
    const regions = [
        { name: 'Hauts-de-France', href: '/region/Hauts-de-France', active: true, bg: 'linear-gradient(160deg,#239BB9,#104553)' },
        { name: 'Auvergne-Rhône-Alpes', href: '/region/Auvergne-Rhône-Alpes', active: true, bg: 'linear-gradient(160deg,#7BC6CC,#264653)' },
        { name: 'Bientôt…', active: false },
        { name: 'Bientôt…', active: false },
    ];
    regions.forEach(r => grid.appendChild(createRegionCard(r)));
}
function renderPlaceholders(id, n = 4) {
    const grid = document.getElementById(id);
    if (!grid) return;
    grid.innerHTML = '';
    for (let i = 0; i < n; i++) {
        const el = document.createElement('div');
        el.className = 'card-box disabled';
        const name = document.createElement('span'); name.className = 'card-name'; name.textContent = 'Bientôt…';
        const th = document.createElement('div'); th.className = 'card-thumb';
        el.append(name, th);
        grid.appendChild(el);
    }
}

/* -------------- Boot -------------- */
document.addEventListener('DOMContentLoaded', () => {
    renderRegionsGrid();
    renderPlaceholders('modes-grid', 4);
    renderPlaceholders('cities-grid', 4);
    const params = new URLSearchParams(location.search);
    const initialQ = params.get('q') || '';
    if (initialQ && searchInput) {
        searchInput.value = initialQ;
        startSearch(initialQ);
    }
    
    // Mise à jour de tous les cœurs si on charge la page
    window.addEventListener('wishbasket:change', () => {
        document.querySelectorAll('.fav-action').forEach(btn => {
            const id = btn.dataset.id;
            if(id && window.WishLikes) {
                btn.className = window.WishLikes.has(id) ? 'fav-action active' : 'fav-action';
            }
        });
    });
});