/* ================================
 * MODE DIAG & HELPERS
 * ================================ */
const DEBUG = new URLSearchParams(location.search).has('debug') || localStorage.getItem('wish_debug') === '1';
function dbg(...args) { if (DEBUG) console.log('[EXP-Debug]', ...args); }

/* --- JOLIE NOTIFICATION FLOTTANTE --- */
function showToast(message) {
    const toast = document.createElement('div');
    toast.textContent = message;
    toast.style.cssText = "position:fixed; bottom:30px; left:50%; transform:translateX(-50%); background:#1C1C1C; color:white; padding:12px 24px; border-radius:30px; z-index:10000; font-family:'Montserrat', sans-serif; font-size:12px; font-weight:700; text-transform:uppercase; letter-spacing:0.5px; box-shadow:0 4px 15px rgba(0,0,0,0.2); opacity:0; transition:opacity 0.3s;";
    document.body.appendChild(toast);
    setTimeout(() => toast.style.opacity = '1', 10);
    setTimeout(() => { toast.style.opacity = '0'; setTimeout(() => toast.remove(), 300); }, 2500);
}

/* -------- Résolution d'image avec Dictionnaire -------- */
const NO_IMG = '/static/img/travel.jpg'; 

function getSafeImage(item) {
    if (!item) return NO_IMG;
    if (typeof window !== 'undefined' && typeof window.getActivityImage === 'function') {
        try {
            return window.getActivityImage(item);
        } catch(e) {
            console.warn("Erreur dictionnaire:", e);
        }
    }
    return item.image || item.photo || item.cover || NO_IMG;
}

/* -------- Fetch & Debounce -------- */
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

function debounce(fn, wait = 350) {
    let t; return (...args) => { clearTimeout(t); t = setTimeout(() => fn(...args), wait); };
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
    const d = document.createElement('div'); d.className = 'results-loader'; d.textContent = 'Recherche en cours...';
    if(resultsBox) resultsBox.appendChild(d);
}

function removeLoader() {
    const l = resultsBox?.querySelector('.results-loader'); if (l) l.remove();
}

function showEnd() {
    if (resultsBox && !resultsBox.querySelector('.results-end')) {
        const e = document.createElement('div'); 
        e.className = 'results-end'; 
        e.textContent = 'Fin des résultats';
        resultsBox.appendChild(e);
    }
}

function renderResults(items, append = false) {
    if (!resultsBox) return;
    if (!append) resultsBox.innerHTML = '';
    
    if (!items || !items.length) {
        resultsBox.innerHTML = '<div class="no-res">Aucun résultat pour cette recherche.</div>';
        resultsBox.style.display = 'block';
        return;
    } 

    const seen = new Set(Array.from(resultsBox.querySelectorAll('.result-item')).map(r => r.getAttribute('data-id')));
    
    items.forEach(item => {
        const id = item.id || item._id || item.identifier || item['@id'] || item.url || '';
        if (!id || seen.has(String(id))) return;

        const row = document.createElement('div');
        row.className = 'result-item';
        row.setAttribute('data-id', id);

        const header = document.createElement('div');
        header.className = 'result-header';

        const left = document.createElement('div'); left.className = 'result-left';
        
        const img = document.createElement('img'); img.className = 'result-thumb';
        const resolved = getSafeImage(item);
        img.src = resolved;
        img.alt = item.name || 'Résultat';
        img.setAttribute('onerror', "this.src='/static/img/travel.jpg'");

        const meta = document.createElement('div');
        const name = document.createElement('div'); name.className = 'result-name'; name.textContent = item.name || 'Sans nom';
        const type = document.createElement('div'); type.style.fontSize = '12px'; type.style.opacity = '.75'; type.textContent = item.locality || item.region || '';
        meta.appendChild(name); meta.appendChild(type);
        left.appendChild(img); left.appendChild(meta);

        const actionsDiv = document.createElement('div');
        actionsDiv.style.display = 'flex'; actionsDiv.style.gap = '8px'; actionsDiv.style.alignItems = 'center';

        const heartBtn = document.createElement('button');
        const isLiked = window.WishLikes ? window.WishLikes.has(id) : false;
        heartBtn.className = isLiked ? 'fav-action active' : 'fav-action';
        heartBtn.textContent = '❤';
        heartBtn.dataset.id = id;
        
        heartBtn.addEventListener('click', (e) => {
            e.stopPropagation();
            if(window.WishLikes) {
                window.WishLikes.toggle({ ...item, id, image: resolved });
                window.WishLikes.refresh();
                heartBtn.className = window.WishLikes.has(id) ? 'fav-action active' : 'fav-action';
            }
        });

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

        const chevron = document.createElement('div');
        chevron.innerHTML = '❯';
        chevron.style.marginLeft = '10px'; chevron.style.transition = 'transform 0.3s'; chevron.style.color = '#ccc';

        actionsDiv.appendChild(heartBtn);
        actionsDiv.appendChild(addBtn);
        actionsDiv.appendChild(chevron);

        header.appendChild(left);
        header.appendChild(actionsDiv);

        const details = document.createElement('div');
        details.className = 'result-details';
        const desc = item.description || "Aucune description détaillée n'est disponible pour cette activité. Laissez-vous surprendre sur place !";
        details.innerHTML = `<p style="margin:0;">${desc}</p>`;

        header.addEventListener('click', (e) => {
            if (e.target.closest('button')) return; 
            const isOpen = details.style.display === 'block';
            details.style.display = isOpen ? 'none' : 'block';
            chevron.style.transform = isOpen ? 'rotate(0deg)' : 'rotate(90deg)';
        });

        row.appendChild(header);
        row.appendChild(details);
        resultsBox.appendChild(row);
    });
    
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

        const isFirstFetch = (S.offset === 0);
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
        
        if (batch.length === 0 && isFirstFetch) {
            resultsBox.innerHTML = '<div class="no-res">Aucun résultat pour cette recherche.</div>';
            S.reachedEnd = true; 
        } else {
            renderResults(batch, !isFirstFetch);
            if (S.reachedEnd && batch.length > 0) showEnd(); 
        }
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

    searchInput.addEventListener('click', () => {
        if (searchInput.value.trim().length >= 2 && resultsBox.children.length > 0) {
            resultsBox.style.display = 'block';
        }
    });
}

/* -------------- Boot -------------- */
document.addEventListener('DOMContentLoaded', () => {
    const params = new URLSearchParams(location.search);
    const initialQ = params.get('q') || '';
    if (initialQ && searchInput) {
        searchInput.value = initialQ;
        startSearch(initialQ);
    }
    
    window.addEventListener('wishbasket:change', () => {
        document.querySelectorAll('.fav-action').forEach(btn => {
            const id = btn.dataset.id;
            if(id && window.WishLikes) {
                btn.className = window.WishLikes.has(id) ? 'fav-action active' : 'fav-action';
            }
        });
    });
});