/* =========================================================
 * Exploration.js — Page Catalogue (recherche + catégories)
 * Branché sur l'API Mongo via /search et /regions/:slug/cards
 * ========================================================= */
(function () {
    'use strict';

    const NO_IMG = '/static/img/no-image.jpg';
    const BASKET_KEY = 'wish_basket_v1';
    const LIKES_KEY  = 'wish_likes_v1';

    const $ = (id) => document.getElementById(id);
    const esc = (s) => String(s == null ? '' : s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/"/g, '&quot;');
    const debounce = (fn, ms = 280) => { let t; return (...a) => { clearTimeout(t); t = setTimeout(() => fn(...a), ms); }; };

    function loadKey(k) { try { return JSON.parse(localStorage.getItem(k) || '[]') || []; } catch { return []; } }
    function saveKey(k, v) { localStorage.setItem(k, JSON.stringify(v || [])); }

    function addBasket(item) {
        const list = loadKey(BASKET_KEY);
        if (list.some(x => String(x.id) === String(item.id))) return false;
        list.unshift({ id: String(item.id), name: item.name || '', image: item.image || '', types: item.types || [] });
        saveKey(BASKET_KEY, list);
        notify('Ajouté au panier ✓');
        updateBadge('basketCount', list.length);
        return true;
    }
    function toggleLike(item) {
        let list = loadKey(LIKES_KEY);
        const exists = list.some(x => String(x.id) === String(item.id));
        if (exists) list = list.filter(x => String(x.id) !== String(item.id));
        else list.unshift({ id: String(item.id), name: item.name || '', image: item.image || '', types: item.types || [] });
        saveKey(LIKES_KEY, list);
        updateBadge('likesCount', list.length);
        return !exists;
    }
    function isLiked(id) {
        return loadKey(LIKES_KEY).some(x => String(x.id) === String(id));
    }
    function updateBadge(badgeId, count) {
        const el = $(badgeId);
        if (!el) return;
        el.textContent = String(count);
        el.hidden = count === 0;
    }

    function notify(msg) {
        let bar = $('catNotify');
        if (!bar) {
            bar = document.createElement('div');
            bar.id = 'catNotify';
            bar.style.cssText = 'position:fixed;bottom:24px;left:50%;transform:translateX(-50%);background:#1C1C1C;color:#fff;padding:10px 18px;border-radius:8px;font-family:Lora,serif;font-size:14px;z-index:9999;box-shadow:0 6px 20px rgba(0,0,0,.2);opacity:0;transition:opacity .25s;';
            document.body.appendChild(bar);
        }
        bar.textContent = msg;
        requestAnimationFrame(() => { bar.style.opacity = '1'; });
        clearTimeout(bar._t);
        bar._t = setTimeout(() => { bar.style.opacity = '0'; }, 1600);
    }

    /* ---------- Floating panels (panier + likes) ---------- */
    function renderPanel(container, key, title, emptyMsg) {
        if (!container) return;
        const items = loadKey(key);
        if (!items.length) {
            container.innerHTML = '<h4 class="wb-title">' + esc(title) + '</h4><p class="wb-empty">' + esc(emptyMsg) + '</p>';
            return;
        }
        container.innerHTML = '<h4 class="wb-title">' + esc(title) + '</h4>' + items.map(x => (
            '<div class="wb-item" data-id="' + esc(x.id) + '">' +
            '  <img src="' + esc(x.image || NO_IMG) + '" alt="" onerror="this.src=\'' + NO_IMG + '\'">' +
            '  <div class="wb-name">' + esc(x.name || 'Sans nom') + '</div>' +
            '  <button class="wb-remove" type="button" data-id="' + esc(x.id) + '" data-key="' + esc(key) + '" title="Retirer">✕</button>' +
            '</div>'
        )).join('');
        container.querySelectorAll('.wb-remove').forEach(btn => {
            btn.addEventListener('click', e => {
                e.stopPropagation();
                const list = loadKey(btn.dataset.key).filter(x => String(x.id) !== String(btn.dataset.id));
                saveKey(btn.dataset.key, list);
                refreshPanels();
                if (btn.dataset.key === BASKET_KEY) updateBadge('basketCount', list.length);
                if (btn.dataset.key === LIKES_KEY)  updateBadge('likesCount',  list.length);
            });
        });
    }
    function refreshPanels() {
        renderPanel($('floatingBasket'), BASKET_KEY, 'Votre panier', 'Votre panier est vide.');
        renderPanel($('floatingLikes'),  LIKES_KEY,  'Mes favoris',  'Aucun favori pour le moment.');
    }
    function wirePanels() {
        const basketIcon = $('basketIcon');
        const likesIcon  = $('likesIcon');
        const fb = $('floatingBasket');
        const fl = $('floatingLikes');
        if (basketIcon && fb) {
            basketIcon.addEventListener('click', e => { e.stopPropagation(); if (fl) fl.style.display = 'none'; fb.style.display = (fb.style.display === 'block') ? 'none' : 'block'; });
        }
        if (likesIcon && fl) {
            likesIcon.addEventListener('click', e => { e.stopPropagation(); if (fb) fb.style.display = 'none'; fl.style.display = (fl.style.display === 'block') ? 'none' : 'block'; });
        }
        document.addEventListener('click', e => {
            if (fb && !fb.contains(e.target) && !basketIcon?.contains(e.target)) fb.style.display = 'none';
            if (fl && !fl.contains(e.target) && !likesIcon?.contains(e.target)) fl.style.display = 'none';
        });
    }

    /* ---------- Recherche ---------- */
    const searchInput = $('search');
    const resultsBox  = $('results');
    let searchSeq = 0;

    function pickImage(item) {
        if (item.image && /^https?:/.test(item.image)) return item.image;
        if (window.WishMedia && typeof window.WishMedia.getBestImage === 'function') {
            return window.WishMedia.getBestImage(item) || NO_IMG;
        }
        return NO_IMG;
    }

    function renderResults(items) {
        if (!resultsBox) return;
        resultsBox.innerHTML = '';
        if (!items || !items.length) {
            resultsBox.innerHTML = '<div class="no-res" style="padding:14px;color:#6b5f57;font-style:italic;">Aucun résultat dans la base de données.</div>';
            resultsBox.style.display = 'block';
            return;
        }
        items.forEach(item => {
            const id = item.id || item._id || '';
            if (!id) return;
            const img = pickImage(item);
            const name = item.name || 'Sans nom';
            const sub  = item.locality || item.region || (item.types && item.types[0]) || '';

            const row = document.createElement('div');
            row.className = 'result-item';
            row.style.cssText = 'display:flex;align-items:center;gap:12px;padding:10px 12px;border-bottom:1px solid #EFE6DC;cursor:pointer;background:#fff;';
            row.innerHTML =
                '<img class="result-thumb" src="' + esc(img) + '" alt="" onerror="this.src=\'' + NO_IMG + '\'" style="width:54px;height:54px;border-radius:6px;object-fit:cover;flex-shrink:0;">' +
                '<div class="result-meta" style="flex:1;min-width:0;">' +
                '  <div class="result-name" style="font-family:Cormorant Garamond,serif;font-size:16px;font-weight:600;color:#1C1C1C;line-height:1.2;">' + esc(name) + '</div>' +
                '  <div class="result-sub" style="font-size:12px;color:#6b5f57;">' + esc(sub) + '</div>' +
                '</div>' +
                '<div class="result-actions" style="display:flex;gap:6px;align-items:center;flex-shrink:0;">' +
                '  <button class="result-fav ' + (isLiked(id) ? 'active' : '') + '" type="button" title="Favoris" style="background:transparent;border:1px solid #D4C3B3;padding:6px 10px;border-radius:6px;cursor:pointer;color:' + (isLiked(id) ? '#FF6F61' : '#6b5f57') + ';">❤</button>' +
                '  <button class="result-add" type="button" title="Ajouter au panier" style="background:#FF6F61;color:#fff;border:none;padding:6px 12px;border-radius:6px;cursor:pointer;font-weight:600;">+ Panier</button>' +
                '</div>';

            row.addEventListener('click', e => {
                if (e.target.closest('.result-fav') || e.target.closest('.result-add')) return;
                location.href = '/object/' + encodeURIComponent(id);
            });
            row.querySelector('.result-fav').addEventListener('click', e => {
                e.stopPropagation();
                const liked = toggleLike({ id, name, image: img, types: item.types || [] });
                e.currentTarget.classList.toggle('active', liked);
                e.currentTarget.style.color = liked ? '#FF6F61' : '#6b5f57';
                refreshPanels();
            });
            row.querySelector('.result-add').addEventListener('click', e => {
                e.stopPropagation();
                addBasket({ id, name, image: img, types: item.types || [] });
                refreshPanels();
            });
            resultsBox.appendChild(row);
        });
        resultsBox.style.display = 'block';
        resultsBox.style.maxHeight = '60vh';
        resultsBox.style.overflow = 'auto';
        resultsBox.style.borderRadius = '12px';
        resultsBox.style.boxShadow = '0 10px 30px rgba(0,0,0,0.08)';
        resultsBox.style.background = '#fff';
        resultsBox.style.marginTop = '10px';
    }

    async function doSearch(query) {
        if (!resultsBox) return;
        const q = (query || '').trim();
        if (q.length < 2) { resultsBox.style.display = 'none'; resultsBox.innerHTML = ''; return; }
        const mySeq = ++searchSeq;
        resultsBox.style.display = 'block';
        resultsBox.innerHTML = '<div style="padding:14px;color:#6b5f57;font-style:italic;">Recherche…</div>';
        try {
            const r = await fetch('/search?query=' + encodeURIComponent(q) + '&limit=30');
            if (mySeq !== searchSeq) return;
            const data = r.ok ? await r.json() : [];
            renderResults(Array.isArray(data) ? data : []);
        } catch (err) {
            if (mySeq !== searchSeq) return;
            resultsBox.innerHTML = '<div style="padding:14px;color:#8B1D2B;">Erreur de connexion au serveur.</div>';
        }
    }

    /* ---------- Catégories rapides ---------- */
    function wireCategoryCards() {
        // Mappe le titre du h4 vers une requête de recherche dans Mongo
        const cardCategoryMap = {
            'Nature & Grand Air': 'nature',
            'Gastronomie':        'gastronomie',
            'Patrimoine Culturel': 'culture',
            'Sport & Aventure':   'sport',
            'Détente & Bien-être': 'détente',
            'Shopping & Boutiques': 'shopping'
        };
        document.querySelectorAll('.category-card').forEach(card => {
            const h4 = card.querySelector('h4');
            if (!h4) return;
            const term = cardCategoryMap[h4.textContent.trim()];
            if (!term) return;
            // Désactive le href="#"
            card.addEventListener('click', e => {
                e.preventDefault();
                if (searchInput) {
                    searchInput.value = term;
                    searchInput.scrollIntoView({ behavior: 'smooth', block: 'start' });
                    searchInput.focus();
                    doSearch(term);
                }
            });
        });
    }

    /* ---------- Boot ---------- */
    document.addEventListener('DOMContentLoaded', () => {
        wirePanels();
        refreshPanels();
        updateBadge('basketCount', loadKey(BASKET_KEY).length);
        updateBadge('likesCount',  loadKey(LIKES_KEY).length);

        if (searchInput) {
            searchInput.addEventListener('input', debounce(e => doSearch(e.target.value), 280));
            // Prend une éventuelle ?q= dans l'URL pour pré-remplir
            const initialQ = new URLSearchParams(location.search).get('q') || '';
            if (initialQ) { searchInput.value = initialQ; doSearch(initialQ); }
        }
        wireCategoryCards();

        // Synchro inter-onglet
        window.addEventListener('storage', e => {
            if (!e.key || e.key === BASKET_KEY || e.key === LIKES_KEY) {
                updateBadge('basketCount', loadKey(BASKET_KEY).length);
                updateBadge('likesCount',  loadKey(LIKES_KEY).length);
                refreshPanels();
            }
        });
    });
})();
