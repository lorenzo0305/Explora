/* =========================================================
 * wishbasket.js — Panier & Favoris partagés
 * Branché sur localStorage (wish_basket_v1, wish_likes_v1)
 * Synchronisé entre onglets via l'événement "storage"
 * Chargement automatique sur DOMContentLoaded
 * ========================================================= */
(function (global) {
    'use strict';

    const BASKET_KEY = 'wish_basket_v1';
    const LIKES_KEY  = 'wish_likes_v1';

    // ---------- Stockage ----------
    function load(key) {
        try { return JSON.parse(localStorage.getItem(key) || '[]') || []; }
        catch { return []; }
    }
    function save(key, arr) {
        localStorage.setItem(key, JSON.stringify(arr || []));
        // pour les autres onglets
        try { window.dispatchEvent(new StorageEvent('storage', { key })); } catch (_) {}
        // pour la page actuelle
        try { window.dispatchEvent(new CustomEvent('wishbasket:change', { detail: { key } })); } catch (_) {}
    }

    function add(key, item) {
        if (!item || item.id == null) return false;
        const list = load(key);
        if (list.some(x => String(x.id) === String(item.id))) return false;
        list.unshift({
            id: String(item.id),
            name: item.name || '',
            image: item.image || item.photo || '',
            types: item.types || (item.type ? [item.type] : [])
        });
        save(key, list);
        return true;
    }
    function remove(key, id) {
        const list = load(key).filter(x => String(x.id) !== String(id));
        save(key, list);
    }
    function has(key, id) {
        return load(key).some(x => String(x.id) === String(id));
    }
    function toggle(key, item) {
        if (has(key, item.id)) { remove(key, item.id); return false; }
        return add(key, item);
    }

    // ---------- Helpers ----------
    function $(id) { return document.getElementById(id); }
    function esc(s) { return String(s == null ? '' : s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/"/g, '&quot;'); }

    // ---------- Rendu ----------
    function updateBadge(badgeId, count) {
        const el = $(badgeId);
        if (!el) return;
        el.textContent = String(count);
        el.hidden = count === 0;
    }
    function updateCounts() {
        updateBadge('basketCount', load(BASKET_KEY).length);
        updateBadge('likesCount',  load(LIKES_KEY).length);
    }

    function renderPanel(container, key, title, emptyMsg, draggable) {
        if (!container) return;
        const items = load(key);
        if (!items.length) {
            container.innerHTML =
                '<h4 class="wb-title">' + esc(title) + '</h4>' +
                '<p class="wb-empty">' + esc(emptyMsg) + '</p>';
            return;
        }
        const rows = items.map(x => (
            '<div class="wb-item"' + (draggable ? ' draggable="true"' : '') +
            '   data-id="' + esc(x.id) + '"' +
            '   data-name="' + esc(x.name) + '"' +
            '   data-image="' + esc(x.image || '') + '">' +
            '   <img src="' + esc(x.image || '/static/img/no-image.jpg') + '" alt="" onerror="this.src=\'/static/img/no-image.jpg\'">' +
            '   <div class="wb-name">' + (esc(x.name) || 'Sans nom') + '</div>' +
            '   <button class="wb-remove" type="button" data-id="' + esc(x.id) + '" data-key="' + esc(key) + '" title="Retirer">✕</button>' +
            '</div>'
        )).join('');
        container.innerHTML = '<h4 class="wb-title">' + esc(title) + '</h4>' + rows;

        if (draggable) {
            container.querySelectorAll('.wb-item').forEach(row => {
                row.addEventListener('dragstart', e => {
                    const data = { id: row.dataset.id, name: row.dataset.name, image: row.dataset.image };
                    try {
                        e.dataTransfer.setData('application/x-basket-item', JSON.stringify(data));
                        e.dataTransfer.effectAllowed = 'copy';
                    } catch (_) {}
                });
            });
        }

        container.querySelectorAll('.wb-remove').forEach(btn => {
            btn.addEventListener('click', e => {
                e.stopPropagation();
                remove(btn.dataset.key, btn.dataset.id);
            });
        });
    }

    function renderPanels() {
        renderPanel($('floatingBasket'), BASKET_KEY, 'Votre panier',  'Votre panier est vide.',          true);
        renderPanel($('floatingLikes'),  LIKES_KEY,  'Mes favoris',   'Aucun favori pour le moment.',    false);
    }

    // ---------- Wiring ----------
    function togglePanel(panel, otherPanel) {
        if (!panel) return;
        if (otherPanel) otherPanel.style.display = 'none';
        panel.style.display = (panel.style.display === 'block') ? 'none' : 'block';
    }
    function wireToggles() {
        const basketIcon = $('basketIcon');
        const likesIcon  = $('likesIcon');
        const floatingBasket = $('floatingBasket');
        const floatingLikes  = $('floatingLikes');

        if (basketIcon && floatingBasket) {
            basketIcon.addEventListener('click', e => {
                e.stopPropagation();
                togglePanel(floatingBasket, floatingLikes);
            });
        }
        if (likesIcon && floatingLikes) {
            likesIcon.addEventListener('click', e => {
                e.stopPropagation();
                togglePanel(floatingLikes, floatingBasket);
            });
        }
        document.addEventListener('click', e => {
            if (floatingBasket && !floatingBasket.contains(e.target) && e.target !== basketIcon && !basketIcon?.contains(e.target)) {
                floatingBasket.style.display = 'none';
            }
            if (floatingLikes && !floatingLikes.contains(e.target) && e.target !== likesIcon && !likesIcon?.contains(e.target)) {
                floatingLikes.style.display = 'none';
            }
        });
    }

    function refresh() { updateCounts(); renderPanels(); }

    function init() {
        wireToggles();
        refresh();

        // Synchronisation inter-onglets et intra-page
        window.addEventListener('storage', e => {
            if (!e.key || e.key === BASKET_KEY || e.key === LIKES_KEY) refresh();
        });
        window.addEventListener('wishbasket:change', refresh);

        // Re-render au retour sur la page (cache navigateur)
        window.addEventListener('pageshow', refresh);
    }

    // ---------- API publique ----------
    global.WishBasket = {
        BASKET_KEY, LIKES_KEY,
        load: () => load(BASKET_KEY),
        add:    (item) => add(BASKET_KEY, item),
        remove: (id)   => remove(BASKET_KEY, id),
        has:    (id)   => has(BASKET_KEY, id),
        toggle: (item) => toggle(BASKET_KEY, item),
        refresh
    };
    global.WishLikes = {
        load: () => load(LIKES_KEY),
        add:    (item) => add(LIKES_KEY, item),
        remove: (id)   => remove(LIKES_KEY, id),
        has:    (id)   => has(LIKES_KEY, id),
        toggle: (item) => toggle(LIKES_KEY, item),
        refresh
    };

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }
})(window);
