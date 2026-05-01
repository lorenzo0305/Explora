/* =========================================================
 * wishbasket.js — Le Maître Absolu du Panier & Favoris
 * ========================================================= */
(function (global) {
    'use strict';

    const BASKET_KEY = 'wish_basket_v1';
    const LIKES_KEY  = 'wish_likes_v1';

    function load(key) {
        try { return JSON.parse(localStorage.getItem(key) || '[]') || []; }
        catch { return []; }
    }
    
    function save(key, arr) {
        localStorage.setItem(key, JSON.stringify(arr || []));
        try { window.dispatchEvent(new StorageEvent('storage', { key })); } catch (_) {}
        try { window.dispatchEvent(new CustomEvent('wishbasket:change', { detail: { key } })); } catch (_) {}
    }

    function add(key, item) {
        if (!item || item.id == null) return false;
        const list = load(key);
        const existing = list.find(x => String(x.id) === String(item.id));
        
        if (existing) {
            // Si c'est le panier, on incrémente la quantité
            if (key === BASKET_KEY) {
                existing.qty = (existing.qty || 1) + 1;
                save(key, list);
                return true;
            } else {
                // Pour les favoris, on ne peut pas liker 2 fois
                return false; 
            }
        }
        
        list.unshift({
            id: String(item.id),
            name: item.name || '',
            image: item.image || item.photo || '',
            types: item.types || (item.type ? [item.type] : []),
            qty: 1
        });
        save(key, list);
        return true;
    }

    function decreaseQty(key, id) {
        const list = load(key);
        const existing = list.find(x => String(x.id) === String(id));
        if (existing) {
            existing.qty = (existing.qty || 1) - 1;
            if (existing.qty <= 0) {
                remove(key, id);
                return;
            }
            save(key, list);
        }
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

    function $(id) { return document.getElementById(id); }
    function esc(s) { return String(s == null ? '' : s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/"/g, '&quot;'); }

    function updateBadge(badgeId, count) {
        const el = $(badgeId);
        if (!el) return;
        el.textContent = String(count);
        el.hidden = count === 0;
    }
    
    function updateCounts() {
        // Le badge du panier compte la SOMME des quantités
        const basketList = load(BASKET_KEY);
        const totalQty = basketList.reduce((acc, item) => acc + (item.qty || 1), 0);
        updateBadge('basketCount', totalQty);
        
        // Le badge des favoris compte juste les éléments uniques
        updateBadge('likesCount',  load(LIKES_KEY).length);
    }

    function renderPanel(container, key, title, emptyMsg, draggable) {
        if (!container) return;
        const items = load(key);
        const isBasket = (key === BASKET_KEY);
        
        const headerHtml = `
            <div style="display: flex; justify-content: space-between; align-items: center; border-bottom: 1px solid #EFE6DC; padding-bottom: 8px; margin-bottom: 10px;">
                <h4 class="wb-title" style="margin: 0; border: none; padding: 0; font-family: 'Cormorant Garamond', serif; font-size: 18px; font-weight: 700; color: #1C1C1C;">${esc(title)}</h4>
                ${isBasket ? `<a href="/editeur" title="Ouvrir l'éditeur" style="font-family:'Montserrat', sans-serif; font-size:10px; font-weight:900; color:#FF6F61; text-decoration:none; text-transform:uppercase; transition: opacity 0.2s;" onmouseover="this.style.opacity='0.7'" onmouseout="this.style.opacity='1'">Aller à l'éditeur ❯</a>` : ''}
            </div>
        `;

        if (!items.length) {
            container.innerHTML = headerHtml + `<p class="wb-empty" style="color: #6b5f57; font-style: italic; text-align: center; margin: 12px 0; font-size: 13px;">${esc(emptyMsg)}</p>`;
            return;
        }
        
        const rows = items.map(x => {
            const qtyControls = isBasket ? `
                <div class="wb-qty-group">
                    <button class="wb-btn-minus" type="button" data-id="${esc(x.id)}" data-key="${esc(key)}">-</button>
                    <span class="wb-qty-val">${x.qty || 1}</span>
                    <button class="wb-btn-plus" type="button" data-id="${esc(x.id)}" data-key="${esc(key)}">+</button>
                </div>
            ` : '';

            return '<div class="wb-item"' + (draggable ? ' draggable="true"' : '') +
            '   data-id="' + esc(x.id) + '"' +
            '   data-name="' + esc(x.name) + '"' +
            '   data-image="' + esc(x.image || '') + '">' +
            '   <img src="' + esc(x.image || '/static/img/no-image.jpg') + '" alt="" onerror="this.src=\'/static/img/no-image.jpg\'">' +
            '   <div class="wb-name">' + (esc(x.name) || 'Sans nom') + '</div>' +
                qtyControls +
            '   <button class="wb-remove" type="button" data-id="' + esc(x.id) + '" data-key="' + esc(key) + '" title="Retirer">✕</button>' +
            '</div>';
        }).join('');
        
        container.innerHTML = headerHtml + rows;

        if (draggable) {
            container.querySelectorAll('.wb-item').forEach(row => {
                row.addEventListener('dragstart', e => {
                    const data = { id: row.dataset.id, name: row.dataset.name, image: row.dataset.image };
                    try {
                        e.dataTransfer.setData('application/x-basket-item', JSON.stringify(data));
                        e.dataTransfer.setData('id', row.dataset.id); 
                        e.dataTransfer.effectAllowed = 'copyMove';
                    } catch (_) {}
                });
            });
        }

        // Événements pour le retrait total
        container.querySelectorAll('.wb-remove').forEach(btn => {
            btn.addEventListener('click', e => {
                e.stopPropagation();
                remove(btn.dataset.key, btn.dataset.id);
                refresh();
            });
        });

        // Événements pour gérer les quantités + et -
        if (isBasket) {
            container.querySelectorAll('.wb-btn-plus').forEach(btn => {
                btn.addEventListener('click', e => {
                    e.stopPropagation();
                    add(btn.dataset.key, { id: btn.dataset.id });
                    refresh();
                });
            });
            container.querySelectorAll('.wb-btn-minus').forEach(btn => {
                btn.addEventListener('click', e => {
                    e.stopPropagation();
                    decreaseQty(btn.dataset.key, btn.dataset.id);
                    refresh();
                });
            });
        }
    }

    function refresh() { updateCounts(); renderPanels(); }

    function renderPanels() {
        renderPanel($('floatingBasket'), BASKET_KEY, 'Votre panier',  'Votre panier est vide.', true);
        renderPanel($('floatingLikes'),  LIKES_KEY,  'Mes favoris',   'Aucun favori pour le moment.', false);
    }

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

        const newBasketIcon = basketIcon ? basketIcon.cloneNode(true) : null;
        if (newBasketIcon) basketIcon.parentNode.replaceChild(newBasketIcon, basketIcon);
        
        const newLikesIcon = likesIcon ? likesIcon.cloneNode(true) : null;
        if (newLikesIcon) likesIcon.parentNode.replaceChild(newLikesIcon, likesIcon);

        if (newBasketIcon && floatingBasket) {
            newBasketIcon.addEventListener('click', e => {
                e.stopPropagation();
                togglePanel(floatingBasket, floatingLikes);
            });
        }
        if (newLikesIcon && floatingLikes) {
            newLikesIcon.addEventListener('click', e => {
                e.stopPropagation();
                togglePanel(floatingLikes, floatingBasket);
            });
        }
        document.addEventListener('click', e => {
            if (floatingBasket && !floatingBasket.contains(e.target) && e.target !== newBasketIcon && !newBasketIcon?.contains(e.target)) {
                floatingBasket.style.display = 'none';
            }
            if (floatingLikes && !floatingLikes.contains(e.target) && e.target !== newLikesIcon && !newLikesIcon?.contains(e.target)) {
                floatingLikes.style.display = 'none';
            }
        });
    }

    function init() {
        wireToggles();
        refresh();
        window.addEventListener('storage', e => {
            if (!e.key || e.key === BASKET_KEY || e.key === LIKES_KEY) refresh();
        });
        window.addEventListener('wishbasket:change', refresh);
        window.addEventListener('pageshow', refresh);
    }

    global.WishBasket = {
        BASKET_KEY, LIKES_KEY,
        load: () => load(BASKET_KEY),
        add:    (item) => add(BASKET_KEY, item),
        decreaseQty,
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