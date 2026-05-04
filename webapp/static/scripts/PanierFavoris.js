/* =========================================================
 * PanierFavoris.js — Le Maître Absolu du Panier & Favoris
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
        
        const finalImage = typeof window !== 'undefined' && window.getActivityImage ? window.getActivityImage(item) : (item.image || item.photo || '');

        if (existing) {
            if (key === BASKET_KEY) {
                existing.qty = (existing.qty || 1) + 1;
                Object.assign(existing, item); 
                existing.image = finalImage;
                
                save(key, list);
                return true;
            } else {
                return false; 
            }
        }
        
        list.unshift({
            ...item, 
            id: String(item.id),
            name: item.name || '',
            image: finalImage,
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
        const basketList = load(BASKET_KEY);
        const totalQty = basketList.reduce((acc, item) => acc + (item.qty || 1), 0);
        updateBadge('basketCount', totalQty);
        updateBadge('likesCount',  load(LIKES_KEY).length);
    }

    function renderPanel(container, key, title, emptyMsg, draggable) {
        if (!container) return;
        const items = load(key);
        const isBasket = (key === BASKET_KEY);
        
        const headerHtml = `
            <div style="display: flex; justify-content: space-between; align-items: center; border-bottom: 1px solid #EFE6DC; padding-bottom: 8px; margin-bottom: 10px;">
                <h4 class="wb-title" style="margin: 0; border: none; padding: 0; font-family: 'Cormorant Garamond', serif; font-size: 18px; font-weight: 700; color: #1C1C1C;">${esc(title)}</h4>
                ${(isBasket && items.length > 0) ? `<a href="/creervoyage" title="Créer mon voyage" style="font-family:'Montserrat', sans-serif; font-size:10px; font-weight:900; color:#FF6F61; text-decoration:none; text-transform:uppercase; transition: opacity 0.2s;" onmouseover="this.style.opacity='0.7'" onmouseout="this.style.opacity='1'">Aller à la création ❯</a>` : ''}
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

            // --- NOUVELLES ICÔNES PARFAITES ---
            let transferBtn = '';
            if (isBasket) {
                // Dans le Panier -> Flèche vers la gauche (vers les favoris)
                transferBtn = `
                <button class="wb-transfer" type="button" data-id="${esc(x.id)}" data-target="likes" title="Déplacer vers les favoris" style="background:none; border:none; cursor:pointer; color:#D4C3B3; margin-right:5px; padding:0; display:flex; align-items:center; transition:all 0.2s;" onmouseover="this.style.color='#FF6F61'" onmouseout="this.style.color='#D4C3B3'">
                    <svg viewBox="0 0 24 24" width="18" height="18" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                        <defs>
                            <mask id="cut-left-${esc(x.id)}">
                                <rect width="24" height="24" fill="white" />
                                <circle cx="4" cy="11" r="8" fill="black" />
                            </mask>
                        </defs>
                        <path mask="url(#cut-left-${esc(x.id)})" d="M20.84 4.61a5.5 5.5 0 0 0-7.78 0L12 5.67l-1.06-1.06a5.5 5.5 0 0 0-7.78 7.78l1.06 1.06L12 21.23l7.78-7.78 1.06-1.06a5.5 5.5 0 0 0 0-7.78z"/>
                        <line x1="14" y1="11" x2="1" y2="11" />
                        <polyline points="5 7 1 11 5 15" />
                    </svg>
                </button>`;
            } else {
                // Dans les Favoris -> Flèche vers la droite (vers le panier)
                transferBtn = `
                <button class="wb-transfer" type="button" data-id="${esc(x.id)}" data-target="basket" title="Déplacer vers le panier" style="background:none; border:none; cursor:pointer; color:#D4C3B3; margin-right:5px; padding:0; display:flex; align-items:center; transition:all 0.2s;" onmouseover="this.style.color='#FF6F61'" onmouseout="this.style.color='#D4C3B3'">
                    <svg viewBox="0 0 24 24" width="18" height="18" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                        <defs>
                            <mask id="cut-right-${esc(x.id)}">
                                <rect width="24" height="24" fill="white" />
                                <circle cx="20" cy="11" r="8" fill="black" />
                            </mask>
                        </defs>
                        <path mask="url(#cut-right-${esc(x.id)})" d="M20.84 4.61a5.5 5.5 0 0 0-7.78 0L12 5.67l-1.06-1.06a5.5 5.5 0 0 0-7.78 7.78l1.06 1.06L12 21.23l7.78-7.78 1.06-1.06a5.5 5.5 0 0 0 0-7.78z"/>
                        <line x1="10" y1="11" x2="23" y2="11" />
                        <polyline points="19 7 23 11 19 15" />
                    </svg>
                </button>`;
            }
            // ----------------------------------------------

            const finalImg = typeof window !== 'undefined' && window.getActivityImage ? window.getActivityImage(x) : (x.image || '/static/img/travel.jpg');

            return '<div class="wb-item"' + (draggable ? ' draggable="true"' : '') +
            '   data-id="' + esc(x.id) + '"' +
            '   data-name="' + esc(x.name) + '"' +
            '   data-image="' + esc(finalImg) + '">' +
            '   <img src="' + esc(finalImg) + '" alt="" onerror="this.src=\'/static/img/travel.jpg\'">' +
            '   <div class="wb-name">' + (esc(x.name) || 'Sans nom') + '</div>' +
                qtyControls +
                transferBtn +
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

        container.querySelectorAll('.wb-remove').forEach(btn => {
            btn.addEventListener('click', e => {
                e.stopPropagation();
                remove(btn.dataset.key, btn.dataset.id);
                refresh();
            });
        });

        // Logique de DÉPLACEMENT
        container.querySelectorAll('.wb-transfer').forEach(btn => {
            btn.addEventListener('click', e => {
                e.stopPropagation();
                const id = btn.dataset.id;
                const target = btn.dataset.target; 
                
                const sourceKey = target === 'likes' ? BASKET_KEY : LIKES_KEY;
                const destKey   = target === 'likes' ? LIKES_KEY : BASKET_KEY;
                
                const itemToTransfer = load(sourceKey).find(i => String(i.id) === String(id));
                if (itemToTransfer) {
                    add(destKey, itemToTransfer);
                    remove(sourceKey, itemToTransfer.id);
                    refresh();
                }
            });
        });

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