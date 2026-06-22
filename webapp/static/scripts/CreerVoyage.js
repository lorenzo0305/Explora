// --- /static/scripts/CreerVoyage.js ---

const BASKET_KEY = 'wish_basket_v1';
const JOURNEYS_KEY = 'wish_journeys_v1';

let draggedElement = null; 
// -1 = Couverture Avant | 0 à N = Pages Intérieures | N+1 = Couverture Arrière
let currentDayIndex = -1; 

document.addEventListener('DOMContentLoaded', () => {
    const basketIconBtn = document.getElementById('basketIcon');
    if (basketIconBtn) {
        const wrapper = basketIconBtn.closest('.icon-wrapper');
        if (wrapper) wrapper.style.visibility = 'hidden'; 
    }
    
    loadBasketIntoSidebar();

    window.addEventListener('wishbasket:change', () => loadBasketIntoSidebar());
    window.addEventListener('storage', e => {
        if (!e.key || e.key === BASKET_KEY) loadBasketIntoSidebar();
    });

    document.getElementById('tripTitle').addEventListener('keypress', function(e) {
        if (e.key === 'Enter') this.blur();
    });

    document.getElementById('tripTitle').addEventListener('input', function() {
        document.getElementById('coverTitleDisplay').textContent = this.value || "Mon Voyage";
    });

    // CORRECTION : On utilise adjustDays au lieu de generateDays pour ne rien effacer
    document.getElementById('generateDaysBtn').addEventListener('click', () => {
        const num = parseInt(document.getElementById('numDaysInput').value);
        if (num > 0) adjustDays(num);
    });

    // Initialisation de base à 1 jour à l'ouverture de la page
    generateDays(1);
});

/* =========================================
   PANIER AVEC ACCORDÉON (STYLE VOYAGE)
========================================= */
function loadBasketIntoSidebar() {
    const list = document.getElementById('basket-items-list');
    list.innerHTML = '';
    
    let items = [];
    try { items = JSON.parse(localStorage.getItem(BASKET_KEY) || '[]'); } catch(e) {}

    if(items.length === 0) {
        list.innerHTML = '<p style="margin:auto; color:#888; font-size:13px; font-style:italic;">Votre panier est vide.</p>';
        return;
    }

    const formatCategories = (catsRaw) => {
        let arr = [];
        if (Array.isArray(catsRaw)) arr = catsRaw.map(c => typeof c === 'string' ? c : (c.name || ""));
        else if (typeof catsRaw === 'string') arr = catsRaw.split(',').map(s => s.trim());
        arr = arr.filter(Boolean);
        return arr.length ? arr.join(' · ') : '';
    };

    const escapeHtml = (s) => String(s ?? '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;').replace(/'/g, '&#39;');

    items.forEach(item => {
        let resolvedImg = typeof window !== 'undefined' && window.getActivityImage ? window.getActivityImage(item) : (item.image || '/static/img/travel.jpg');

        const qty = item.qty || 1;
        for (let i = 0; i < qty; i++) {
            const div = document.createElement('div');
            div.className = 'draggable-item';
            div.draggable = true; 
            div.dataset.id = item.id;
            div.dataset.name = item.name;
            div.dataset.image = resolvedImg; 
            
            div.dataset.full = JSON.stringify(item); 
            
            const rawCity = item.ville || item.locality || item.city || item.commune || item.adresse || '';
            const rawDesc = item.description || item.desc || item.summary || item.comment || item.abstract || "Aucune description détaillée n'est disponible.";
            
            const actCity = escapeHtml(rawCity);
            const cats = escapeHtml(formatCategories(item.categories || item.types || item.category));
            const actDesc = escapeHtml(rawDesc).trim();

            div.innerHTML = `
                <div class="btn-activite">
                    <img src="${resolvedImg}" class="inline-act-thumb" alt="${item.name || ''}" onerror="this.src='/static/img/travel.jpg'">
                    <div class="activite-info-header">
                        <span class="activite-nom">${item.name}</span>
                        <span class="activite-meta-header">${actCity}</span>
                    </div>
                    <span class="icone-fleche">▶</span>
                </div>
                <div class="details-activite">
                    <p class="categories-text">${cats}</p>
                    <p><strong>📍 Lieu :</strong> ${actCity}</p>
                    <p>${actDesc}</p>
                </div>
                <button class="btn-remove-item" title="Retirer du carnet">✕</button>
            `;
            
            div.querySelector('.btn-activite').addEventListener('click', function(e) {
                const detailsDiv = this.nextElementSibling;
                detailsDiv.classList.toggle('visible');
                this.classList.toggle('ouvert');
            });

            div.addEventListener('dragstart', handleDragStart);
            div.addEventListener('dragend', handleDragEnd);
            div.querySelector('.btn-remove-item').addEventListener('click', function(e) {
                e.stopPropagation(); 
                returnToBasket(div);
            });
            list.appendChild(div);
        }
    });
    
    const sideBasket = document.getElementById('sidebarBasket');
    if (sideBasket) {
        sideBasket.addEventListener('dragover', e => { e.preventDefault(); sideBasket.style.backgroundColor = 'rgba(0,0,0,0.03)'; });
        sideBasket.addEventListener('dragleave', e => { sideBasket.style.backgroundColor = ''; });
        sideBasket.addEventListener('drop', function(e) {
            e.preventDefault(); sideBasket.style.backgroundColor = '';
            if (draggedElement && draggedElement.classList.contains('dropped')) returnToBasket(draggedElement);
        });
    }
}

function returnToBasket(element) {
    const list = document.getElementById('basket-items-list');
    element.classList.remove('dropped');
    
    const btnAct = element.querySelector('.btn-activite');
    const details = element.querySelector('.details-activite');
    if(btnAct && details) {
        btnAct.classList.remove('ouvert');
        details.classList.remove('visible');
    }

    const parentZone = element.parentElement;
    list.appendChild(element);

    if(parentZone && parentZone.classList.contains('drop-zone')) {
        if(parentZone.querySelectorAll('.draggable-item').length === 0) {
            parentZone.classList.remove('filled');
            if(!parentZone.querySelector('.drop-placeholder')) {
                parentZone.innerHTML = '<span class="drop-placeholder">Glissez vos activités ici...</span>';
            }
        }
    }
}

function showToast(message) {
    let toast = document.getElementById('toast-notification');
    if (!toast) {
        toast = document.createElement('div');
        toast.id = 'toast-notification';
        toast.style.cssText = "position:fixed; bottom:30px; left:50%; transform:translateX(-50%); background:#1C1C1C; color:white; padding:12px 24px; border-radius:30px; z-index:10000; font-family:'Montserrat', sans-serif; font-size:12px; font-weight:700; text-transform:uppercase; letter-spacing:0.5px; box-shadow:0 4px 15px rgba(0,0,0,0.2); opacity:0; transition:opacity 0.3s;";
        document.body.appendChild(toast);
    }
    toast.textContent = message;
    toast.style.opacity = '1';
    setTimeout(() => { toast.style.opacity = '0'; }, 2500);
}

/* =========================================
   MOTEUR 3D ET GESTION DES PAGES
========================================= */
window.turnPage = function(direction) {
    if(document.querySelector('.page-flipper')) return;

    let spreads = document.querySelectorAll('.day-spread');
    let maxIndex = spreads.length;
    let newIndex = currentDayIndex + direction;

    if (newIndex < -1 || newIndex > maxIndex) return;

    const flipper = document.createElement('div');
    flipper.className = direction === 1 ? 'page-flipper flip-forward' : 'page-flipper flip-backward';

    const frontFace = document.createElement('div');
    const backFace = document.createElement('div');

    const coverTitle = document.getElementById('coverTitleDisplay').textContent;
    const coverEl = document.getElementById('bookCover');
    const exactBg = window.getComputedStyle(coverEl).backgroundImage.replace(/"/g, "'"); 
    
    const renderCoverContent = (title) => {
        return `<div class="book-cover-bg" style="display:flex; flex-direction:column; justify-content:center; align-items:center; text-align:center; width:100%; height:100%; padding:40px; background-image:${exactBg}; background-size:cover; background-position:center; background-color:#1C1C1C;">
            ${title ? `<h1 style="font-family:'Cormorant Garamond',serif; font-size:2.8rem; text-transform:uppercase; margin:0; padding:0 20px; color:white;">${title}</h1>` : ''}
        </div>`;
    };

    if (direction === 1) { 
        frontFace.className = 'flipper-face flipper-front';
        backFace.className = 'flipper-face flipper-back';

        if (currentDayIndex === -1) { 
            frontFace.innerHTML = renderCoverContent(coverTitle);
            frontFace.style.borderRadius = '0 15px 15px 0';
            backFace.classList.add('book-page', 'left-page');
            backFace.innerHTML = spreads[0].querySelector('.left-page').innerHTML;
            document.getElementById('bookCover').style.display = 'none';
            document.getElementById('bookSpreadsContainer').style.display = 'block';
            spreads.forEach(s => s.style.display = 'none');
            spreads[0].style.display = 'flex';
            spreads[0].querySelector('.left-page').style.visibility = 'hidden';
            
        } else if (currentDayIndex === maxIndex - 1) { 
            frontFace.classList.add('book-page', 'right-page');
            frontFace.innerHTML = spreads[currentDayIndex].querySelector('.right-page').innerHTML;
            backFace.innerHTML = renderCoverContent(''); 
            backFace.style.borderRadius = '15px 0 0 15px';
            spreads[currentDayIndex].querySelector('.right-page').style.visibility = 'hidden';
            
        } else { 
            frontFace.classList.add('book-page', 'right-page');
            frontFace.innerHTML = spreads[currentDayIndex].querySelector('.right-page').innerHTML;
            backFace.classList.add('book-page', 'left-page');
            backFace.innerHTML = spreads[newIndex].querySelector('.left-page').innerHTML;
            spreads[currentDayIndex].style.position = 'absolute';
            spreads[currentDayIndex].style.top = '0';
            spreads[currentDayIndex].style.left = '0';
            spreads[currentDayIndex].style.width = '100%';
            spreads[currentDayIndex].style.zIndex = 2;
            spreads[currentDayIndex].querySelector('.right-page').style.visibility = 'hidden';
            spreads[newIndex].style.display = 'flex';
            spreads[newIndex].style.position = 'absolute';
            spreads[newIndex].style.top = '0';
            spreads[newIndex].style.left = '0';
            spreads[newIndex].style.width = '100%';
            spreads[newIndex].style.zIndex = 1;
            spreads[newIndex].querySelector('.left-page').style.visibility = 'hidden';
        }
    } else { 
        frontFace.className = 'flipper-face flipper-front';
        backFace.className = 'flipper-face flipper-back';

        if (currentDayIndex === 0) { 
            frontFace.classList.add('book-page', 'left-page');
            frontFace.innerHTML = spreads[0].querySelector('.left-page').innerHTML;
            backFace.innerHTML = renderCoverContent(coverTitle);
            backFace.style.borderRadius = '0 15px 15px 0';
            spreads[0].querySelector('.left-page').style.visibility = 'hidden';
            
        } else if (currentDayIndex === maxIndex) { 
            frontFace.innerHTML = renderCoverContent('');
            frontFace.style.borderRadius = '15px 0 0 15px';
            backFace.classList.add('book-page', 'right-page');
            backFace.innerHTML = spreads[maxIndex - 1].querySelector('.right-page').innerHTML;
            document.getElementById('bookBackCover').style.display = 'none';
            document.getElementById('bookSpreadsContainer').style.display = 'block';
            spreads.forEach(s => s.style.display = 'none');
            spreads[maxIndex - 1].style.display = 'flex';
            spreads[maxIndex - 1].querySelector('.right-page').style.visibility = 'hidden';
            
        } else { 
            frontFace.classList.add('book-page', 'left-page');
            frontFace.innerHTML = spreads[currentDayIndex].querySelector('.left-page').innerHTML;
            backFace.classList.add('book-page', 'right-page');
            backFace.innerHTML = spreads[newIndex].querySelector('.right-page').innerHTML;
            spreads[currentDayIndex].style.position = 'absolute';
            spreads[currentDayIndex].style.top = '0';
            spreads[currentDayIndex].style.left = '0';
            spreads[currentDayIndex].style.width = '100%';
            spreads[currentDayIndex].style.zIndex = 2;
            spreads[currentDayIndex].querySelector('.left-page').style.visibility = 'hidden';
            spreads[newIndex].style.display = 'flex';
            spreads[newIndex].style.position = 'absolute';
            spreads[newIndex].style.top = '0';
            spreads[newIndex].style.left = '0';
            spreads[newIndex].style.width = '100%';
            spreads[newIndex].style.zIndex = 1;
            spreads[newIndex].querySelector('.right-page').style.visibility = 'hidden';
        }
    }

    flipper.appendChild(frontFace);
    flipper.appendChild(backFace);
    document.querySelector('.book-container').appendChild(flipper);

    requestAnimationFrame(() => {
        flipper.classList.add('flipping');
        setTimeout(() => {
            flipper.remove();
            
            if (direction === 1) {
                if (currentDayIndex === -1) {
                    spreads[0].querySelector('.left-page').style.visibility = 'visible';
                } else if (currentDayIndex === maxIndex - 1) {
                    spreads[currentDayIndex].querySelector('.right-page').style.visibility = 'visible';
                    document.getElementById('bookSpreadsContainer').style.display = 'none';
                    document.getElementById('bookBackCover').style.display = 'flex';
                } else {
                    spreads[currentDayIndex].style.display = 'none';
                    spreads[currentDayIndex].style.position = '';
                    spreads[currentDayIndex].style.zIndex = '';
                    spreads[currentDayIndex].style.width = '';
                    spreads[currentDayIndex].querySelector('.right-page').style.visibility = 'visible';
                    spreads[newIndex].querySelector('.left-page').style.visibility = 'visible';
                    spreads[newIndex].style.zIndex = '';
                    spreads[newIndex].style.width = '';
                }
            } else {
                if (currentDayIndex === 0) {
                    spreads[0].querySelector('.left-page').style.visibility = 'visible';
                    document.getElementById('bookSpreadsContainer').style.display = 'none';
                    document.getElementById('bookCover').style.display = 'flex';
                } else if (currentDayIndex === maxIndex) {
                    spreads[maxIndex - 1].querySelector('.right-page').style.visibility = 'visible';
                } else {
                    spreads[currentDayIndex].style.display = 'none';
                    spreads[currentDayIndex].style.position = '';
                    spreads[currentDayIndex].style.zIndex = '';
                    spreads[currentDayIndex].style.width = '';
                    spreads[currentDayIndex].querySelector('.left-page').style.visibility = 'visible';
                    spreads[newIndex].querySelector('.right-page').style.visibility = 'visible';
                    spreads[newIndex].style.zIndex = '';
                    spreads[newIndex].style.width = '';
                }
            }
            currentDayIndex = newIndex;
            updateDayNumbers();
        }, 800);
    });
};

function generateDays(num) {
    const container = document.getElementById('bookSpreadsContainer');
    container.innerHTML = ''; // Normal, c'est l'initialisation au chargement de la page
    for(let i=0; i<num; i++) addDayHTML(container, i+1);
    initDropZones();
    
    document.getElementById('bookCover').style.display = 'flex';
    document.getElementById('bookBackCover').style.display = 'none';
    document.getElementById('bookSpreadsContainer').style.display = 'none';
    currentDayIndex = -1;
}

// CORRECTION: AJOUT / SUPPRESSION SANS EFFACER LES DONNÉES EXISTANTES
function adjustDays(targetNum) {
    const container = document.getElementById('bookSpreadsContainer');
    const spreads = document.querySelectorAll('.day-spread');
    const currentCount = spreads.length;

    if (targetNum === currentCount) return;

    if (targetNum > currentCount) {
        // Ajouter les pages manquantes
        for (let i = currentCount; i < targetNum; i++) {
            addDayHTML(container, i + 1);
        }
        initDropZones();
    } else {
        // Retirer les pages en trop et renvoyer les activités au panier
        for (let i = currentCount - 1; i >= targetNum; i--) {
            const spread = spreads[i];
            spread.querySelectorAll('.draggable-item').forEach(item => returnToBasket(item));
            spread.remove();
        }
        
        // Si l'utilisateur était sur une page qui vient d'être supprimée, on le ramène sur la dernière
        const remainingSpreads = document.querySelectorAll('.day-spread');
        if (currentDayIndex >= remainingSpreads.length) {
            currentDayIndex = remainingSpreads.length - 1;
            if (document.getElementById('bookSpreadsContainer').style.display !== 'none') {
                remainingSpreads.forEach(s => s.style.display = 'none');
                if (currentDayIndex >= 0) {
                    remainingSpreads[currentDayIndex].style.display = 'flex';
                }
            }
        }
        updateDayNumbers();
    }
}

document.getElementById('addDayBtn').addEventListener('click', () => {
    const container = document.getElementById('bookSpreadsContainer');
    const newCount = container.children.length + 1;
    
    // Sync l'input
    const numInput = document.getElementById('numDaysInput');
    if (numInput) numInput.value = newCount;
    
    addDayHTML(container, newCount);
    initDropZones(); 
    
    if (currentDayIndex !== -1 && currentDayIndex !== container.children.length - 1) { 
        let spreads = document.querySelectorAll('.day-spread');
        spreads.forEach(s => s.style.display = 'none');
        currentDayIndex = spreads.length - 1;
        spreads[currentDayIndex].style.display = 'flex';
        document.getElementById('bookCover').style.display = 'none';
        document.getElementById('bookBackCover').style.display = 'none';
        document.getElementById('bookSpreadsContainer').style.display = 'block';
        updateDayNumbers();
    }
});

function addDayHTML(container, dayCount) {
    const dayHTML = `
        <div class="day-spread" data-day="${dayCount}">
            <div class="book-page left-page">
                <div class="page-chevron left" onclick="turnPage(-1)" title="Page précédente">❮</div>
                <div class="page-header">
                    <h2 class="day-title">Jour ${dayCount}</h2>
                    <span class="page-number">${(dayCount*2) - 1 < 10 ? '0'+((dayCount*2)-1) : (dayCount*2)-1}</span>
                </div>
                <div class="drop-zone-container full-height">
                    <h4 class="time-title">Le Matin</h4>
                    <div class="drop-zone" data-time="matin"><span class="drop-placeholder">Glissez vos activités ici...</span></div>
                </div>
            </div>
            <div class="book-page right-page">
                <div class="page-header right-align">
                    <button class="btn-delete-day" title="Supprimer la page">Supprimer la page ✕</button>
                    <span class="page-number">${(dayCount*2) < 10 ? '0'+(dayCount*2) : (dayCount*2)}</span>
                </div>
                <div class="drop-zone-container full-height">
                    <h4 class="time-title">L'Après-midi</h4>
                    <div class="drop-zone" data-time="aprem"><span class="drop-placeholder">Glissez vos activités ici...</span></div>
                </div>
                <div class="page-chevron right" onclick="turnPage(1)" title="Page suivante">❯</div>
            </div>
        </div>
    `;
    container.insertAdjacentHTML('beforeend', dayHTML);
}

function handleDragStart(e) { draggedElement = this; setTimeout(() => this.style.opacity = '0.4', 0); }
function handleDragEnd(e) { this.style.opacity = '1'; draggedElement = null; document.querySelectorAll('.drop-zone').forEach(z => z.classList.remove('dragover')); }

function initDropZones() {
    // CORRECTION : On initialise uniquement les zones qui n'ont pas encore été initialisées 
    // (pour ne pas écraser les activités existantes !)
    document.querySelectorAll('.drop-zone:not(.initialized)').forEach(zone => {
        zone.classList.add('initialized');
        
        zone.addEventListener('dragover', e => { e.preventDefault(); zone.classList.add('dragover'); });
        zone.addEventListener('dragleave', e => { zone.classList.remove('dragover'); });
        zone.addEventListener('drop', function(e) {
            e.preventDefault();
            this.classList.remove('dragover');
            if (!draggedElement) return;

            const placeholder = this.querySelector('.drop-placeholder');
            if (placeholder) placeholder.remove();

            this.classList.add('filled');
            draggedElement.classList.add('dropped');
            
            const btnAct = draggedElement.querySelector('.btn-activite');
            const details = draggedElement.querySelector('.details-activite');
            if(btnAct && details) {
                btnAct.classList.remove('ouvert');
                details.classList.remove('visible');
            }

            this.appendChild(draggedElement); 
        });
    });

    document.querySelectorAll('.btn-delete-day:not(.initialized)').forEach(btn => {
        btn.classList.add('initialized');
        btn.onclick = function() { 
            const spreads = document.querySelectorAll('.day-spread');
            if(spreads.length <= 1) return;

            const spread = this.closest('.day-spread');
            spread.querySelectorAll('.draggable-item').forEach(item => returnToBasket(item));
            spread.remove(); 
            
            const remainingSpreads = document.querySelectorAll('.day-spread');
            if (currentDayIndex >= remainingSpreads.length) currentDayIndex = remainingSpreads.length - 1;
            
            spreads.forEach(s => s.style.display = 'none');
            remainingSpreads[currentDayIndex].style.display = 'flex';
            
            updateDayNumbers(); 
            
            // Sync l'input
            const numInput = document.getElementById('numDaysInput');
            if (numInput) numInput.value = remainingSpreads.length;
        };
    });
    
    updateDayNumbers();
}

function updateDayNumbers() {
    const spreads = document.querySelectorAll('.day-spread');
    const isOnlyOne = spreads.length <= 1;

    spreads.forEach((spread, index) => {
        const dayNum = index + 1;
        spread.querySelector('.day-title').textContent = `Jour ${dayNum}`;
        spread.querySelector('.left-page .page-number').textContent = (dayNum*2)-1 < 10 ? '0'+((dayNum*2)-1) : (dayNum*2)-1;
        spread.querySelector('.right-page .page-number').textContent = (dayNum*2) < 10 ? '0'+(dayNum*2) : (dayNum*2);
        
        const delBtn = spread.querySelector('.btn-delete-day');
        if (delBtn) {
            if (isOnlyOne) {
                delBtn.style.opacity = '0.3';
                delBtn.style.cursor = 'not-allowed';
                delBtn.style.pointerEvents = 'none';
            } else {
                delBtn.style.opacity = '1';
                delBtn.style.cursor = 'pointer';
                delBtn.style.pointerEvents = 'auto';
            }
        }
    });
}

document.getElementById('saveJourneyBtn').addEventListener('click', async () => {    
    // --- SÉCURITÉ : VÉRIFICATION DU VOYAGE VIDE ---
    const spreadsForCheck = document.querySelectorAll('.day-spread');
    let hasAtLeastOneActivity = false;
    
    spreadsForCheck.forEach(spread => {
        if (spread.querySelectorAll('.draggable-item.dropped').length > 0) {
            hasAtLeastOneActivity = true;
        }
    });

    if (!hasAtLeastOneActivity) {
        showToast("Votre carnet est vide ! Glissez au moins une activité avant de sauvegarder.");
        return; 
    }
    // ----------------------------------------------

    const btn = document.getElementById('saveJourneyBtn');
    const originalText = btn.textContent;
    btn.disabled = true;
    btn.textContent = 'Sauvegarde...';

    const title = document.getElementById('tripTitle').value || 'Mon Voyage';
    const spreads = document.querySelectorAll('.day-spread');

    let plan = [];

    spreads.forEach((spread, index) => {
        let dayPlan = { day: index + 1, slots: [] };

        ['matin', 'aprem'].forEach(time => {
            const zone = spread.querySelector(`.drop-zone[data-time="${time}"]`);
            if(zone) {
                const itemsInZone = Array.from(zone.querySelectorAll('.draggable-item')).map(item => {
                    try {
                        return JSON.parse(item.dataset.full);
                    } catch(e) {
                        return { id: item.dataset.id, name: item.dataset.name, image: item.dataset.image };
                    }
                });
                if(itemsInZone.length > 0) dayPlan.slots.push({ key: time, items: itemsInZone });
            }
        });
        plan.push(dayPlan);
    });

    const nowIso = new Date().toISOString();
    const newJourney = {
        id: 'j_' + Date.now().toString(36),
        name: title,
        location: 'Mon Carnet Magazine',
        cover: '', 
        createdAt: nowIso,
        updatedAt: nowIso,
        plan: plan,
        source: 'editor'
    };

    try {
        const res = await fetch('/journeys', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(newJourney) });
        if (res.ok) {
            const result = await res.json();
            if (result && result.id) newJourney.id = result.id;
        }
    } catch (err) { }

    let allJourneys = [];
    try { allJourneys = JSON.parse(localStorage.getItem(JOURNEYS_KEY) || '[]'); } catch(e){}
    allJourneys.unshift(newJourney);
    localStorage.setItem(JOURNEYS_KEY, JSON.stringify(allJourneys));

    const list = document.getElementById('basket-items-list');
    const remainingItemsMap = {};
    Array.from(list.querySelectorAll('.draggable-item')).forEach(item => {
        const id = item.dataset.id;
        let fullData = { id: id, name: item.dataset.name, image: item.dataset.image, qty: 1 };
        try {
            fullData = { ...JSON.parse(item.dataset.full), qty: 1 };
        } catch(e) {}

        if (!remainingItemsMap[id]) {
            remainingItemsMap[id] = fullData;
        } else {
            remainingItemsMap[id].qty++;
        }
    });
    
    localStorage.setItem(BASKET_KEY, JSON.stringify(Object.values(remainingItemsMap)));

    btn.textContent = originalText;
    btn.disabled = false;
    window.location.href = '/mesvoyages'; 
});