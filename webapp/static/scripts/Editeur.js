// --- /static/scripts/Editeur.js ---

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

    document.getElementById('generateDaysBtn').addEventListener('click', () => {
        const num = parseInt(document.getElementById('numDaysInput').value);
        if (num > 0) generateDays(num);
    });

    generateDays(1);
});

function loadBasketIntoSidebar() {
    const list = document.getElementById('basket-items-list');
    list.innerHTML = '';
    
    let items = [];
    try { items = JSON.parse(localStorage.getItem(BASKET_KEY) || '[]'); } catch(e) {}

    if(items.length === 0) {
        list.innerHTML = '<p style="margin:auto; color:#888; font-size:13px; font-style:italic;">Votre panier est vide.</p>';
        return;
    }

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

            div.innerHTML = `
                <img src="${resolvedImg}" alt="${item.name || ''}" onerror="this.src='/static/img/travel.jpg'">
                <div class="p-name" style="font-family: 'Montserrat', sans-serif; font-size: 11px; font-weight: 600; text-transform: uppercase; text-align: center; margin-top: 8px; letter-spacing: 0.5px;">${item.name}</div>
                <button class="btn-remove-item" title="Remettre dans le panier">✕</button>
            `;
            
            div.addEventListener('dragstart', handleDragStart);
            div.addEventListener('dragend', handleDragEnd);
            div.querySelector('.btn-remove-item').addEventListener('click', function(e) {
                e.stopPropagation(); returnToBasket(div);
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
   MOTEUR 3D (FLUIDITÉ PARFAITE + COVER IMAGE FIXÉE)
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

    if (direction === 1) { // On avance
        frontFace.className = 'flipper-face flipper-front';
        backFace.className = 'flipper-face flipper-back';

        if (currentDayIndex === -1) { // 1. Ouvre la couv avant
            frontFace.innerHTML = renderCoverContent(coverTitle);
            frontFace.style.borderRadius = '0 15px 15px 0';
            
            backFace.classList.add('book-page', 'left-page');
            backFace.innerHTML = spreads[0].querySelector('.left-page').innerHTML;
            
            document.getElementById('bookCover').style.display = 'none';
            document.getElementById('bookSpreadsContainer').style.display = 'block';
            spreads.forEach(s => s.style.display = 'none');
            spreads[0].style.display = 'flex';
            spreads[0].querySelector('.left-page').style.visibility = 'hidden';
            
        } else if (currentDayIndex === maxIndex - 1) { // 2. Ferme sur couv arrière
            frontFace.classList.add('book-page', 'right-page');
            frontFace.innerHTML = spreads[currentDayIndex].querySelector('.right-page').innerHTML;

            backFace.innerHTML = renderCoverContent(''); 
            backFace.style.borderRadius = '15px 0 0 15px';
            
            spreads[currentDayIndex].querySelector('.right-page').style.visibility = 'hidden';
            
        } else { // 3. Page normale (Fluidité interne)
            frontFace.classList.add('book-page', 'right-page');
            frontFace.innerHTML = spreads[currentDayIndex].querySelector('.right-page').innerHTML;

            backFace.classList.add('book-page', 'left-page');
            backFace.innerHTML = spreads[newIndex].querySelector('.left-page').innerHTML;

            // SUPERPOSITION TEMPORELLE ANTI-TROU
            spreads[currentDayIndex].style.position = 'absolute';
            spreads[currentDayIndex].style.zIndex = 2;
            spreads[currentDayIndex].querySelector('.right-page').style.visibility = 'hidden';

            spreads[newIndex].style.display = 'flex';
            spreads[newIndex].style.zIndex = 1;
            spreads[newIndex].querySelector('.left-page').style.visibility = 'hidden';
        }
    } else { // On recule (-1)
        frontFace.className = 'flipper-face flipper-front';
        backFace.className = 'flipper-face flipper-back';

        if (currentDayIndex === 0) { // 4. On referme la couverture avant
            frontFace.classList.add('book-page', 'left-page');
            frontFace.innerHTML = spreads[0].querySelector('.left-page').innerHTML;

            backFace.innerHTML = renderCoverContent(coverTitle);
            backFace.style.borderRadius = '0 15px 15px 0';
            
            spreads[0].querySelector('.left-page').style.visibility = 'hidden';
            
        } else if (currentDayIndex === maxIndex) { // 5. On rouvre depuis le dos du livre
            frontFace.innerHTML = renderCoverContent('');
            frontFace.style.borderRadius = '15px 0 0 15px';

            backFace.classList.add('book-page', 'right-page');
            backFace.innerHTML = spreads[maxIndex - 1].querySelector('.right-page').innerHTML;

            document.getElementById('bookBackCover').style.display = 'none';
            document.getElementById('bookSpreadsContainer').style.display = 'block';
            spreads.forEach(s => s.style.display = 'none');
            spreads[maxIndex - 1].style.display = 'flex';
            spreads[maxIndex - 1].querySelector('.right-page').style.visibility = 'hidden';
            
        } else { // 6. Page normale en arrière (Fluidité interne)
            frontFace.classList.add('book-page', 'left-page');
            frontFace.innerHTML = spreads[currentDayIndex].querySelector('.left-page').innerHTML;

            backFace.classList.add('book-page', 'right-page');
            backFace.innerHTML = spreads[newIndex].querySelector('.right-page').innerHTML;

            // SUPERPOSITION TEMPORELLE ANTI-TROU
            spreads[currentDayIndex].style.position = 'absolute';
            spreads[currentDayIndex].style.zIndex = 2;
            spreads[currentDayIndex].querySelector('.left-page').style.visibility = 'hidden';

            spreads[newIndex].style.display = 'flex';
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
                    // Nettoyage de la superposition
                    spreads[currentDayIndex].style.display = 'none';
                    spreads[currentDayIndex].style.position = '';
                    spreads[currentDayIndex].style.zIndex = '';
                    spreads[currentDayIndex].querySelector('.right-page').style.visibility = 'visible';

                    spreads[newIndex].querySelector('.left-page').style.visibility = 'visible';
                    spreads[newIndex].style.zIndex = '';
                }
            } else {
                if (currentDayIndex === 0) {
                    spreads[0].querySelector('.left-page').style.visibility = 'visible';
                    document.getElementById('bookSpreadsContainer').style.display = 'none';
                    document.getElementById('bookCover').style.display = 'flex';
                } else if (currentDayIndex === maxIndex) {
                    spreads[maxIndex - 1].querySelector('.right-page').style.visibility = 'visible';
                } else {
                    // Nettoyage de la superposition
                    spreads[currentDayIndex].style.display = 'none';
                    spreads[currentDayIndex].style.position = '';
                    spreads[currentDayIndex].style.zIndex = '';
                    spreads[currentDayIndex].querySelector('.left-page').style.visibility = 'visible';

                    spreads[newIndex].querySelector('.right-page').style.visibility = 'visible';
                    spreads[newIndex].style.zIndex = '';
                }
            }
            
            currentDayIndex = newIndex;
            updateDayNumbers();
        }, 800);
    });
};

function generateDays(num) {
    const container = document.getElementById('bookSpreadsContainer');
    container.innerHTML = '';
    for(let i=0; i<num; i++) addDayHTML(container, i+1);
    initDropZones();
    
    document.getElementById('bookCover').style.display = 'flex';
    document.getElementById('bookBackCover').style.display = 'none';
    document.getElementById('bookSpreadsContainer').style.display = 'none';
    currentDayIndex = -1;
}

document.getElementById('addDayBtn').addEventListener('click', () => {
    const container = document.getElementById('bookSpreadsContainer');
    addDayHTML(container, container.children.length + 1);
    initDropZones();
    
    if (currentDayIndex !== -1 && currentDayIndex !== container.children.length) {
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
    document.querySelectorAll('.drop-zone').forEach(zone => {
        const newZone = zone.cloneNode(true);
        zone.parentNode.replaceChild(newZone, zone);
        
        newZone.addEventListener('dragover', e => { e.preventDefault(); newZone.classList.add('dragover'); });
        newZone.addEventListener('dragleave', e => { newZone.classList.remove('dragover'); });
        newZone.addEventListener('drop', function(e) {
            e.preventDefault();
            this.classList.remove('dragover');
            if (!draggedElement) return;

            const placeholder = this.querySelector('.drop-placeholder');
            if (placeholder) placeholder.remove();

            this.classList.add('filled');
            draggedElement.classList.add('dropped');
            this.appendChild(draggedElement); 
        });
    });

    document.querySelectorAll('.btn-delete-day').forEach(btn => {
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
                    return { id: item.dataset.id, name: item.dataset.name, image: item.dataset.image };
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
        if (!remainingItemsMap[id]) {
            remainingItemsMap[id] = { id: id, name: item.dataset.name, image: item.dataset.image, types: [item.dataset.type], qty: 1 };
        } else {
            remainingItemsMap[id].qty++;
        }
    });
    
    localStorage.setItem(BASKET_KEY, JSON.stringify(Object.values(remainingItemsMap)));

    btn.textContent = originalText;
    btn.disabled = false;
    window.location.href = '/topics';
});