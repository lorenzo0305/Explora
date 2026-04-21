const BASKET_KEY = 'wish_basket_v1';
const JOURNEYS_KEY = 'wish_journeys_v1';

let draggedElement = null; 
let currentDayIndex = 0; 
let isBookOpen = false;

document.addEventListener('DOMContentLoaded', () => {
    loadBasketIntoSidebar();
    
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

/* =========================================
   PANIER (BARRE LATÉRALE)
========================================= */
function loadBasketIntoSidebar() {
    const list = document.getElementById('basket-items-list');
    list.innerHTML = '';
    
    let items = [];
    try { items = JSON.parse(localStorage.getItem(BASKET_KEY) || '[]'); } catch(e){}

    if(items.length === 0) {
        list.innerHTML = '<p style="margin:auto; color:#888; font-size:13px; font-style:italic;">Votre panier est vide.</p>';
        return;
    }

    items.forEach(item => {
        const div = document.createElement('div');
        div.className = 'draggable-item';
        div.draggable = true; 
        div.dataset.id = item.id;
        div.dataset.name = item.name;
        div.dataset.image = item.image || '/static/img/no-image.jpg';
        div.dataset.type = (item.types && item.types[0]) ? item.types[0] : 'Activité';

        div.innerHTML = `
            <img src="${div.dataset.image}" alt="">
            <div class="p-name">${div.dataset.name}</div>
            <button class="btn-remove-item" title="Remettre dans le panier">✕</button>
        `;
        
        div.addEventListener('dragstart', handleDragStart);
        div.addEventListener('dragend', handleDragEnd);
        div.querySelector('.btn-remove-item').addEventListener('click', function(e) {
            e.stopPropagation(); returnToBasket(div);
        });

        list.appendChild(div);
    });
    
    const sideBasket = document.getElementById('sidebarBasket');
    sideBasket.addEventListener('dragover', e => { e.preventDefault(); sideBasket.style.backgroundColor = 'rgba(0,0,0,0.03)'; });
    sideBasket.addEventListener('dragleave', e => { sideBasket.style.backgroundColor = ''; });
    sideBasket.addEventListener('drop', function(e) {
        e.preventDefault(); sideBasket.style.backgroundColor = '';
        if (draggedElement && draggedElement.classList.contains('dropped')) returnToBasket(draggedElement);
    });
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

/* =========================================
   PAGINATION ET VRAIE ANIMATION 3D
========================================= */
window.openBook = function() {
    isBookOpen = true;
    document.getElementById('bookSpreadsContainer').style.display = 'block';
    showPage(0);
    
    document.getElementById('bookCover').classList.add('opened');
    
    setTimeout(() => {
        document.getElementById('bookCover').style.display = 'none';
    }, 800);
};

function closeBook() {
    isBookOpen = false;
    const cover = document.getElementById('bookCover');
    cover.style.display = 'flex';
    setTimeout(() => {
        cover.classList.remove('opened');
        setTimeout(() => {
            document.getElementById('bookSpreadsContainer').style.display = 'none';
        }, 800);
    }, 50);
}

function showPage(index) {
    const spreads = document.querySelectorAll('.day-spread');
    spreads.forEach((s, i) => {
        s.style.display = (i === index) ? 'flex' : 'none';
    });
    currentDayIndex = index;
}

window.turnPage = function(direction) {
    if(document.querySelector('.page-flipper')) return; // Empêche le spam de clic

    let spreads = document.querySelectorAll('.day-spread');
    let currentSpread = spreads[currentDayIndex];

    if (direction === 1) {
        // Tourne vers la droite (On avance)
        if (currentDayIndex >= spreads.length - 1) { 
            addDay(true); // Ajoute silencieusement
            spreads = document.querySelectorAll('.day-spread');
        }
        const nextSpread = spreads[currentDayIndex + 1];

        // Créer l'élément 3D
        const flipper = document.createElement('div');
        flipper.className = 'page-flipper flip-forward';
        flipper.innerHTML = `
            <div class="flipper-face flipper-front">${currentSpread.querySelector('.right-page').innerHTML}</div>
            <div class="flipper-face flipper-back">${nextSpread.querySelector('.left-page').innerHTML}</div>
        `;
        document.querySelector('.book-container').appendChild(flipper);

        currentSpread.style.display = 'none';
        nextSpread.style.display = 'flex';
        nextSpread.querySelector('.left-page').style.visibility = 'hidden';

        requestAnimationFrame(() => {
            flipper.classList.add('flipping');
            setTimeout(() => {
                flipper.remove();
                nextSpread.querySelector('.left-page').style.visibility = 'visible';
                currentDayIndex++;
                updateDayNumbers();
            }, 800); // Durée de l'animation CSS
        });
    } else {
        // Tourne vers la gauche (On recule)
        if (currentDayIndex === 0) { closeBook(); return; }
        const prevSpread = spreads[currentDayIndex - 1];

        const flipper = document.createElement('div');
        flipper.className = 'page-flipper flip-backward';
        flipper.innerHTML = `
            <div class="flipper-face flipper-front">${currentSpread.querySelector('.left-page').innerHTML}</div>
            <div class="flipper-face flipper-back">${prevSpread.querySelector('.right-page').innerHTML}</div>
        `;
        document.querySelector('.book-container').appendChild(flipper);

        currentSpread.style.display = 'none';
        prevSpread.style.display = 'flex';
        prevSpread.querySelector('.right-page').style.visibility = 'hidden';

        requestAnimationFrame(() => {
            flipper.classList.add('flipping');
            setTimeout(() => {
                flipper.remove();
                prevSpread.querySelector('.right-page').style.visibility = 'visible';
                currentDayIndex--;
                updateDayNumbers();
            }, 800);
        });
    }
};

/* =========================================
   GÉNÉRATION DES 4 BLOCS PAR JOUR (SYMETRIE)
========================================= */
function generateDays(num) {
    const container = document.getElementById('bookSpreadsContainer');
    container.innerHTML = ''; // Le crash de null venait d'ici, c'est réparé !
    for(let i=0; i<num; i++) addDayHTML(container, i+1);
    initDropZones();
    if(isBookOpen) showPage(0);
}

document.getElementById('addDayBtn').addEventListener('click', () => addDay());

function addDay(silent = false) {
    const container = document.getElementById('bookSpreadsContainer');
    const dayCount = container.children.length + 1;
    addDayHTML(container, dayCount);
    initDropZones();
    if(!silent && isBookOpen) {
        showPage(dayCount - 1);
    }
}

function addDayHTML(container, dayCount) {
    const dayHTML = `
        <div class="day-spread" data-day="${dayCount}">
            <div class="book-page left-page">
                <div class="page-chevron left" onclick="turnPage(-1)" title="Page précédente">❮</div>
                <div class="page-header">
                    <h2 class="day-title">Jour ${dayCount}</h2>
                    <span class="page-number">${(dayCount*2) - 1 < 10 ? '0'+((dayCount*2)-1) : (dayCount*2)-1}</span>
                </div>
                
                <div class="drop-zone-container">
                    <h4 class="time-title">Le Matin</h4>
                    <div class="drop-zone" data-time="matin"><span class="drop-placeholder">Glissez vos activités ici...</span></div>
                </div>
                <div class="drop-zone-container" style="margin-top:15px;">
                    <h4 class="time-title">Le Midi</h4>
                    <div class="drop-zone" data-time="midi"><span class="drop-placeholder">Glissez vos activités ici...</span></div>
                </div>
            </div>

            <div class="book-page right-page">
                <div class="page-header right-align">
                    <button class="btn-delete-day" title="Supprimer la page">Supprimer la page ✕</button>
                    <span class="page-number">${(dayCount*2) < 10 ? '0'+(dayCount*2) : (dayCount*2)}</span>
                </div>
                
                <div class="drop-zone-container">
                    <h4 class="time-title">L'Après-midi</h4>
                    <div class="drop-zone" data-time="aprem"><span class="drop-placeholder">Glissez vos activités ici...</span></div>
                </div>
                <div class="drop-zone-container" style="margin-top:15px;">
                    <h4 class="time-title">Le Soir</h4>
                    <div class="drop-zone" data-time="soir"><span class="drop-placeholder">Glissez vos activités ici...</span></div>
                </div>
                <div class="page-chevron right" onclick="turnPage(1)" title="Page suivante">❯</div>
            </div>
        </div>
    `;
    container.insertAdjacentHTML('beforeend', dayHTML);
}

/* =========================================
   DRAG & DROP
========================================= */
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
            const spread = this.closest('.day-spread');
            spread.querySelectorAll('.draggable-item').forEach(item => returnToBasket(item));
            spread.remove(); 
            
            const remainingSpreads = document.querySelectorAll('.day-spread');
            if(remainingSpreads.length === 0) { closeBook(); return; }
            
            if (currentDayIndex >= remainingSpreads.length) currentDayIndex = remainingSpreads.length - 1;
            updateDayNumbers(); 
            showPage(currentDayIndex);
        };
    });
}

function updateDayNumbers() {
    document.querySelectorAll('.day-spread').forEach((spread, index) => {
        const dayNum = index + 1;
        spread.querySelector('.day-title').textContent = `Jour ${dayNum}`;
        spread.querySelector('.left-page .page-number').textContent = (dayNum*2)-1 < 10 ? '0'+((dayNum*2)-1) : (dayNum*2)-1;
        spread.querySelector('.right-page .page-number').textContent = (dayNum*2) < 10 ? '0'+(dayNum*2) : (dayNum*2);
    });
}

/* =========================================
   SAUVEGARDER
========================================= */
document.getElementById('saveJourneyBtn').addEventListener('click', () => {
    const title = document.getElementById('tripTitle').value || 'Mon Voyage';
    const spreads = document.querySelectorAll('.day-spread');
    
    let plan = [];
    let firstImage = '/static/img/no-image.jpg';

    spreads.forEach((spread, index) => {
        let dayPlan = { day: index + 1, slots: [] };
        
        ['matin', 'midi', 'aprem', 'soir'].forEach(time => {
            const zone = spread.querySelector(`.drop-zone[data-time="${time}"]`);
            if(zone) {
                const itemsInZone = Array.from(zone.querySelectorAll('.draggable-item')).map(item => {
                    if (firstImage === '/static/img/no-image.jpg' && item.dataset.image) firstImage = item.dataset.image;
                    return { id: item.dataset.id, name: item.dataset.name, image: item.dataset.image };
                });
                if(itemsInZone.length > 0) dayPlan.slots.push({ key: time, items: itemsInZone });
            }
        });
        plan.push(dayPlan);
    });

    const newJourney = {
        id: 'j_' + Date.now().toString(36), name: title, location: 'Mon Carnet Magazine', cover: firstImage,
        createdAt: new Date().toISOString(), updatedAt: new Date().toISOString(), plan: plan
    };

    let allJourneys = [];
    try { allJourneys = JSON.parse(localStorage.getItem(JOURNEYS_KEY) || '[]'); } catch(e){}
    allJourneys.unshift(newJourney);
    localStorage.setItem(JOURNEYS_KEY, JSON.stringify(allJourneys));

    const list = document.getElementById('basket-items-list');
    const remainingItems = Array.from(list.querySelectorAll('.draggable-item')).map(item => ({ id: item.dataset.id, name: item.dataset.name, image: item.dataset.image, types: [item.dataset.type] }));
    localStorage.setItem(BASKET_KEY, JSON.stringify(remainingItems));

    window.location.href = '/topics';
});