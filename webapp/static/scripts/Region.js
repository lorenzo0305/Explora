const escapeHtmlStr = s => String(s ?? '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;').replace(/'/g, '&#39;');

function showToast(message) {
    const toast = document.createElement('div');
    toast.textContent = message;
    toast.style.cssText = "position:fixed; bottom:30px; left:50%; transform:translateX(-50%); background:#1C1C1C; color:white; padding:12px 24px; border-radius:30px; z-index:10000; font-family:'Montserrat', sans-serif; font-size:12px; font-weight:700; text-transform:uppercase; letter-spacing:0.5px; box-shadow:0 4px 15px rgba(0,0,0,0.2); opacity:0; transition:opacity 0.3s;";
    document.body.appendChild(toast);
    setTimeout(() => toast.style.opacity = '1', 10);
    setTimeout(() => { toast.style.opacity = '0'; setTimeout(() => toast.remove(), 300); }, 2500);
}

// Ici on s'assure que "fond" renvoie systématiquement vers simple.jpg comme tu le souhaitais
const THEMES_PHONES = {
    "Nature": {
        fond: "/static/img/simple.jpg", 
        icones: ['/static/img/nature1.jpg','/static/img/nature2.jpg','/static/img/nature3.jpg','/static/img/nature4.jpg','/static/img/nature5.jpg','/static/img/nature6.jpg']
    },
    "Gastronomie": {
        fond: "/static/img/simple.jpg", 
        icones: ['/static/img/food1.jpg','/static/img/food2.jpg','/static/img/food3.jpg','/static/img/food4.jpg','/static/img/food5.jpg','/static/img/food6.jpg']
    },
    "Culture": {
        fond: "/static/img/simple.jpg", 
        icones: ['/static/img/culture1.jpg','/static/img/culture2.jpg','/static/img/culture3.jpg','/static/img/culture4.jpg','/static/img/culture5.jpg','/static/img/culture6.jpg']
    },
    "Sport": {
        fond: "/static/img/simple.jpg", 
        icones: ['/static/img/sport1.jpg','/static/img/sport2.jpg','/static/img/sport3.jpg','/static/img/sport4.jpg','/static/img/sport5.png','/static/img/sport6.jpg']
    },   
    "Détente": {
        fond: "/static/img/simple.jpg", 
        icones: ['/static/img/detente1.jpg','/static/img/detente2.jpg','/static/img/detente3.jpg','/static/img/detente4.jpg','/static/img/detente55.jpg','/static/img/detente6.jpg']
    },
    "Shopping": {
        fond: "/static/img/simple.jpg", 
        icones: ['/static/img/shopping1.jpg','/static/img/shopping2.jpg','/static/img/shopping3.jpg','/static/img/shopping4.jpg','/static/img/shopping5.jpg','/static/img/shopping6.jpg']
    },
};

const MIX_SEQ = [0, 1, 2, 3, 4, 5, 2, 4, 0, 5, 1, 3, 4, 2, 5, 0, 3, 1, 5, 3, 1, 4, 2];
let currentCategoryName = ''; 

window.openPhone = async function(categoryName) {
    currentCategoryName = categoryName; 
    const modal = document.getElementById('giantPhoneModal');
    const grid = document.getElementById('appGrid');
    const preview = document.getElementById('activityPreview');
    const phoneScreen = document.querySelector('.giant-screen-landscape');
    
    const regionNameEl = document.getElementById('regionName');
    let regionTitre = regionNameEl ? regionNameEl.textContent.toUpperCase() : ''; 
    let regionSource = regionTitre.includes("AUVERGNE") ? "Auvergne" : (regionTitre.includes("HAUT") ? "Haut_de_France" : regionTitre);

    const currentTheme = THEMES_PHONES[categoryName] || THEMES_PHONES["Nature"];
    phoneScreen.style.backgroundImage = `url('${currentTheme.fond}')`;

    // On retire la potentielle classe de fermeture de la dernière fois
    modal.classList.remove('fade-out');

    grid.classList.remove('hidden');
    preview.classList.remove('show');
    preview.innerHTML = ''; 

    // LE TITRE SE FAIT UNE SEULE FOIS ICI (Pas de "Catégorie")
    grid.innerHTML = `
        <h2 class="app-title">${categoryName}</h2>
        <div class="app-grid-icons"><p style="color:#111; font-weight:bold;">Chargement...</p></div>
    `;
    
    // On l'affiche avec display flex pour qu'il soit cliquable
    modal.style.display = 'flex';

    try {
        const response = await fetch(`/api/activites/${regionSource}/${categoryName}`);
        const data = await response.json();
        
        const gridIconsContainer = grid.querySelector('.app-grid-icons');
        gridIconsContainer.innerHTML = ''; 

        if (!data || data.length === 0) {
            gridIconsContainer.innerHTML = `<p style="color:#111;">Aucune activité trouvée.</p>`;
            return;
        }

        data.forEach((act, index) => {
            let imgUrl = "";

            if (act.image && !act.image.includes('no-image') && !act.image.includes('appareil_photo')) {
                imgUrl = act.image;
            } else {
                const variantIndex = MIX_SEQ[index % MIX_SEQ.length] % currentTheme.icones.length;
                imgUrl = currentTheme.icones[variantIndex];
            }

            const icon = document.createElement('div');
            icon.className = 'app-icon';
            icon.innerHTML = `
                <div class="app-icon-img" style="background-image: url('${imgUrl}');"></div>
                <div class="app-icon-text">${escapeHtmlStr(act.name)}</div>
            `;

            icon.addEventListener('click', () => {
                showActivityDetails(act, imgUrl);
            });

            gridIconsContainer.appendChild(icon);
        });
    } catch (err) {
        const gridIconsContainer = grid.querySelector('.app-grid-icons');
        if(gridIconsContainer) gridIconsContainer.innerHTML = '<p style="color:red;">Erreur de connexion</p>';
    }
};

function showActivityDetails(act, resolvedImgUrl) {
    const grid = document.getElementById('appGrid');
    const preview = document.getElementById('activityPreview');

    const activityName = act.name || 'Sans nom';

    // On cache doucement la grille des apps
    grid.classList.add('hidden');

    const types = Array.isArray(act.types) ? act.types.join(' · ') : act.category || '';
    const desc = act.description || "Aucune description détaillée n'est disponible pour cette activité. Laissez-vous surprendre sur place !";
    const id = act.id || act._id || act.url || '';
    const isFav = window.WishLikes ? window.WishLikes.has(id) : false;

    // Plein écran dans le téléphone !
    preview.innerHTML = `
        <div class="preview-header">
            <button class="back-to-apps-btn" onclick="window.backToAppGrid()">←</button>
            <div class="header-name">${escapeHtmlStr(activityName)}</div>
        </div>
        
        <div class="preview-body">
            <div class="preview-visual" style="background-image: url('${resolvedImgUrl}');"></div>
            
            <div class="preview-info-pane">
                <div class="pi-meta">${escapeHtmlStr(types)}</div>
                <h3 class="pi-title">${escapeHtmlStr(activityName)}</h3>
                <p class="pi-desc">${escapeHtmlStr(desc)}</p>
                
                <div class="pi-actions">
                    <button class="btn-fav ${isFav ? 'active' : ''}">❤</button>
                    <button class="btn-pan">+ Panier</button>
                </div>
            </div>
        </div>
    `;

    const btnFav = preview.querySelector('.btn-fav');
    btnFav.addEventListener('click', (e) => {
        e.stopPropagation();
        if(window.WishLikes) {
            window.WishLikes.toggle({ id, name: activityName, image: resolvedImgUrl, types: act.types || [] });
            btnFav.classList.toggle('active', window.WishLikes.has(id));
            window.WishLikes.refresh();
        }
    });

    const btnPan = preview.querySelector('.btn-pan');
    btnPan.addEventListener('click', (e) => {
        e.stopPropagation();
        if(window.WishBasket) {
            window.WishBasket.add({ id, name: activityName, image: resolvedImgUrl, types: act.types || [] });
            window.WishBasket.refresh();
            showToast('Ajouté au panier !'); 
        }
    });

    preview.classList.add('show');
}

window.backToAppGrid = function() {
    const grid = document.getElementById('appGrid');
    const preview = document.getElementById('activityPreview');

    preview.classList.remove('show');
    grid.classList.remove('hidden');
    
    setTimeout(() => {
        if(!preview.classList.contains('show')) preview.innerHTML = ''; 
    }, 400);
};

window.closePhone = function(e) {
    const modal = document.getElementById('giantPhoneModal');
    const grid = document.getElementById('appGrid');
    const preview = document.getElementById('activityPreview');
    
    // On lance la superbe animation de fermeture (Rotation inverse)
    modal.classList.add('fade-out');
    
    // Une fois l'animation CSS terminée (300ms), on remet display à none pour éviter le bug de la page !
    setTimeout(() => {
        modal.style.display = 'none';
        modal.classList.remove('fade-out');
        grid.classList.remove('hidden');
        preview.classList.remove('show');
        preview.innerHTML = '';
    }, 300);
};