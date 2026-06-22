// --- JOLIE NOTIFICATION FLOTTANTE ---
function showToast(message) {
    const toast = document.createElement('div');
    toast.textContent = message;
    toast.style.cssText = "position:fixed; bottom:30px; left:50%; transform:translateX(-50%); background:#1C1C1C; color:white; padding:12px 24px; border-radius:30px; z-index:10000; font-family:'Montserrat', sans-serif; font-size:12px; font-weight:700; text-transform:uppercase; letter-spacing:0.5px; box-shadow:0 4px 15px rgba(0,0,0,0.2); opacity:0; transition:opacity 0.3s;";
    document.body.appendChild(toast);
    
    setTimeout(() => toast.style.opacity = '1', 10);
    setTimeout(() => {
        toast.style.opacity = '0';
        setTimeout(() => toast.remove(), 300);
    }, 2500);
}

const NO_IMG = '/static/img/travel.jpg';
function getSafeImage(item) {
    if (!item) return NO_IMG;
    if (typeof window !== 'undefined' && typeof window.getActivityImage === 'function') {
        try { return window.getActivityImage(item); } catch(e) { }
    }
    return item.image || item.photo || item.cover || NO_IMG;
}

const escapeHtml = (s) => String(s ?? '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;').replace(/'/g, '&#39;');

document.addEventListener('DOMContentLoaded', async () => {
    const query = typeof CATEGORIE_ACTUELLE !== 'undefined' ? CATEGORIE_ACTUELLE : 'Catégorie';
    
    const pageTitleEl = document.getElementById('pageTitle');
    if (pageTitleEl) pageTitleEl.textContent = query.toUpperCase();
    
    const listContainer = document.getElementById('activitiesList');
    if (!listContainer) return;

    try {
        const res = await fetch(`/search?query=${encodeURIComponent(query)}&limit=30`);
        if (!res.ok) throw new Error("Erreur réseau");
        
        const data = await res.json();
        const items = Array.isArray(data) ? data : (data.items || []);

        if (items.length === 0) {
            listContainer.innerHTML = `<p style="text-align:center; color:#888; width: 100%;">Aucune activité trouvée pour la catégorie ${escapeHtml(query)}.</p>`;
            return;
        }

        listContainer.innerHTML = '';

        items.forEach((item) => {
            const id = item.id || item._id || item.url || '#';
            const name = escapeHtml(item.name || 'Activité sans nom');
            
            const imgUrl = getSafeImage(item);
            const safeImgHtml = escapeHtml(imgUrl);

            const type = (item.types && item.types[0]) ? item.types[0] : query;
            const desc = escapeHtml(item.description || "Aucune description détaillée n'est disponible pour cette activité. Laissez-vous surprendre sur place !");

            const card = document.createElement('div');
            card.className = 'activity-accordion';
            const isFav = window.WishLikes ? window.WishLikes.has(id) : false;

            // Ajout des data-id sur les boutons
            card.innerHTML = `
                <div class="alc-img-wrapper">
                    <img src="${safeImgHtml}" alt="" class="alc-img" onerror="this.src='${NO_IMG}'">
                </div>
                <div class="alc-body">
                    <div class="alc-title-container">
                        <h3 class="alc-title" title="${name}">${name}</h3>
                    </div>
                    
                    <div class="alc-actions">
                        <button class="btn-fav ${isFav ? 'active' : ''}" data-id="${id}" title="Ajouter aux favoris">❤</button>
                        <button class="btn-pan" data-id="${id}">+ Panier</button>
                        <button class="btn-expand" title="Lire la description"><span class="alc-chevron">❯</span></button>
                    </div>

                    <div class="activity-details">
                        <p class="alc-desc">${desc}</p>
                    </div>
                </div>
            `;

            const btnExpand = card.querySelector('.btn-expand');
            const titleEl = card.querySelector('.alc-title');
            
            const toggleDesc = () => card.classList.toggle('open');
            btnExpand.addEventListener('click', toggleDesc);
            titleEl.addEventListener('click', toggleDesc);
            titleEl.style.cursor = 'pointer';

            const btnFav = card.querySelector('.btn-fav');
            btnFav.addEventListener('click', (e) => {
                e.stopPropagation(); 
                if(window.WishLikes) {
                    window.WishLikes.toggle({ 
                        ...item,
                        id: id, 
                        name: item.name, 
                        image: imgUrl, 
                        types: item.types || [type] 
                    });
                    btnFav.classList.toggle('active', window.WishLikes.has(id));
                    window.WishLikes.refresh();
                }
            });

            const btnPan = card.querySelector('.btn-pan');
            btnPan.addEventListener('click', (e) => {
                e.stopPropagation(); 
                if(window.WishBasket) {
                    window.WishBasket.add({ 
                        ...item,
                        id: id, 
                        name: item.name, 
                        image: imgUrl, 
                        types: item.types || [type] 
                    });
                    window.WishBasket.refresh();
                    showToast('Ajouté au panier !'); 
                }
            });

            listContainer.appendChild(card);
        });

    } catch (error) {
        listContainer.innerHTML = `<p style="text-align:center; color:red; width: 100%;">Impossible de charger les activités.</p>`;
    }
});

// --- SYNCHRONISATION DES COEURS EN DIRECT ---
window.addEventListener('wishbasket:change', (e) => {
    if (window.WishLikes) {
        document.querySelectorAll('.btn-fav').forEach(btn => {
            const id = btn.dataset.id;
            if (id) {
                if (window.WishLikes.has(id)) {
                    btn.classList.add('active');
                } else {
                    btn.classList.remove('active');
                }
            }
        });
    }
});