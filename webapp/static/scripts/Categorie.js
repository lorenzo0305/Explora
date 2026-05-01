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

// --- SYSTÈME D'IMAGES PREMIUM ---
const THEMES = {
    "Nature": { icones: ['/static/img/nature1.jpg', '/static/img/nature2.jpg', '/static/img/nature3.jpg', '/static/img/nature4.jpg', '/static/img/nature5.jpg', '/static/img/nature6.jpg'] },
    "Gastronomie": { icones: ['/static/img/food1.jpg', '/static/img/food2.jpg', '/static/img/food3.jpg', '/static/img/food4.jpg', '/static/img/food5.jpg', '/static/img/food6.jpg'] },
    "Culture": { icones: ['/static/img/culture1.jpg', '/static/img/culture2.jpg', '/static/img/culture3.jpg', '/static/img/culture4.jpg', '/static/img/culture5.jpg', '/static/img/culture6.jpg'] },
    "Sport": { icones: ['/static/img/sport1.jpg', '/static/img/sport2.jpg', '/static/img/sport3.jpg', '/static/img/sport4.jpg', '/static/img/sport5.png', '/static/img/sport6.jpg'] },   
    "Détente": { icones: ['/static/img/detente1.jpg', '/static/img/detente2.jpg', '/static/img/detente3.jpg', '/static/img/detente4.jpg', '/static/img/detente55.jpg', '/static/img/detente6.jpg'] },
    "Shopping": { icones: ['/static/img/shopping1.jpg', '/static/img/shopping2.jpg', '/static/img/shopping3.jpg', '/static/img/shopping4.jpg', '/static/img/shopping5.jpg', '/static/img/shopping6.jpg'] }
};

const MIX = [0, 1, 2, 3, 4, 5, 2, 4, 0, 5, 1, 3, 4, 2, 5, 0, 3, 1, 5, 3, 1, 4, 2];

// Initialisation au chargement
document.addEventListener('DOMContentLoaded', async () => {
    const query = CATEGORIE_ACTUELLE;
    document.getElementById('pageTitle').textContent = query.toUpperCase();
    const currentTheme = THEMES[query] || THEMES["Nature"];
    const listContainer = document.getElementById('activitiesList');

    try {
        const res = await fetch(`/search?query=${encodeURIComponent(query)}&limit=50`);
        if (!res.ok) throw new Error("Erreur réseau");
        
        const data = await res.json();
        const items = Array.isArray(data) ? data : (data.items || []);

        if (items.length === 0) {
            listContainer.innerHTML = `<p style="text-align:center; color:#888;">Aucune activité trouvée pour la catégorie ${query}.</p>`;
            return;
        }

        listContainer.innerHTML = '';

        items.forEach((item, index) => {
            const id = item.id || item._id || item.url || '#';
            const name = item.name || 'Activité sans nom';
            
            let imgUrl = "";
            if (item.image && !item.image.includes('no-image') && !item.image.includes('appareil_photo')) { imgUrl = item.image; } 
            else if (item.photo && !item.photo.includes('no-image') && !item.photo.includes('appareil_photo')) { imgUrl = item.photo; } 
            else if (item.thumbnail && !item.thumbnail.includes('no-image') && !item.thumbnail.includes('appareil_photo')) { imgUrl = item.thumbnail; } 
            else {
                const variantIndex = MIX[index % MIX.length] % currentTheme.icones.length;
                imgUrl = currentTheme.icones[variantIndex];
            }

            const type = (item.types && item.types[0]) ? item.types[0].toLowerCase() : query.toLowerCase();
            const locality = item.locality || item.region || 'Lieu inconnu';
            const metaText = `${type} ~ ${locality}`;
            const desc = item.description || "Aucune description détaillée n'est disponible pour cette activité. Laissez-vous surprendre sur place !";

            const card = document.createElement('div');
            card.className = 'activity-accordion';
            const isFav = window.WishLikes ? window.WishLikes.has(id) : false;

            card.innerHTML = `
                <div class="activity-header">
                    <div class="alc-left">
                        <img src="${imgUrl}" alt="" class="alc-img">
                        <div class="alc-info">
                            <h3 class="alc-title">${name}</h3>
                            <p class="alc-meta">${metaText}</p>
                        </div>
                    </div>
                    <div class="alc-right-group">
                        <div class="alc-actions-header">
                            <!-- HOVER SUPPRIMÉ ICI -->
                            <button class="btn-fav ${isFav ? 'active' : ''}">❤</button>
                            <button class="btn-pan">+ Panier</button>
                        </div>
                        <div class="alc-chevron">❯</div>
                    </div>
                </div>
                <div class="activity-details"><p class="alc-desc">${desc}</p></div>
            `;

            const header = card.querySelector('.activity-header');
            header.addEventListener('click', (e) => {
                if (!e.target.closest('button')) card.classList.toggle('open');
            });

            const btnFav = card.querySelector('.btn-fav');
            btnFav.addEventListener('click', (e) => {
                e.stopPropagation(); 
                if(window.WishLikes) {
                    window.WishLikes.toggle({ id, name, image: imgUrl, types: item.types || [] });
                    btnFav.classList.toggle('active', window.WishLikes.has(id));
                    window.WishLikes.refresh();
                }
            });

            const btnPan = card.querySelector('.btn-pan');
            btnPan.addEventListener('click', (e) => {
                e.stopPropagation(); 
                if(window.WishBasket) {
                    window.WishBasket.add({ id, name, image: imgUrl, types: item.types || [] });
                    window.WishBasket.refresh();
                    showToast('Ajouté au panier !'); 
                }
            });

            listContainer.appendChild(card);
        });

    } catch (error) {
        listContainer.innerHTML = `<p style="text-align:center; color:red;">Impossible de charger les activités.</p>`;
    }
});