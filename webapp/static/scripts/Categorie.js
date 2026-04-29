// Outils pour la gestion du panier et des likes
const BASKET_KEY = 'wish_basket_v1';
const LIKES_KEY  = 'wish_likes_v1';

function loadKey(k) { try { return JSON.parse(localStorage.getItem(k) || '[]') || []; } catch { return []; } }
function saveKey(k, v) { localStorage.setItem(k, JSON.stringify(v || [])); }

function updateBadge(badgeId, count) {
    const el = document.getElementById(badgeId);
    if (el) { el.textContent = String(count); el.hidden = count === 0; }
}

function addBasket(item) {
    const list = loadKey(BASKET_KEY);
    if (list.some(x => String(x.id) === String(item.id))) return false;
    list.unshift(item);
    saveKey(BASKET_KEY, list);
    updateBadge('basketCount', list.length);
    alert('Ajouté au panier !'); // Petite confirmation visuelle
    return true;
}

function toggleLike(item) {
    let list = loadKey(LIKES_KEY);
    const exists = list.some(x => String(x.id) === String(item.id));
    if (exists) list = list.filter(x => String(x.id) !== String(item.id));
    else list.unshift(item);
    saveKey(LIKES_KEY, list);
    updateBadge('likesCount', list.length);
    return !exists;
}

function isLiked(id) {
    return loadKey(LIKES_KEY).some(x => String(x.id) === String(id));
}

// Initialisation au chargement
document.addEventListener('DOMContentLoaded', async () => {
    updateBadge('basketCount', loadKey(BASKET_KEY).length);
    updateBadge('likesCount', loadKey(LIKES_KEY).length);

    const query = CATEGORIE_ACTUELLE;
    document.getElementById('pageTitle').textContent = query.toUpperCase();

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

        items.forEach(item => {
            const id = item.id || item._id || item.url || '#';
            const name = item.name || 'Activité sans nom';
            
            let imgUrl = '/static/img/no-image.jpg';
            if (item.image) imgUrl = item.image;
            else if (item.photo) imgUrl = item.photo;
            else if (item.thumbnail) imgUrl = item.thumbnail;

            const type = (item.types && item.types[0]) ? item.types[0].toLowerCase() : query.toLowerCase();
            const locality = item.locality || item.region || 'Lieu inconnu';
            const metaText = `${type} ~ ${locality}`;
            
            // On récupère la description de la BDD
            const desc = item.description || "Aucune description détaillée n'est disponible pour cette activité. Laissez-vous surprendre sur place !";

            const card = document.createElement('div');
            card.className = 'activity-accordion';
            const isFav = isLiked(id);

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
                            <button class="btn-fav ${isFav ? 'active' : ''}" title="Ajouter aux favoris">❤</button>
                            <button class="btn-pan" title="Ajouter au planning">+ Panier</button>
                        </div>
                        <div class="alc-chevron">❯</div>
                    </div>
                </div>
                <div class="activity-details">
                    <p class="alc-desc">${desc}</p>
                </div>
            `;

            // 1. Déplier/Enrouler l'accordéon (uniquement si on ne clique pas sur un bouton)
            const header = card.querySelector('.activity-header');
            header.addEventListener('click', (e) => {
                if (!e.target.closest('button')) {
                    card.classList.toggle('open');
                }
            });

            // 2. Action Cœur
            const btnFav = card.querySelector('.btn-fav');
            btnFav.addEventListener('click', (e) => {
                e.stopPropagation(); // Empêche l'accordéon de s'ouvrir
                const liked = toggleLike({ id, name, image: imgUrl, types: item.types || [] });
                btnFav.classList.toggle('active', liked);
            });

            // 3. Action Panier
            const btnPan = card.querySelector('.btn-pan');
            btnPan.addEventListener('click', (e) => {
                e.stopPropagation(); // Empêche l'accordéon de s'ouvrir
                addBasket({ id, name, image: imgUrl, types: item.types || [] });
            });

            listContainer.appendChild(card);
        });

    } catch (error) {
        console.error(error);
        listContainer.innerHTML = `<p style="text-align:center; color:red;">Impossible de charger les activités. Veuillez réessayer plus tard.</p>`;
    }
});