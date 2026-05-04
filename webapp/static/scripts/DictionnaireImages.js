// --- /static/scripts/imageDictionary.js ---

const THEMES = {
    "Nature": { icones: ['/static/img/nature1.jpg', '/static/img/nature2.jpg', '/static/img/nature3.jpg', '/static/img/nature4.jpg', '/static/img/nature5.jpg', '/static/img/nature6.jpg'] },
    "Gastronomie": { icones: ['/static/img/food1.jpg', '/static/img/food2.jpg', '/static/img/food3.jpg', '/static/img/food4.jpg', '/static/img/food5.jpg', '/static/img/food6.jpg'] },
    "Culture": { icones: ['/static/img/culture1.jpg', '/static/img/culture2.jpg', '/static/img/culture3.jpg', '/static/img/culture4.jpg', '/static/img/culture5.jpg', '/static/img/culture6.jpg'] },
    "Sport": { icones: ['/static/img/sport1.jpg', '/static/img/sport2.jpg', '/static/img/sport3.jpg', '/static/img/sport4.jpg', '/static/img/sport5.png', '/static/img/sport6.jpg'] },   
    "Détente": { icones: ['/static/img/detente1.jpg', '/static/img/detente2.jpg', '/static/img/detente3.jpg', '/static/img/detente4.jpg', '/static/img/detente55.jpg', '/static/img/detente6.jpg'] },
    "Shopping": { icones: ['/static/img/shopping1.jpg', '/static/img/shopping2.jpg', '/static/img/shopping3.jpg', '/static/img/shopping4.jpg', '/static/img/shopping5.jpg', '/static/img/shopping6.jpg'] },
    "Defaut": { icones: ['/static/img/travel.jpg', '/static/img/bordeaux.jpg', '/static/img/roussillon.jpg', '/static/img/chamonix.jpg', '/static/img/autoir.jpg'] }
};

const CATEGORY_MAP = {
    "nature": "Nature", "parc": "Nature", "randonnée": "Nature", "randonnee": "Nature", "jardin": "Nature", "foret": "Nature",
    "gastronomie": "Gastronomie", "restaurant": "Gastronomie", "bar": "Gastronomie", "dégustation": "Gastronomie", "degustation": "Gastronomie", "food": "Gastronomie",
    "culture": "Culture", "musée": "Culture", "musee": "Culture", "patrimoine": "Culture", "histoire": "Culture", "site": "Culture",
    "sport": "Sport", "loisir": "Sport", "vélo": "Sport", "velo": "Sport", "aventure": "Sport", "kayak": "Sport",
    "détente": "Détente", "detente": "Détente", "spa": "Détente", "bien-être": "Détente", "bien-etre": "Détente", "relaxation": "Détente",
    "shopping": "Shopping", "boutique": "Shopping", "magasin": "Shopping", "marché": "Shopping", "marche": "Shopping"
};

window.getActivityImage = function(activity) {
    if (!activity) return THEMES["Defaut"].icones[0];

    let imgUrl = activity.image || activity.imageUrl || activity.photo || activity.cover;
    if (imgUrl && typeof imgUrl === 'string' && !imgUrl.includes('no-image') && !imgUrl.includes('no-img') && !imgUrl.includes('appareil_photo')) {
        return imgUrl;
    }

    let firstCat = "";
    let catsRaw = activity.categories || activity.types || activity.category || activity.type || [];

    if (Array.isArray(catsRaw) && catsRaw.length > 0) {
        firstCat = typeof catsRaw[0] === 'string' ? catsRaw[0] : (catsRaw[0].name || "");
    } else if (typeof catsRaw === 'string') {
        firstCat = catsRaw.split(',')[0].trim();
    } else if (catsRaw && catsRaw.name) {
        firstCat = catsRaw.name;
    }

    firstCat = firstCat.toLowerCase().normalize("NFD").replace(/[\u0300-\u036f]/g, ""); 

    let selectedThemeKey = "Defaut";
    for (const [key, themeName] of Object.entries(CATEGORY_MAP)) {
        let cleanKey = key.toLowerCase().normalize("NFD").replace(/[\u0300-\u036f]/g, "");
        if (firstCat.includes(cleanKey)) {
            selectedThemeKey = themeName;
            break;
        }
    }

    const imagesArray = THEMES[selectedThemeKey].icones;
    const stringForHash = String(activity.id || activity._id || activity.nom || activity.name || "defaut");
    let hash = 0;
    for (let i = 0; i < stringForHash.length; i++) {
        hash = stringForHash.charCodeAt(i) + ((hash << 5) - hash);
    }
    const index = Math.abs(hash) % imagesArray.length;

    return imagesArray[index];
};