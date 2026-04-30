/* ====== Flags ====== */
const ENABLE_SERVER_FETCH = false; 
const ENABLE_ENRICH_NETWORK = false; 

/* ====== Constantes / helpers ====== */
const DEFAULT_ACTIVITY_IMG = "/static/img/no-image.jpg";
const JOURNEY_KEYS = ["wish_journeys_v1", "journeys"];
const BASKET_KEY = "wish_basket_v1";
const OBJ_CACHE_KEY = "wish_obj_cache_v1";
const LAST_ID_KEY = "wish_last_journey_id";

const isHttp = (u) => typeof u === "string" && /^https?:\/\//i.test(u);
const isData = (u) => typeof u === "string" && /^data:image\//i.test(u);
const isPath = (u) => typeof u === "string" && u.startsWith("/");
const isFileLike = (u) => typeof u === "string" && /\.(jpe?g|png|webp|gif|tiff?|bmp)$/i.test(u);
const badVal = (u) => !u || /^(match|default|placeholder|null|undefined)$/i.test(String(u).trim());
const A = (a) => Array.isArray(a) ? a : (a && typeof a === "object") ? Object.values(a) : [];
const pickFirstStr = (...vals) => vals.find(v => typeof v === "string" && v.trim()) || "";

/* ====== Storage ====== */
function loadAllLocalJourneys() {
    for (const k of JOURNEY_KEYS) {
        try { const arr = JSON.parse(localStorage.getItem(k) || "[]"); if (Array.isArray(arr) && arr.length) return arr; } catch { }
    }
    try { return JSON.parse(localStorage.getItem(JOURNEY_KEYS[0]) || "[]"); } catch { return []; }
}
function loadBasket() { try { return JSON.parse(localStorage.getItem(BASKET_KEY) || "[]"); } catch { return []; } }
function byLocalId(id) { return loadAllLocalJourneys().find(j => String(j.id) === String(id)) || null; }

function getJourneyId() {
    const m = location.pathname.match(/\/journeys\/view\/([^\/\?#]+)/);
    if (m) return m[1];
    const q = new URLSearchParams(location.search).get("id");
    if (q) return q;
    const last = localStorage.getItem(LAST_ID_KEY);
    if (last) return last;
    const first = (loadAllLocalJourneys()[0] || {}).id;
    return first || "";
}

/* ====== Normalisation ====== */
const SLOT_FR_EN = { matin:'morning', midi:'noon', aprem:'afternoon', soir:'evening' };
function slotsArrayToObj(slotsArr){
    const obj={morning:[],noon:[],afternoon:[],evening:[]};
    (Array.isArray(slotsArr)?slotsArr:[]).forEach(s=>{
    const raw=String(s?.key||"").toLowerCase();
    const k=SLOT_FR_EN[raw]||raw;
    if(obj[k]) obj[k]=A(s.items);
    });
    return obj;
}
function normalizeSlots(s){
    s=s||{};
    return {
    morning:   A(s.morning   ?? s.matin),
    noon:      A(s.noon      ?? s.midi),
    afternoon: A(s.afternoon ?? s.aprem),
    evening:   A(s.evening   ?? s.soir),
    };
}
function normalizeDayAny(d,i){
    if(!d||typeof d!=="object") return {day:(i||0)+1,slots:{morning:[],noon:[],afternoon:[],evening:[]}};
    const day=(d.day!=null)?Number(d.day):(d.jour!=null?Number(d.jour):(i||0)+1);
    if(Array.isArray(d.slots)) return {day,slots:slotsArrayToObj(d.slots)};
    const src = (d.slots && typeof d.slots==="object" && !Array.isArray(d.slots)) ? d.slots : { morning:d.morning??d.matin, noon:d.noon??d.midi, afternoon:d.afternoon??d.aprem, evening:d.evening??d.soir };
    return {day,slots:normalizeSlots(src)};
}
function deriveDays(j){
    if(Array.isArray(j?.plan)) return j.plan.map((d,i)=>normalizeDayAny(d,i));
    let days=j?.days ?? j?.days_json;
    if(typeof days==="string"){ try{ days=JSON.parse(days); }catch{ days=null; } }
    if(Array.isArray(days)) return days.map((d,i)=>normalizeDayAny(d,i));
    const s=normalizeSlots(j?.slots||{}); const total=s.morning.length+s.noon.length+s.afternoon.length+s.evening.length;
    return total?[{day:1,slots:s}]:[];
}

/* ====== Images ====== */
function resolveImageUrl(u){
    if (!u || badVal(u)) return "";
    u = String(u).trim();
    if (u.startsWith("data:image/")) return u;
    if (u.startsWith("//")) return "https:" + u;
    if (isHttp(u) || isPath(u)) return u;
    if (/^(?:data\.datatourisme\.fr|images?\.)/i.test(u)) return "https://" + u;
    if (isFileLike(u)) return "/static/img/phototheque/" + u.replace(/^\/+/, "");
    return "";
}
function pickLang(val){
    if(typeof val==="string") return val;
    if(Array.isArray(val)){
    const fr = val.find(v => v && (v["@language"]==="fr" || v.lang==="fr")); if(fr) return fr["@value"]||fr.value||"";
    const s  = val.find(v => typeof v==="string"); if(s) return s;
    const f  = val[0]; return (f && (f["@value"]||f.value)) || "";
    }
    if(val && typeof val==="object"){
    if(val.fr) return Array.isArray(val.fr)?val.fr[0]:val.fr;
    if(val["@value"]) return val["@value"];
    }
    return "";
}
function extractImage(obj){
    if(!obj || typeof obj!=="object") return "";
    let direct = resolveImageUrl(pickFirstStr(obj.image, obj.photo, obj.picture, obj.thumbnail, obj.cover, obj["https://schema.org/image"]));
    if(direct) return direct;
    for(const [k,val] of Object.entries(obj)){
        const kl = String(k||"").toLowerCase();
        if(!["image","photo","thumbnail","picture"].some(n => kl.includes(n))) continue;
        if(typeof val === "string") { const ru = resolveImageUrl(val); if(ru) return ru; }
    }
    return "";
}

/* ====== Index images ====== */
const objCache = (()=>{ try{ return JSON.parse(localStorage.getItem(OBJ_CACHE_KEY)||"{}"); }catch{ return {}; } })();
function saveObjCache(){ try{ localStorage.setItem(OBJ_CACHE_KEY, JSON.stringify(objCache)); }catch{} }
function activityId(a){ return a?.objectId || a?.id || a?._id || a?.["@id"] || a?.uri || a?.url || null; }

const ACT_IMG_INDEX = new Map();
function indexLocalActivityImages(){
    ACT_IMG_INDEX.clear();
    try{
        loadAllLocalJourneys().forEach(j=>{
            deriveDays(j).forEach(d=>{
                ["morning","noon","afternoon","evening"].forEach(slotK=>{
                    (d.slots?.[slotK]||[]).forEach(it=>{
                        const aid = String(activityId(it)||"");
                        const u = resolveImageUrl(pickFirstStr(it.image, it.photo) || extractImage(it));
                        if(aid && u && !badVal(u) && !ACT_IMG_INDEX.has(aid)) ACT_IMG_INDEX.set(aid, u);
                    });
                });
            });
        });
    }catch{}
}

function getItemImageSync(item){
    if(!item) return "";
    const prim = resolveImageUrl(pickFirstStr(item.image, item.photo, item.thumbnail));
    if(prim) return prim;
    const fromObj = extractImage(item);
    if(fromObj) return fromObj;
    const id = activityId(item);
    if(id){
        const cached = objCache[id]?.image && resolveImageUrl(objCache[id].image);
        if(cached) return cached;
        if(ACT_IMG_INDEX.has(String(id))) return ACT_IMG_INDEX.get(String(id));
    }
    return "";
}
function resolveActivityImage(a){ return getItemImageSync(a) || DEFAULT_ACTIVITY_IMG; }

/* ====== Fetch & fusion ====== */
async function tryEnrichThumbAsync(th, a){
    if (!ENABLE_ENRICH_NETWORK || !th || th.dataset.enriched === "1") return;
    const aid = activityId(a);
    if (!aid) return;
    const candidateUrls = [];
    if (isHttp(aid)) candidateUrls.push(aid);
    if (!isHttp(aid) && ENABLE_SERVER_FETCH){ candidateUrls.push(`/objects/${encodeURIComponent(aid)}`); }
    for (const url of candidateUrls){
        try{
            const r = await fetch(url, { cache:"no-store" });
            if (!r.ok) continue;
            const obj = await r.json();
            const img = resolveImageUrl(pickFirstStr(obj.image, obj.photo) || extractImage(obj) || obj.imageUrl);
            if (img){
                th.style.backgroundImage = `url('${img}')`; th.dataset.enriched = "1";
                const key = String(aid); objCache[key] = { ...(objCache[key]||{}), image: img };
                saveObjCache(); ACT_IMG_INDEX.set(key, img); return;
            }
        }catch{}
    }
}
async function fetchServerJourney(id){
    if(!ENABLE_SERVER_FETCH) return null;
    try{ const r=await fetch(`/journeys/${encodeURIComponent(id)}`); if(r.ok) return await r.json(); }catch{}
    return null;
}
function mergeJourneys(a={},b={}){ 
    const newer=(new Date(a?.updatedAt||0)>=new Date(b?.updatedAt||0))?a:b; const older=(newer===a)?b:a;
    const m={...older,...newer}; const nd=deriveDays(newer), od=deriveDays(older);
    if(!nd.length && od.length){ if(older.plan)m.plan=older.plan; }
    return m; 
}

/* ====== Méta ====== */
const SLOT_LABEL = { morning: "Matinée", noon: "Midi", afternoon: "Après-midi", evening: "Soirée" };
const fmtMeta = (a) => {
    const parts=[];
    const type = a?.type || a?.category || (Array.isArray(a?.categories) ? a.categories[0] : null) || (Array.isArray(a?.types) ? a.types[0] : null);
    if (type) parts.push(type);
    const place = a?.place || a?.location || a?.ville || a?.locality;
    if (place) parts.push(place);
    if (a?.price) parts.push(`${a.price}€`);
    return parts.length ? parts.join(" ~ ") : "Activité sélectionnée";
};

/* ====== Sauvegarde Champs ====== */
let _saveTimer = null;
function persistJourneyChanges(j){
    try {
        for (const k of JOURNEY_KEYS){
            const arr = JSON.parse(localStorage.getItem(k) || "[]");
            if (!Array.isArray(arr) || !arr.length) continue;
            const i = arr.findIndex(x => String(x.id) === String(j.id));
            if (i >= 0){
                arr[i] = { ...arr[i], persons: j.persons, price: j.price, updatedAt: new Date().toISOString() };
                localStorage.setItem(k, JSON.stringify(arr));
            }
        }
    } catch {}
    const badge = document.getElementById("saveBadge");
    if (badge){
        badge.hidden = false; badge.classList.remove("show");
        void badge.offsetWidth; badge.classList.add("show");
        clearTimeout(badge._t);
        badge._t = setTimeout(()=>{ badge.classList.remove("show"); badge.hidden = true; }, 1600);
    }
}
function wireSummaryEdits(j){
    const personsInput = document.getElementById("editPersons");
    const priceInput   = document.getElementById("editPrice");
    const onChange = () => {
        j.persons = personsInput && personsInput.value !== "" ? Math.max(1, parseInt(personsInput.value, 10)) : null;
        j.price   = priceInput   && priceInput.value   !== "" ? Math.max(0, parseInt(priceInput.value, 10))   : null;
        clearTimeout(_saveTimer); _saveTimer = setTimeout(() => persistJourneyChanges(j), 320);
    };
    if (personsInput) personsInput.addEventListener("input", onChange);
    if (priceInput)   priceInput.addEventListener("input", onChange);
}

// --- SYSTÈME D'IMAGES PREMIUM ---
const THEMES = {
    "Nature": {
        icones: [
            '/static/img/nature1.jpg',
            '/static/img/nature2.jpg',
            '/static/img/nature3.jpg',
            '/static/img/nature4.jpg',
            '/static/img/nature5.jpg',
            '/static/img/nature6.jpg'
        ]
    },
    "Gastronomie": {
        icones: [
            '/static/img/food1.jpg', 
            '/static/img/food2.jpg',
            '/static/img/food3.jpg',
            '/static/img/food4.jpg',
            '/static/img/food5.jpg',
            '/static/img/food6.jpg'
        ]
    },
    "Culture": {
        icones: [
            '/static/img/culture1.jpg', 
            '/static/img/culture2.jpg',
            '/static/img/culture3.jpg',
            '/static/img/culture4.jpg',
            '/static/img/culture5.jpg',
            '/static/img/culture6.jpg'
        ]
    },
    "Sport": {
        icones: [
            '/static/img/sport1.jpg', 
            '/static/img/sport2.jpg',
            '/static/img/sport3.jpg',
            '/static/img/sport4.jpg',
            '/static/img/sport5.png',
            '/static/img/sport6.jpg'
        ]
    },   
    "Détente": {
        icones: [
            '/static/img/detente1.jpg', 
            '/static/img/detente2.jpg',
            '/static/img/detente3.jpg',
            '/static/img/detente4.jpg',
            '/static/img/detente55.jpg',
            '/static/img/detente6.jpg'
        ]
    },
    "Shopping": {
        icones: [
            '/static/img/shopping1.jpg', 
            '/static/img/shopping2.jpg',
            '/static/img/shopping3.jpg',
            '/static/img/shopping4.jpg',
            '/static/img/shopping5.jpg',
            '/static/img/shopping6.jpg'
        ]
    }
};

const MIX = [0, 1, 2, 3, 4, 5, 2, 4, 0, 5, 1, 3, 4, 2, 5, 0, 3, 1, 5, 3, 1, 4, 2];

// Fonction utilitaire pour trouver la bonne catégorie
function getCategoryFromActivity(activity) {
    let typeStr = activity?.type || activity?.category || (Array.isArray(activity?.categories) ? activity.categories[0] : null) || (Array.isArray(activity?.types) ? activity.types[0] : null) || "";
    typeStr = String(typeStr).toLowerCase();

    if (typeStr.includes('nature') || typeStr.includes('parc') || typeStr.includes('jardin')) return "Nature";
    if (typeStr.includes('gastronomie') || typeStr.includes('restaurant') || typeStr.includes('food')) return "Gastronomie";
    if (typeStr.includes('culture') || typeStr.includes('musée') || typeStr.includes('patrimoine')) return "Culture";
    if (typeStr.includes('sport') || typeStr.includes('loisir') || typeStr.includes('aventure')) return "Sport";
    if (typeStr.includes('détente') || typeStr.includes('spa') || typeStr.includes('bien-être')) return "Détente";
    if (typeStr.includes('shopping') || typeStr.includes('boutique') || typeStr.includes('magasin')) return "Shopping";

    // Si on ne trouve pas de correspondance évidente, on choisit "Nature" par défaut
    return "Nature";
}
// --------------------------------


/* ====== Rendu ====== */
function render(j, dayIndex){
    localStorage.setItem(LAST_ID_KEY, String(j.id));

    const days = deriveDays(j);
    const N = Math.max(1, days.length||1);
    const idx = Math.min(Math.max(1, dayIndex||1), N);

    document.getElementById("journeyTitle").textContent = j?.name || "Voyage";
    document.getElementById("journeyLocation").textContent = j?.location || "Ville, lieux...";
    document.getElementById("dayTitle").textContent = `Jour ${idx}`;

    const personsInput = document.getElementById("editPersons");
    const priceInput   = document.getElementById("editPrice");
    if (personsInput) personsInput.value = (j?.persons ?? "") === "" ? "" : String(j.persons);
    if (priceInput)   priceInput.value   = (j?.price   ?? "") === "" ? "" : String(j.price);
    wireSummaryEdits(j);

    const d = days[idx-1] || days[0];
    const wrap = document.getElementById("slotsWrap");
    wrap.innerHTML = "";

    // Variable pour continuer la séquence MIX au fil des activités
    let globalActivityIndex = 0;

    ["morning","noon","afternoon","evening"].forEach(key=>{
        const acts=d?.slots?.[key]||[]; if(!acts.length) return;
        
        const sec=document.createElement("div"); sec.className="sec";
        const ttl=document.createElement("div"); ttl.className="sec-title"; ttl.textContent=SLOT_LABEL[key]||key;
        sec.appendChild(ttl);

        acts.forEach(a=>{
            const row = document.createElement("div");
            row.className = "act";
            row.tabIndex = 0;

            let firstImg = resolveActivityImage(a);
            const name = a?.name || a?.nom || a?.title || "Activité";
            const metaText = fmtMeta(a);
            
            // --- Logique d'application des images premium ---
            if (firstImg === DEFAULT_ACTIVITY_IMG || firstImg.includes('no-image') || firstImg.includes('appareil_photo')) {
                const category = getCategoryFromActivity(a);
                const currentTheme = THEMES[category] || THEMES["Nature"];
                const variantIndex = MIX[globalActivityIndex % MIX.length] % currentTheme.icones.length;
                firstImg = currentTheme.icones[variantIndex];
            }
            globalActivityIndex++; // On incrémente pour la prochaine activité sans image
            // ------------------------------------------------

            // On récupère la description si elle existe
            let desc = a?.description || a?.shortDescription || "";
            if (typeof desc === 'object') desc = desc.fr || desc.en || Object.values(desc)[0] || "";
            if (!desc || desc.trim() === "") desc = "Aucune description détaillée n'est disponible pour cette activité.";

            row.innerHTML = `
                <div class="act-header">
                    <div class="thumb" style="background-image: url('${firstImg}')"></div>
                    <div class="ameta">
                        <div class="aname">${name}</div>
                        <div class="adesc">${metaText}</div>
                    </div>
                    <svg viewBox="0 0 24 24" class="chev">
                        <path d="M9 18l6-6-6-6" stroke-width="2" fill="none" stroke-linecap="round" stroke-linejoin="round"></path>
                    </svg>
                </div>
                <div class="act-details">
                    <p class="act-desc-text">${desc}</p>
                </div>
            `;

            // On désactive tryEnrichThumbAsync car on a déjà géré l'image premium
            // if (firstImg === DEFAULT_ACTIVITY_IMG) {
            //     const th = row.querySelector('.thumb');
            //     tryEnrichThumbAsync(th, a);
            // }

            row.addEventListener("click", () => {
                row.classList.toggle("open");
            });
            row.addEventListener("keydown", (e) => {
                if (e.key === "Enter" || e.key === " ") {
                    e.preventDefault();
                    row.click();
                }
            });

            sec.appendChild(row);
        });

        wrap.appendChild(sec);
    });

    const notesKey=`journey_day_notes_${j.id}_${idx}`;
    const ta=document.getElementById("privateNotes");
    ta.value=localStorage.getItem(notesKey)||"";
    ta.addEventListener("input",()=>localStorage.setItem(notesKey,ta.value));
}

/* ====== Init ====== */
document.getElementById("backBtn").addEventListener("click",()=>history.back());
(async function init(){
    indexLocalActivityImages();

    const { id, day } = (function(){
        const m=location.pathname.match(/\/journeys\/view\/([^\/\?#]+)\/day\/(\d+)/);
        if(m) return { id:m[1], day:parseInt(m[2],10)||1 };
        const sp=new URLSearchParams(location.search);
        return { id: sp.get("id") || getJourneyId(), day: parseInt(sp.get("day")||"1",10) || 1 };
    })();

    if(!id){ document.getElementById("journeyTitle").textContent="Voyage introuvable"; return; }

    const local = byLocalId(id);
    const server = await fetchServerJourney(id);
    const j = (local && server) ? mergeJourneys(local, server) : (server || local);

    if(!j){ document.getElementById("journeyTitle").textContent="Voyage introuvable"; return; }
    render(j, day);
})();