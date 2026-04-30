/* ================== Utilitaires ================== */
const DEFAULT_COVER = "/static/img/no-image.jpg";
const isDefaultCover = (u) => !u || String(u).includes(DEFAULT_COVER);

function getDetailId() {
    const m = location.pathname.match(/\/(?:detail-act-perso|object)\/(.+)$/);
    if (!m) return "";
    const raw = m[1];
    try { const once = decodeURIComponent(raw); try { return decodeURIComponent(once); } catch { return once; } }
    catch { return raw; }
}
function showErrorUI(msg) {
    console.error(msg);
    document.querySelector(".title")?.insertAdjacentText("afterbegin", "Détail — ");
    const nameEl = document.querySelector(".name");
    if (nameEl && !nameEl.textContent.trim()) nameEl.textContent = "Activité introuvable";
    const card = document.getElementById("descBox");
    if (card) card.textContent = typeof msg === "string" ? msg : "Impossible de récupérer les données de l’activité.";
    const cover = document.querySelector(".cover");
    if (cover) {
        const current = cover.getAttribute("src") || "";
        if (isDefaultCover(current)) cover.src = DEFAULT_COVER; // ne pas downgrader si une bonne image est déjà affichée
    }
}

/* ================== Lookup local (voyages) ================== */
const JOURNEY_KEYS = ["wish_journeys_v1", "journeys"];
function loadAllJourneys() {
    for (const k of JOURNEY_KEYS) {
        try {
            const arr = JSON.parse(localStorage.getItem(k) || "[]");
            if (Array.isArray(arr) && arr.length) return arr;
        } catch { }
    }
    try { return JSON.parse(localStorage.getItem(JOURNEY_KEYS[0]) || "[]"); } catch { return []; }
}
const normArr = a => Array.isArray(a) ? a : (a && typeof a === "object") ? Object.values(a) : [];
function slotsArrayToObj(slotsArr) {
    const o = { morning: [], noon: [], afternoon: [], evening: [] };
    (Array.isArray(slotsArr) ? slotsArr : []).forEach(s => {
        const k = String(s?.key || "").toLowerCase();
        if (o[k]) o[k] = normArr(s.items);
    });
    return o;
}
function normalizeDayAny(d, i) {
    const day = (d && d.day != null) ? Number(d.day) : (i || 0) + 1;
    if (Array.isArray(d?.slots)) return { day, slots: slotsArrayToObj(d.slots) };
    return {
        day,
        slots: {
            morning: normArr(d?.slots?.morning || d?.morning),
            noon: normArr(d?.slots?.noon || d?.noon),
            afternoon: normArr(d?.slots?.afternoon || d?.afternoon),
            evening: normArr(d?.slots?.evening || d?.evening),
        }
    };
}
function deriveDays(j) {
    if (Array.isArray(j?.plan)) return j.plan.map((d, i) => normalizeDayAny(d, i));
    let days = j?.days ?? j?.days_json;
    if (typeof days === "string") { try { days = JSON.parse(days); } catch { days = null; } }
    if (Array.isArray(days)) return days.map((d, i) => normalizeDayAny(d, i));
    const s = normalizeDayAny({ slots: j?.slots || {} }, 0).slots;
    const total = s.morning.length + s.noon.length + s.afternoon.length + s.evening.length;
    return total ? [{ day: 1, slots: s }] : [];
}
function activityId(a) {
    return a?.objectId || a?.id || a?._id || a?.["@id"] || a?.uri || a?.url || null;
}
function idsEqual(a, b) {
    if (!a || !b) return false;
    a = String(a); b = String(b);
    if (a === b) return true;
    try { if (decodeURIComponent(a) === b) return true; } catch { }
    try { if (a === decodeURIComponent(b)) return true; } catch { }
    return false;
}
function findLocalObjectById(targetId) {
    const journeys = loadAllJourneys();
    for (const j of journeys) {
        const days = deriveDays(j);
        for (const d of days) {
            for (const k of ["morning", "noon", "afternoon", "evening"]) {
                for (const a of normArr(d.slots?.[k])) {
                    const aid = activityId(a);
                    if (idsEqual(aid, targetId)) return a;
                }
            }
        }
    }
    return null;
}

/* ================== Réseau (proxy-first + IDs courts) ================== */
function asRemoteUrl(id) {
    if (!id) return null;
    if (/^https?:\/\//i.test(id)) return id;
    if (/^\d{2}\//.test(id)) return `https://data.datatourisme.fr/${id}`; // ex: 13/uuid
    if (/^data\.datatourisme\.fr\//i.test(id)) return `https://${id}`;
    return null;
}
async function tryJsonResponse(r) {
    const ct = (r.headers && r.headers.get && r.headers.get("content-type")) || "";
    if (/json|ld\+json/i.test(ct)) return await r.json();
    const txt = await r.text();
    try { return JSON.parse(txt); } catch {
        console.warn("Réponse non-JSON:", txt.slice(0, 160) + (txt.length > 160 ? "…" : ""));
        throw new Error("Réponse non-JSON");
    }
}
async function fetchObjectRemote(id) {
    const canon = asRemoteUrl(id) || id;
    if (!/^https?:\/\//i.test(canon)) return null;
    const tries = [
        `/api/object?url=${encodeURIComponent(canon)}`,
        `/proxy?url=${encodeURIComponent(canon)}`,
    ];
    for (const u of tries) {
        try {
            const r = await fetch(u);
            if (r.ok) return await tryJsonResponse(r);
            const body = await r.text().catch(() => "(no body)");
            console.warn(u, "→", r.status, body.slice(0, 160));
        } catch (e) { console.warn(u, "échoué:", e); }
    }
    try {
        const r = await fetch(canon, { headers: { "Accept": "application/ld+json, application/json;q=0.9, */*;q=0.1" } });
        if (r.ok) return await tryJsonResponse(r);
    } catch (e) { console.warn("Direct fetch échoué:", e); }
    try {
        const url1 = canon.includes("?") ? canon + "&format=jsonld" : canon + "?format=jsonld";
        const r1 = await fetch(url1, { headers: { "Accept": "application/ld+json" } });
        if (r1.ok) return await tryJsonResponse(r1);
    } catch (e) { console.warn("fetch ?format=jsonld échoué:", e); }
    return null;
}

/* ================== Normalisation (IRIs + FR-only desc) ================== */
function pickLang(val) {
    if (typeof val === "string") return val;
    if (Array.isArray(val)) {
        let fr = val.find(v => v && (v["@language"] === "fr" || v.lang === "fr"));
        if (fr) return fr["@value"] || fr.value || "";
        let en = val.find(v => v && (v["@language"] === "en" || v.lang === "en"));
        if (en) return en["@value"] || en.value || "";
        const s = val.find(v => typeof v === "string");
        if (s) return s;
        const first = val[0];
        return (first && (first["@value"] || first.value)) || "";
    }
    if (val && typeof val === "object") {
        if (val.fr) return Array.isArray(val.fr) ? val.fr[0] : val.fr;
        if (val.en) return Array.isArray(val.en) ? val.en[0] : val.en;
        if (val["@value"]) return val["@value"];
    }
    return "";
}
function firstString() {
    for (let i = 0; i < arguments.length; i++) {
        const c = arguments[i];
        if (!c) continue;
        if (typeof c === "string" && c.trim()) return c.trim();
        if (Array.isArray(c)) {
            const s = c.find(x => typeof x === "string" && x.trim());
            if (s) return s.trim();
            const t = c.find(x => x && (x["@value"] || x.value));
            if (t) return (t["@value"] || t.value || "").trim();
        }
        if (typeof c === "object") {
            const p = pickLang(c);
            if (p && typeof p === "string" && p.trim()) return p.trim();
        }
    }
    return "";
}
function arrayOfStrings(x) {
    if (!x) return [];
    if (typeof x === "string") return [x];
    if (Array.isArray(x)) return x.map(e => (typeof e === "string" ? e : (e && (e["@value"] || e.value) || ""))).filter(Boolean);
    if (typeof x === "object") {
        const v = pickLang(x);
        return v ? [v] : [];
    }
    return [];
}
// FR-only
function getLangStrings(v, lang = 'fr') {
    const out = [];
    if (!v) return out;
    if (typeof v === 'string') { out.push(v); return out; }
    if (Array.isArray(v)) { v.forEach(x => out.push(...getLangStrings(x, lang))); return out; }
    if (typeof v === 'object') {
        const fr = v[lang] || v[lang.toUpperCase()] || v[`${lang}-FR`] || v[`${lang}-fr`];
        if (fr) {
            if (Array.isArray(fr)) fr.forEach(s => typeof s === 'string' && out.push(s));
            else if (typeof fr === 'string') out.push(fr);
        } else if ((v['@language'] && String(v['@language']).toLowerCase().startsWith(lang)) && (v['@value'] || v.value)) {
            out.push(v['@value'] || v.value);
        }
    }
    return out;
}

const K_LABEL = [
    "name", "title", "dc:title", "rdfs:label", "schema:name",
    "http://www.w3.org/2000/01/rdf-schema#label",
    "https://schema.org/name"
];
const K_IMAGE = ["image", "photo", "thumbnail", "picture", "cover", "https://schema.org/image"];
const K_CONTACT = {
    homepage: ["homepage", "website", "url", "sameAs", "http://xmlns.com/foaf/0.1/homepage", "https://schema.org/url"],
    telephone: ["telephone", "phone", "contactPhone", "https://schema.org/telephone"],
    email: ["email", "mail", "contactEmail", "https://schema.org/email"]
};

// Raccourcit un IRI de type pour l’affichage
function shortType(t) {
    if (!t) return "";
    if (/^https?:\/\//.test(t)) {
        const lastHash = t.split("#").pop();
        const last = lastHash.includes("/") ? lastHash.split("/").pop() : lastHash;
        return last.replace(/_/g, " ");
    }
    return String(t);
}

function extractImage(obj) {
    // 1) champs directs / schema:image
    const direct = firstString(...K_IMAGE.map(k => obj[k]));
    if (direct) return direct;

    // 2) Datatourisme: has(Main)Representation → ebucore:hasRelatedResource → ebucore:locator
    const dtRepKeys = [
        "https://www.datatourisme.fr/ontology/core#hasMainRepresentation",
        "https://www.datatourisme.fr/ontology/core#hasRepresentation",
        "hasMainRepresentation", "hasRepresentation"
    ];
    for (const k of dtRepKeys) {
        const reps = obj[k];
        if (Array.isArray(reps)) {
            for (const rep of reps) {
                if (!rep || typeof rep !== "object") continue;
                const rels = [].concat(rep["ebucore:hasRelatedResource"] || [], rep.hasRelatedResource || []);
                for (const r of rels) {
                    if (!r || typeof r !== "object") continue;
                    const locs = [].concat(r["ebucore:locator"] || [], r.locator || []);
                    for (const loc of locs) {
                        if (typeof loc === "string" && /\.(jpg|jpeg|png|gif|webp)(\?|#|$)/i.test(loc)) return loc;
                        if (loc && typeof loc === "object") {
                            const u = firstString(loc.url, loc["@id"], loc["@value"]);
                            if (u && /\.(jpg|jpeg|png|gif|webp)(\?|#|$)/i.test(u)) return u;
                        }
                    }
                }
            }
        }
    }

    // 3) objets {contentUrl}/{url}
    for (const v of Object.values(obj)) {
        if (Array.isArray(v)) {
            for (const it of v) {
                if (it && typeof it === "object") {
                    const url = firstString(it.contentUrl, it.url, it["@id"], it["https://schema.org/contentUrl"], it["https://schema.org/url"]);
                    if (url && /^https?:\/\//i.test(url) && /\.(jpg|jpeg|png|gif|webp)(\?|#|$)/i.test(url)) return url;
                }
            }
        }
    }

    // 4) heuristique générique
    for (const [k, v] of Object.entries(obj)) {
        if (!/image|photo|thumbnail|picture|illustration/i.test(k)) continue;
        const arr = arrayOfStrings(v);
        const found = arr.find(u => /^https?:\/\/.+\.(jpg|jpeg|png|webp|gif)(\?|#|$)/i.test(u));
        if (found) return found;
    }
    return DEFAULT_COVER;
}

function normalizeContacts(obj) {
    const contacts = [];
    const pick = (keys) => firstString(...keys.map(k => obj[k]));
    const site = pick(K_CONTACT.homepage);
    const tel = pick(K_CONTACT.telephone);
    const mail = pick(K_CONTACT.email);
    if (site || tel || mail) contacts.push({ homepage: site, telephone: tel, email: mail });

    const groups = [].concat(
        obj.hasContact || [], obj.contact || [], obj.contacts || [], obj.contactPoint || [], obj["schema:contactPoint"] || [],
        obj["https://www.datatourisme.fr/ontology/core#hasContact"] || []
    );
    groups.forEach(c => {
        if (!c || typeof c !== "object") return;
        const s = firstString(...K_CONTACT.homepage.map(k => c[k]));
        const t = firstString(...K_CONTACT.telephone.map(k => c[k]));
        const m = firstString(...K_CONTACT.email.map(k => c[k]));
        if (s || t || m) contacts.push({ homepage: s, telephone: t, email: m });
    });
    const id = getDetailId();
    if (!contacts.length && /^https?:\/\//i.test(id)) contacts.push({ homepage: id });
    return contacts;
}

function extractDescriptionsFR(obj) {
    const DIRECT_KEYS = [
        'description', 'dc:description', 'schema:description',
        'rdfs:comment', 'http://www.w3.org/2000/01/rdf-schema#comment',
        'shortDescription', 'longDescription',
        'https://www.datatourisme.fr/ontology/core#shortDescription',
        'https://www.datatourisme.fr/ontology/core#longDescription',
    ];
    let descs = [];
    DIRECT_KEYS.forEach(k => descs = descs.concat(getLangStrings(obj[k], 'fr')));

    const groups = [].concat(
        obj.hasDescription || [],
        obj['https://www.datatourisme.fr/ontology/core#hasDescription'] || []
    );
    groups.forEach(d => {
        descs = descs.concat(
            getLangStrings(d?.shortDescription, 'fr'),
            getLangStrings(d?.longDescription, 'fr'),
            getLangStrings(d?.description, 'fr'),
            getLangStrings(d?.['dc:description'], 'fr'),
            getLangStrings(d?.['schema:description'], 'fr'),
            getLangStrings(d?.['rdfs:comment'], 'fr'),
            getLangStrings(d?.['http://www.w3.org/2000/01/rdf-schema#comment'], 'fr')
        );
    });

    return descs.map(s => s.trim()).filter((s, i, a) => s && a.indexOf(s) === i);
}

function normalizeObject(objLike) {
    const obj = objLike || {};

    // name
    const K_LABEL = [
        "name", "title", "dc:title", "rdfs:label", "schema:name",
        "http://www.w3.org/2000/01/rdf-schema#label",
        "https://schema.org/name"
    ];
    const name =
        firstString(...K_LABEL.map(k => obj[k])) ||
        (obj['@id'] ? String(obj['@id']).split('/').pop() : 'Activité');

    // types
    let types = [];
    if (Array.isArray(obj['@type'])) types = obj['@type'].map(String);
    else if (obj['@type']) types = [String(obj['@type'])];
    else if (obj.type) types = arrayOfStrings(obj.type);

    // desc FR
    const descriptions = extractDescriptionsFR(obj);

    // image + contacts
    const image = extractImage(obj);
    const contacts = normalizeContacts(obj);

    return { name, types, descriptions, image, contacts, raw: obj };
}

/* ================== Rendu ================== */
function setText(sel, txt) { const el = document.querySelector(sel); if (el) el.textContent = txt; }

// 👇 Upgrade-only : on ne remplace JAMAIS une image déjà bonne par le placeholder
function setCover(src) {
    const img = document.querySelector(".cover"); if (!img) return;
    const current = img.getAttribute("src") || "";
    const newSrc = src || "";
    const curIsDefault = isDefaultCover(current);
    const newIsDefault = isDefaultCover(newSrc);

    // Si la nouvelle est défaut mais l'actuelle ne l'est pas → ne rien faire (évite le “switch”)
    if (newIsDefault && !curIsDefault) return;

    // Sinon, on met à jour si différent
    if (newSrc && newSrc !== current) img.src = newSrc;
}

function renderType(t) {
    const el = document.querySelector(".name-type .type");
    if (!el) return;
    el.textContent = t ? "— " + shortType(t) : "";
}
function renderContacts(list) {
    const wrap = document.querySelector(".contacts");
    if (!wrap) return;
    wrap.innerHTML = "";
    if (!list || !list.length) {
        wrap.innerHTML = `<div class="contact-line">Aucun contact fourni.</div>`;
        return;
    }
    list.forEach(c => {
        const line = document.createElement("div");
        line.className = "contact-line";
        if (c.homepage) {
            const a = document.createElement("a");
            a.href = c.homepage; a.target = "_blank"; a.rel = "noopener";
            a.textContent = "Site";
            line.appendChild(a);
        }
        if (c.telephone) {
            const s = document.createElement("span");
            s.textContent = (c.homepage ? " • " : "") + c.telephone;
            line.appendChild(s);
        }
        if (c.email) {
            const s = document.createElement("span");
            s.innerHTML = (c.homepage || c.telephone ? " • " : "") + `<a href="mailto:${c.email}">Email</a>`;
            line.appendChild(s);
        }
        wrap.appendChild(line);
    });
}
function renderDescription(descriptions) {
    const box = document.getElementById("descBox");
    if (!box) return;
    const arr = Array.isArray(descriptions) ? descriptions.filter(Boolean) : [];
    if (!arr.length) {
        // Ne rien faire → on garde le fallback rendu par Jinja (serveur)
        return;
    }
    box.textContent = arr.join("\n\n");
}


// (Optionnel) préremplir depuis l’URL: /object/:id?img=...&name=...
function prefillFromQuery() {
    const p = new URLSearchParams(location.search);
    const img = p.get("img");
    const nm = p.get("name");
    if (img) setCover(img);
    if (nm) setText(".name", nm);
}

/* ================== Notes & Photos ================== */
function initNotes() {
    const objId = getDetailId();
    const key = objId ? `object_notes_${objId}` : null;
    const ta = document.getElementById("privateNotes");
    if (!key || !ta) return;
    try { ta.value = localStorage.getItem(key) || ""; } catch (e) { }
    ta.addEventListener("input", () => { try { localStorage.setItem(key, ta.value); } catch (e) { } });
}
function initPhotos() {
    const objId = getDetailId();
    const key = objId ? `object_photos_${objId}` : null;
    const grid = document.getElementById("photosGrid");
    const input = document.getElementById("fileInput");
    const add = document.getElementById("addPhoto");
    function renderPhotos(urls) {
        Array.from(grid.querySelectorAll(".ph.item")).forEach(n => n.remove());
        (urls || []).forEach(u => {
            const d = document.createElement("div");
            d.className = "ph item";
            const img = document.createElement("img");
            img.src = u;
            d.appendChild(img);
            grid.appendChild(d);
        });
    }
    function load() { if (!key) return []; try { return JSON.parse(localStorage.getItem(key) || "[]"); } catch { return [] } }
    function save(urls) { if (!key) return; try { localStorage.setItem(key, JSON.stringify(urls)); } catch { } }
    add.addEventListener("click", () => input.click());
    input.addEventListener("change", async (e) => {
        const files = Array.from(e.target.files || []);
        if (!files.length) return;
        const urls = load();
        for (const f of files) {
            const dataUrl = await new Promise((res, rej) => { const r = new FileReader(); r.onload = () => res(r.result); r.onerror = rej; r.readAsDataURL(f); });
            urls.push(dataUrl);
        }
        save(urls); renderPhotos(urls); input.value = "";
    });
    renderPhotos(load());
}

/* ================== INIT ================== */
document.getElementById("backBtn").addEventListener("click", () => history.back());

(async function init() {
    // Préremplissage éventuel (ne fait rien si pas de ?img=)
    prefillFromQuery();

    const id = getDetailId();
    if (!id) {
        showErrorUI("ID introuvable dans l’URL.");
        return;
    }
    console.debug("DetailActPerso id =", id);

    // 1) local d’abord
    let obj = findLocalObjectById(id);
    if (!obj) console.warn("Objet non trouvé en local, tentative réseau…");

    // 2) réseau — proxy d'abord (évite CORS)
    if (!obj) {
        let remote = await fetchObjectRemote(id);
        if (!remote) {
            showErrorUI("Impossible de récupérer l’objet (CORS ou ressource non JSON). Pense à configurer un proxy /api/object.");
            return;
        }
        const canon = asRemoteUrl(id) || id;
        const suffix = String(canon).split("/13/").pop(); // cas Datatourisme (ORG 13)

        if (Array.isArray(remote)) {
            obj = remote.find(n => n && typeof n === "object" && (idsEqual(n["@id"], canon) || (n["@id"] && n["@id"].endsWith(suffix)))) || remote[0];
        } else if (remote["@graph"] && Array.isArray(remote["@graph"])) {
            obj = remote["@graph"].find(n => idsEqual(n["@id"], canon) || (n["@id"] && n["@id"].endsWith(suffix))) || remote["@graph"][0];
        } else {
            obj = remote;
        }
    }

    try {
        const norm = normalizeObject(obj || {});
        console.debug('normalized object:', norm);

        setText(".title", norm.name || "Détail");
        setText(".name", norm.name || "Activité");
        renderType(norm.types && norm.types[0]);

        // ⚠️ Upgrade-only : ne pas écraser une image déjà bonne par un placeholder
        setCover(norm.image);

        renderDescription(norm.descriptions);
        renderContacts(norm.contacts);

        initNotes();
        initPhotos();
    } catch (e) {
        console.error(e);
        showErrorUI("Erreur lors de l’affichage de l’activité.");
    }
})();