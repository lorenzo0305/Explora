/* ====== Flags (mets à true si ton backend expose ces routes) ====== */
const ENABLE_SERVER_FETCH = false; // /journeys/:id
const ENABLE_ENRICH_NETWORK = false; // /objects/:id ou ID HTTP direct (CORS requis)

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

/* ====== Normalisation jours/slots ====== */
function slotsArrayToObj(slotsArr) {
    const obj = { morning: [], noon: [], afternoon: [], evening: [] };
    (Array.isArray(slotsArr) ? slotsArr : []).forEach(s => { const k = String(s?.key || "").toLowerCase(); if (obj[k]) obj[k] = A(s.items); }); return obj;
}
function normalizeSlots(s) { s = s || {}; return { morning: A(s.morning), noon: A(s.noon), afternoon: A(s.afternoon), evening: A(s.evening) }; }
function normalizeDayAny(d, i) {
    if (!d || typeof d !== "object") return { day: (i || 0) + 1, slots: { morning: [], noon: [], afternoon: [], evening: [] } };
    const day = (d.day != null) ? Number(d.day) : (i || 0) + 1;
    if (Array.isArray(d.slots)) return { day, slots: slotsArrayToObj(d.slots) };
    return { day, slots: normalizeSlots(d.slots || { morning: d.morning, noon: d.noon, afternoon: d.afternoon, evening: d.evening }) };
}
function deriveDays(j) {
    if (Array.isArray(j?.plan)) return j.plan.map((d, i) => normalizeDayAny(d, i));
    let days = j?.days ?? j?.days_json;
    if (typeof days === "string") { try { days = JSON.parse(days); } catch { days = null; } }
    if (Array.isArray(days)) return days.map((d, i) => normalizeDayAny(d, i));
    const s = normalizeSlots(j?.slots || {}); const total = s.morning.length + s.noon.length + s.afternoon.length + s.evening.length;
    return total ? [{ day: 1, slots: s }] : [];
}

/* ====== Image utils ====== */
function resolveImageUrl(u) {
    if (!u || badVal(u)) return "";
    u = String(u).trim();
    if (u.startsWith("data:image/")) return u;
    if (u.startsWith("//")) return "https:" + u;
    if (isHttp(u) || isPath(u)) return u;
    if (/^(?:data\.datatourisme\.fr|images?\.)/i.test(u)) return "https://" + u;
    if (isFileLike(u)) return "/static/img/phototheque/" + u.replace(/^\/+/, "");
    return "";
}
function pickLang(val) {
    if (typeof val === "string") return val;
    if (Array.isArray(val)) {
        const fr = val.find(v => v && (v["@language"] === "fr" || v.lang === "fr")); if (fr) return fr["@value"] || fr.value || "";
        const en = val.find(v => v && (v["@language"] === "en" || v.lang === "en")); if (en) return en["@value"] || en.value || "";
        const s = val.find(v => typeof v === "string"); if (s) return s;
        const f = val[0]; return (f && (f["@value"] || f.value)) || "";
    }
    if (val && typeof val === "object") {
        if (val.fr) return Array.isArray(val.fr) ? val.fr[0] : val.fr;
        if (val.en) return Array.isArray(val.en) ? val.en[0] : val.en;
        if (val["@value"]) return val["@value"];
    }
    return "";
}
function arrayOfStrings(x) {
    if (!x) return [];
    if (typeof x === "string") return [x];
    if (Array.isArray(x)) return x.map(e => (typeof e === "string" ? e : (e && (e["@value"] || e.value) || ""))).filter(Boolean);
    if (typeof x === "object") { const v = pickLang(x); return v ? [v] : []; }
    return [];
}
function firstString() {
    for (let i = 0; i < arguments.length; i++) {
        const c = arguments[i]; if (!c) continue;
        if (typeof c === "string" && c.trim()) return c.trim();
        if (Array.isArray(c)) {
            const s = c.find(x => typeof x === "string" && x.trim()); if (s) return s.trim();
            const t = c.find(x => x && (x["@value"] || x.value)); if (t) return (t["@value"] || t.value || "").trim();
        }
        if (typeof c === "object") { const p = pickLang(c); if (p && typeof p === "string" && p.trim()) return p.trim(); }
    }
    return "";
}
function extractImage(obj) {
    if (!obj || typeof obj !== "object") return "";
    let direct = resolveImageUrl(firstString(obj.image, obj.photo, obj.picture, obj.thumbnail, obj.cover, obj["https://schema.org/image"]));
    if (direct) return direct;

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
                for (const rr of rels) {
                    if (!rr || typeof rr !== "object") continue;
                    const locs = [].concat(rr["ebucore:locator"] || [], rr.locator || []);
                    for (const loc of locs) {
                        if (typeof loc === "string") { const u = resolveImageUrl(loc); if (u) return u; }
                        else if (loc && typeof loc === "object") {
                            const u = resolveImageUrl(firstString(loc.url, loc["@id"], loc["@value"])); if (u) return u;
                        }
                    }
                }
            }
        }
    }
    for (const v of Object.values(obj)) {
        if (Array.isArray(v)) {
            for (const it of v) {
                if (it && typeof it === "object") {
                    const u = resolveImageUrl(firstString(
                        it.contentUrl, it.url, it["@id"], it["https://schema.org/contentUrl"], it["https://schema.org/url"]
                    ));
                    if (u) return u;
                }
            }
        }
    }
    for (const [k, val] of Object.entries(obj)) {
        const kl = String(k || "").toLowerCase();
        if (!["image", "photo", "thumbnail", "picture", "illustration"].some(n => kl.includes(n))) continue;
        for (const u of arrayOfStrings(val)) { const ru = resolveImageUrl(u); if (ru) return ru; }
    }
    return "";
}

/* ====== Index images local ====== */
const objCache = (() => {
    try { return JSON.parse(localStorage.getItem(OBJ_CACHE_KEY) || "{}"); } catch { return {}; }
})();
function saveObjCache() { try { localStorage.setItem(OBJ_CACHE_KEY, JSON.stringify(objCache)); } catch { } }

function activityId(a) { return a?.objectId || a?.id || a?._id || a?.["@id"] || a?.uri || a?.url || null; }

const ACT_IMG_INDEX = new Map();
function indexLocalActivityImages() {
    ACT_IMG_INDEX.clear();
    try {
        const journeys = loadAllLocalJourneys();
        journeys.forEach(j => {
            deriveDays(j).forEach(d => {
                ["morning", "noon", "afternoon", "evening"].forEach(slotK => {
                    (d.slots?.[slotK] || []).forEach(it => {
                        const aid = String(activityId(it) || "");
                        const u = resolveImageUrl(
                            pickFirstStr(it.image, it.photo, it.picture, it.thumbnail, it.cover) || extractImage(it)
                        );
                        if (aid && u && !badVal(u) && !ACT_IMG_INDEX.has(aid)) ACT_IMG_INDEX.set(aid, u);
                    });
                });
            });
        });
    } catch { }
    try {
        loadBasket().forEach(it => {
            const aid = String(it?.id || "");
            const u = resolveImageUrl(pickFirstStr(it.image, it.photo) || extractImage(it));
            if (aid && u && !badVal(u) && !ACT_IMG_INDEX.has(aid)) ACT_IMG_INDEX.set(aid, u);
        });
    } catch { }
}

function getItemImageSync(item) {
    if (!item) return "";
    const prim = resolveImageUrl(pickFirstStr(item.image, item.photo, item.picture, item.thumbnail, item.cover));
    if (prim) return prim;
    const fromObj = extractImage(item);
    if (fromObj) return fromObj;
    const id = activityId(item);
    if (id) {
        const cached = objCache[id]?.image && resolveImageUrl(objCache[id].image);
        if (cached) return cached;
        if (ACT_IMG_INDEX.has(String(id))) return ACT_IMG_INDEX.get(String(id));
    }
    return "";
}
function resolveActivityImage(a) { return getItemImageSync(a) || DEFAULT_ACTIVITY_IMG; }

/* ====== Enrichissement réseau (désactivé par défaut) ====== */
async function tryEnrichThumbAsync(th, a) {
    if (!ENABLE_ENRICH_NETWORK || !th || th.dataset.enriched === "1") return;
    const aid = activityId(a);
    if (!aid) return;

    // 1) si l’id est une URL HTTP, tenter direct (CORS nécessaire côté remote)
    const candidateUrls = [];
    if (isHttp(aid)) candidateUrls.push(aid);

    // 2) endpoints du même domaine (activés seulement si tu actives aussi ENABLE_SERVER_FETCH)
    if (!isHttp(aid) && ENABLE_SERVER_FETCH) {
        candidateUrls.push(`/objects/${encodeURIComponent(aid)}`,
            `/api/objects/${encodeURIComponent(aid)}`,
            `/object/${encodeURIComponent(aid)}`,
            `/api/object/${encodeURIComponent(aid)}`);
    }

    for (const url of candidateUrls) {
        try {
            const r = await fetch(url, { cache: "no-store" });
            if (!r.ok) continue;
            const obj = await r.json();
            const img = resolveImageUrl(
                pickFirstStr(obj.image, obj.photo, obj.picture, obj.thumbnail, obj.cover) ||
                extractImage(obj) || obj.image_url || obj.imageUrl
            );
            if (img) {
                th.style.backgroundImage = `url('${img}')`;
                th.dataset.enriched = "1";
                const key = String(aid);
                objCache[key] = { ...(objCache[key] || {}), image: img };
                saveObjCache();
                ACT_IMG_INDEX.set(key, img);
                return;
            }
        } catch { }
    }
}

/* ====== Fusion serveur + local (désactivée par défaut) ====== */
async function fetchServerJourney(id) {
    if (!ENABLE_SERVER_FETCH) return null;
    try { const r = await fetch(`/journeys/${encodeURIComponent(id)}`); if (r.ok) return await r.json(); } catch { }
    return null;
}
function mergeJourneys(a = {}, b = {}) {
    const newer = (new Date(a?.updatedAt || 0) >= new Date(b?.updatedAt || 0)) ? a : b; const older = (newer === a) ? b : a;
    const m = { ...older, ...newer }; const nd = deriveDays(newer), od = deriveDays(older);
    if (!nd.length && od.length) { if (older.plan) m.plan = older.plan; if (older.days) m.days = older.days; if (older.days_json) m.days_json = older.days_json; if (!m.slots && older.slots) m.slots = older.slots; }
    return m;
}

/* ====== Libellés ====== */
const SLOT_LABEL = { morning: "Matinée", noon: "Midi", afternoon: "Après-midi", evening: "Soirée" };
const fmtMeta = (a) => {
    const parts = []; if (a?.type || a?.category) parts.push(a.type || a.category);
    if (a?.place || a?.location) parts.push(a.place || a.location);
    if (a?.price) parts.push(`${a.price}€`);
    return parts.length ? parts.join(" ~ ") : "type ~ lieu ~ prix";
};

/* ====== Rendu ====== */
function render(j, dayIndex) {
    localStorage.setItem(LAST_ID_KEY, String(j.id));

    const days = deriveDays(j);
    const N = Math.max(1, days.length || 1);
    const idx = Math.min(Math.max(1, dayIndex || 1), N);

    document.getElementById("journeyTitle").textContent = j?.name || "Voyage";
    document.getElementById("journeyLocation").textContent = j?.location || "Ville, lieux...";
    document.getElementById("dayTitle").textContent = `Jour ${idx}`;
    document.getElementById("summaryLine").innerHTML = `pour <b>${j?.persons ?? "…"}</b> pers. • <b>${j?.price ?? "…"}€</b>`;

    const d = days[idx - 1] || days[0];
    const wrap = document.getElementById("slotsWrap");
    wrap.innerHTML = "";

    ["morning", "noon", "afternoon", "evening"].forEach(key => {
        const acts = d?.slots?.[key] || []; if (!acts.length) return;
        const sec = document.createElement("div"); sec.className = "sec";
        const ttl = document.createElement("div"); ttl.className = "sec-title"; ttl.textContent = SLOT_LABEL[key] || key;
        sec.appendChild(ttl);

        acts.forEach(a => {
            const row = document.createElement("div"); row.className = "act";
            const th = document.createElement("div"); th.className = "thumb";

            const firstImg = resolveActivityImage(a);
            th.style.backgroundImage = `url('${firstImg}')`;
            if (firstImg === DEFAULT_ACTIVITY_IMG) tryEnrichThumbAsync(th, a);

            const meta = document.createElement("div"); meta.className = "ameta";
            const an = document.createElement("div"); an.className = "aname"; an.textContent = a?.name || "Activité";
            const ad = document.createElement("div"); ad.className = "adesc"; ad.textContent = fmtMeta(a);
            meta.appendChild(an); meta.appendChild(ad);

            const chev = document.createElementNS("http://www.w3.org/2000/svg", "svg");
            chev.setAttribute("viewBox", "0 0 24 24"); chev.setAttribute("class", "chev");
            const path = document.createElementNS("http://www.w3.org/2000/svg", "path");
            path.setAttribute("d", "M9 18l6-6-6-6"); path.setAttribute("stroke", "#F9F0F7");
            path.setAttribute("stroke-width", "2"); path.setAttribute("fill", "none");
            path.setAttribute("stroke-linecap", "round"); path.setAttribute("stroke-linejoin", "round");
            chev.appendChild(path);

            row.appendChild(th); row.appendChild(meta); row.appendChild(chev);

            const tid = activityId(a);
            if (tid) {
                const href = `/detail-act-perso/${encodeURIComponent(tid)}`;
                row.setAttribute("role", "link");
                row.setAttribute("aria-label", `${a?.name || "Activité"} — détails`);
                row.tabIndex = 0;
                row.addEventListener("click", () => { window.location.href = href; });
                row.addEventListener("keydown", (e) => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); row.click(); } });
            } else {
                row.setAttribute("aria-disabled", "true");
            }

            sec.appendChild(row);
        });

        wrap.appendChild(sec);
    });

    const notesKey = `journey_day_notes_${j.id}_${idx}`;
    const ta = document.getElementById("privateNotes");
    ta.value = localStorage.getItem(notesKey) || "";
    ta.addEventListener("input", () => localStorage.setItem(notesKey, ta.value));
}

/* ====== Init ====== */
document.getElementById("backBtn").addEventListener("click", () => history.back());
(async function init() {
    indexLocalActivityImages();

    const { id, day } = (function () {
        const m = location.pathname.match(/\/journeys\/view\/([^\/\?#]+)\/day\/(\d+)/);
        if (m) return { id: m[1], day: parseInt(m[2], 10) || 1 };
        const sp = new URLSearchParams(location.search);
        return { id: sp.get("id") || getJourneyId(), day: parseInt(sp.get("day") || "1", 10) || 1 };
    })();

    if (!id) { document.getElementById("journeyTitle").textContent = "Voyage introuvable"; return; }

    const local = byLocalId(id);
    const server = await fetchServerJourney(id); // nul si flag off
    const j = (local && server) ? mergeJourneys(local, server) : (server || local);

    if (!j) { document.getElementById("journeyTitle").textContent = "Voyage introuvable"; return; }
    render(j, day);
})();