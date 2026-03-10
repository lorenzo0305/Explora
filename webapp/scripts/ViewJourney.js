const DEFAULT_COVER = "/static/img/no-image.jpg";
const JOURNEY_KEYS = ["wish_journeys_v1", "journeys"];
const LAST_ID_KEY = "wish_last_journey_id";

function loadAllLocalJourneys() {
    for (const k of JOURNEY_KEYS) { try { const a = JSON.parse(localStorage.getItem(k) || "[]"); if (a && a.length) return a; } catch { } }
    try { return JSON.parse(localStorage.getItem(JOURNEY_KEYS[0]) || "[]"); } catch { return []; }
}
function byLocalId(id) { return loadAllLocalJourneys().find(j => String(j.id) === String(id)) || null }
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

const A = (a) => Array.isArray(a) ? a : (a && typeof a === "object") ? Object.values(a) : [];
function slotsArrayToObj(slotsArr) {
    const obj = { morning: [], noon: [], afternoon: [], evening: [] };
    (Array.isArray(slotsArr) ? slotsArr : []).forEach(s => { const k = String(s?.key || "").toLowerCase(); if (obj[k]) obj[k] = A(s.items); }); return obj;
}
function normalizeSlots(s) { s = s || {}; return { morning: A(s.morning), noon: A(s.noon), afternoon: A(s.afternoon), evening: A(s.evening) }; }
function normalizeDayAny(d, i) {
    if (!d || typeof d !== "object") return { day: (i || 0) + 1, slots: { morning: [], noon: [], afternoon: [], evening: [] } };
    const day = (d.day != null) ? Number(d.day) : (i || 0) + 1; if (Array.isArray(d.slots)) return { day, slots: slotsArrayToObj(d.slots) };
    return { day, slots: normalizeSlots(d.slots || { morning: d.morning, noon: d.noon, afternoon: d.afternoon, evening: d.evening }) };
}
function deriveDays(j) {
    if (Array.isArray(j?.plan)) return j.plan.map((d, i) => normalizeDayAny(d, i));
    let days = j?.days ?? j?.days_json; if (typeof days === "string") { try { days = JSON.parse(days); } catch { days = null; } }
    if (Array.isArray(days)) return days.map((d, i) => normalizeDayAny(d, i));
    const s = normalizeSlots(j?.slots || {}); const total = s.morning.length + s.noon.length + s.afternoon.length + s.evening.length;
    return total ? [{ day: 1, slots: s }] : [];
}

const pickFirstStr = (...vals) => vals.find(v => typeof v === "string" && v.trim()) || "";
const safeImg = (u) => u && (/^data:image\//i.test(u) || /^https?:\/\//i.test(u) || u.startsWith("/")) ? u : "";
function activityImage(it) {
    const u = pickFirstStr(it?.image, it?.photo, it?.picture, it?.thumbnail, it?.cover);
    return safeImg(u);
}
function pickRandom(arr) { return (!arr || !arr.length) ? null : arr[Math.floor(Math.random() * arr.length)]; }
function randomDayImage(d) {
    const order = ['morning', 'noon', 'afternoon', 'evening']; const pool = [];
    for (const k of order) { for (const it of (d.slots?.[k] || [])) { const u = activityImage(it); if (u) pool.push(u); } }
    return pool.length ? pickRandom(pool) : DEFAULT_COVER;
}
function coverFrom(j) {
    if (j?.cover) return safeImg(j.cover) || DEFAULT_COVER;
    const days = deriveDays(j || {});
    for (const d of days) {
        for (const k of ['morning', 'noon', 'afternoon', 'evening']) {
            const u = activityImage((d.slots?.[k] || [])[0]); if (u) return u;
        }
    }
    return DEFAULT_COVER;
}
function countActivities(j) {
    const days = deriveDays(j || {}); let n = 0;
    for (const d of days) { n += (d.slots?.morning?.length || 0) + (d.slots?.noon?.length || 0) + (d.slots?.afternoon?.length || 0) + (d.slots?.evening?.length || 0); }
    return n;
}

async function fetchServerJourney(id) { try { const r = await fetch('/journeys/' + encodeURIComponent(id)); if (r.ok) return await r.json(); } catch { } return null }
function mergeJourneys(a = {}, b = {}) {
    const newer = (new Date(a?.updatedAt || 0) >= new Date(b?.updatedAt || 0)) ? a : b; const older = (newer === a) ? b : a;
    const m = { ...older, ...newer }; const nd = deriveDays(newer), od = deriveDays(older);
    if (!nd.length && od.length) { if (older.plan) m.plan = older.plan; if (older.days) m.days = older.days; if (older.days_json) m.days_json = older.days_json; if (!m.slots && older.slots) m.slots = older.slots; }
    return m;
}

function setImgWithFallback(imgEl, url) {
    const finalUrl = url || DEFAULT_COVER;
    imgEl.onerror = function () { if (imgEl.dataset.fallback !== "1") { imgEl.dataset.fallback = "1"; imgEl.src = DEFAULT_COVER; } };
    imgEl.src = finalUrl;
}

function render(j) {
    localStorage.setItem(LAST_ID_KEY, String(j.id)); // mémorise l’ID ouvert

    document.getElementById('journeyTitle').textContent = j?.name || 'Voyage';
    document.getElementById('journeyLocation').textContent = j?.location || 'Ville, lieux...';
    setImgWithFallback(document.getElementById('cover'), coverFrom(j));

    const days = deriveDays(j || {}); const aCount = countActivities(j || {});
    document.getElementById('summaryLine').innerHTML =
        `<span><b id="daysLabel">${days.length}</b> jours pour <b>${j?.persons ?? '…'}</b> pers.</span>
         <span>• <b>${j?.price ?? '…'}€</b> • <b>${aCount}</b> activité${aCount > 1 ? 's' : ''}</span>`;

    const list = document.getElementById('daysList'); list.innerHTML = '';
    for (let idx = 0; idx < days.length; idx++) { // ← let pour bon index
        const d = days[idx];
        const item = document.createElement('div'); item.className = 'day-item';
        const left = document.createElement('div'); left.className = 'day-left';
        const thumb = document.createElement('div'); thumb.className = 'day-thumb';
        thumb.style.backgroundImage = `url('${randomDayImage(d)}')`;

        const meta = document.createElement('div'); meta.className = 'day-meta';
        const name = document.createElement('div'); name.className = 'day-name'; name.textContent = 'Journée ' + d.day;
        const labels = [['morning', 'Matinée'], ['noon', 'Midi'], ['afternoon', 'Après-midi'], ['evening', 'Soirée']];
        const present = []; let count = 0;
        for (const [k, fr] of labels) { const len = (d.slots?.[k]?.length || 0); if (len) { present.push(fr); count += len; } }
        const desc = document.createElement('div'); desc.className = 'day-desc';
        desc.textContent = count ? `${count} activités ~ ${present.join(' · ')}` : 'type des activités ~ distance ~ prix';

        meta.appendChild(name); meta.appendChild(desc);
        left.appendChild(thumb); left.appendChild(meta);
        item.appendChild(left);

        item.addEventListener('click', () => {
            const id = getJourneyId();
            window.location.href = `/journeys/view/${encodeURIComponent(id)}/day/${idx + 1}`;
        });

        list.appendChild(item);
    }

    const notesKey = 'journey_notes_' + j.id;
    const ta = document.getElementById('privateNotes');
    ta.value = localStorage.getItem(notesKey) || '';
    ta.addEventListener('input', () => localStorage.setItem(notesKey, ta.value));
}

document.getElementById('backBtn').addEventListener('click', () => history.back());

(async function init() {
    const id = getJourneyId();
    if (!id) { document.getElementById('journeyTitle').textContent = 'Voyage introuvable'; return; }
    const local = byLocalId(id); const server = await fetchServerJourney(id);
    const journey = (local && server) ? mergeJourneys(local, server) : (server || local);
    if (!journey) { document.getElementById('journeyTitle').textContent = 'Voyage introuvable'; return; }
    render(journey);
})();