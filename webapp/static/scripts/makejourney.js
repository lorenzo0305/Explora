/* =========================================================
   GenererVoyage.js — multi-journées (responsive)
   - Branché sur WishBasket (PanierFavoris.js)
   - Drag & Drop par journée
   - Sauvegarde → routes /journeys et page /creation
   ========================================================= */

function createActivityElement(act, draggable = true, withRemoveBtn = false){
  const row = document.createElement("div");
  row.className = "activity";
  row.dataset.id = act.id;

  const img = document.createElement("img");
  img.src = act.image || "/static/img/no-image.jpg";
  img.alt = act.name || "Activité";
  row.appendChild(img);

  const span = document.createElement("span");
  span.textContent = act.name || "Sans nom";
  row.appendChild(span);

  if (withRemoveBtn){
    const btn = document.createElement("button");
    btn.className = "remove";
    btn.type = "button";
    btn.textContent = "Retirer";
    btn.addEventListener("click", (e) => {
      e.stopPropagation();
      row.remove(); // Retire juste de la zone (pas du panier global)
    });
    row.appendChild(btn);
  }

  if (draggable){
    row.setAttribute("draggable", "true");
    row.addEventListener("dragstart", e => {
      e.dataTransfer.setData("id", act.id);
      e.dataTransfer.effectAllowed = "move";
    });
  }
  return row;
}

function wireSlotsFor(root){
  const slots = root.querySelectorAll(".slot");
  slots.forEach(slot => {
    slot.addEventListener("dragover", e => { e.preventDefault(); e.dataTransfer.dropEffect = "move"; });
    slot.addEventListener("drop", e => {
      e.preventDefault();
      const id = e.dataTransfer.getData("id");
      // ON UTILISE LE VRAI WISHBASKET POUR TROUVER L'ACTIVITÉ
      const act = window.WishBasket.load().find(a => String(a.id) === String(id));
      if (act){
        slot.appendChild(createActivityElement(act, false, true));
      }
    });
  });
}

function wireExistingSlots(){
  document.querySelectorAll(".day-section").forEach(section => wireSlotsFor(section));
}

function getIdFromURL(){
  const m = location.pathname.match(/\/creation(?:\/([^\/]+))?$/i);
  return m && m[1] && m[1] !== "nouveau" ? m[1] : "";
}

function pickCoverFromDOMOrBasket(){
  const slotImg = document.querySelector(".slot .activity img")?.src;
  if (slotImg) return slotImg;
  const firstBasket = window.WishBasket.load()[0]?.image;
  return firstBasket || "/static/img/no-image.jpg";
}

function nextDayIndex(){
  const existing = document.querySelectorAll(".day-section");
  return existing.length + 1;
}

function createDaySection(day){
  const title = document.createElement("h3");
  title.className = "day-title";
  title.textContent = `Journée ${day}`;

  const section = document.createElement("section");
  section.className = "day-section";
  section.setAttribute("data-day", String(day));

  const grid = document.createElement("div");
  grid.className = "slots";

  const makeSlot = (key, label) => {
    const div = document.createElement("div");
    div.className = "slot";
    div.id = `day${day}-${key}`;
    div.setAttribute("data-key", key);
    const h5 = document.createElement("h5");
    h5.textContent = label;
    div.appendChild(h5);
    return div;
  };
  grid.appendChild(makeSlot("morning", "Matinée"));
  grid.appendChild(makeSlot("noon", "Midi"));
  grid.appendChild(makeSlot("afternoon", "Après-midi"));
  grid.appendChild(makeSlot("evening", "Soirée"));

  section.appendChild(grid);

  const addBtn = document.getElementById("addDayBtn");
  const panel = document.querySelector(".editor-panel");
  if (addBtn && panel){
    panel.insertBefore(title, addBtn);
    panel.insertBefore(section, addBtn);
  } else {
    panel?.appendChild(title);
    panel?.appendChild(section);
  }

  wireSlotsFor(section);
  return section;
}

function clearActivitiesIn(container){
  container.querySelectorAll(".slot").forEach(s => {
    s.querySelectorAll(".activity").forEach(el => el.remove());
  });
}

function normalizeSlots(slots){
  return {
    morning: Array.isArray(slots?.morning) ? slots.morning : [],
    noon: Array.isArray(slots?.noon) ? slots.noon : [],
    afternoon: Array.isArray(slots?.afternoon) ? slots.afternoon : [],
    evening: Array.isArray(slots?.evening) ? slots.evening : []
  };
}

function hydrateDayInto(section, daySlots){
  const norm = normalizeSlots(daySlots);
  ["morning","noon","afternoon","evening"].forEach(k => {
    const slot = section.querySelector(`.slot[data-key="${k}"]`);
    if (!slot) return;
    norm[k].forEach(a => slot.appendChild(createActivityElement(a, false, true)));
  });
}

function ensureDayOneWrapper(){
  const hasSection = !!document.querySelector('.day-section[data-day="1"]');
  const slotsGrid = document.querySelector(".slots");
  if (hasSection || !slotsGrid) return;

  const day = 1;
  const title = document.createElement("h3");
  title.className = "day-title";
  title.textContent = `Journée ${day}`;

  const section = document.createElement("section");
  section.className = "day-section";
  section.setAttribute("data-day", String(day));

  slotsGrid.parentNode.insertBefore(title, slotsGrid);
  slotsGrid.parentNode.insertBefore(section, slotsGrid);
  section.appendChild(slotsGrid);

  const keys = ["morning","noon","afternoon","evening"];
  const labels = ["Matinée","Midi","Après-midi","Soirée"];
  section.querySelectorAll(".slot").forEach((slot, i) => {
    if (!slot.getAttribute("data-key")) slot.setAttribute("data-key", keys[i] || `k${i}`);
    if (!slot.id) slot.id = `day${day}-${keys[i] || `k${i}`}`;
    const h = slot.querySelector("h5");
    if (h && !h.textContent.trim()) h.textContent = labels[i] || "Slot";
  });

  wireSlotsFor(section);
}

async function preloadIfEditing(){
  const form = document.getElementById("createJourneyForm");
  if (!form) return;

  const dataId = form.getAttribute("data-id") || "";
  const urlId  = getIdFromURL();
  const id = dataId || urlId;
  if (!id) return;

  try{
    const res = await fetch(`/journeys/${id}`);
    if (!res.ok) throw new Error("not ok");
    const j = await res.json();
    hydrateEditorFromJourney(j);
    return;
  }catch(e){
    console.warn("Préchargement serveur indisponible, fallback localStorage.", e);
  }

  const list = lsGetJourneys();
  const j = list.find(x => String(x.id) === String(id));
  if (j) hydrateEditorFromJourney(j);
}

function hydrateEditorFromJourney(j){
  const nameEl = document.getElementById("journeyName");
  if (nameEl) nameEl.value = j.name || "";
  const locEl = document.getElementById("journeyLocation");
  if (locEl) locEl.value = j.location || "";

  // Assigner le panier du voyage dans le global
  localStorage.setItem(window.WishBasket.BASKET_KEY, JSON.stringify(j.basket || []));
  window.WishBasket.refresh();

  const days = parseMaybeJSON(j.days) ?? j.days;
  if (Array.isArray(days) && days.length){
    const day1 = document.querySelector('.day-section[data-day="1"]');
    if (day1){ clearActivitiesIn(day1); hydrateDayInto(day1, days[0]?.slots || {}); }
    for (let i = 1; i < days.length; i++){
      const d = i + 1;
      const sec = createDaySection(d);
      hydrateDayInto(sec, days[i]?.slots || {});
    }
  } else {
    const day1 = document.querySelector('.day-section[data-day="1"]');
    if (day1){ clearActivitiesIn(day1); hydrateDayInto(day1, j.slots || {}); }
  }
}

const LS_JOURNEYS = "journeys";
function lsGetJourneys(){ try{ return JSON.parse(localStorage.getItem(LS_JOURNEYS) || "[]"); }catch{ return []; } }
function lsSetJourneys(arr){ localStorage.setItem(LS_JOURNEYS, JSON.stringify(arr)); }
function parseMaybeJSON(val){ if (typeof val === "string"){ try{ return JSON.parse(val); }catch{ return null; } } return val; }

async function saveJourney(e){
  if (e && typeof e.preventDefault === "function") e.preventDefault();

  const form = document.getElementById("createJourneyForm");
  const idAttr = form ? (form.getAttribute("data-id") || "") : "";
  const urlId  = getIdFromURL();
  const id = idAttr || (urlId && urlId !== "nouveau" ? urlId : "");

  const name = (document.getElementById("journeyName")?.value || "").trim();
  const location = (document.getElementById("journeyLocation")?.value || "").trim();
  if (!name) { alert("Veuillez saisir un nom de voyage"); return; }

  const days = [];
  document.querySelectorAll(".day-section").forEach(section => {
    const dayIndex = parseInt(section.getAttribute("data-day"), 10) || (days.length + 1);
    const daySlots = { morning:[], noon:[], afternoon:[], evening:[] };

    section.querySelectorAll(".slot").forEach(slot => {
      const key = slot.getAttribute("data-key");
      if (!key) return;
      slot.querySelectorAll(".activity").forEach(actEl => {
        daySlots[key].push({
          id: actEl.dataset.id,
          name: actEl.querySelector("span")?.textContent || "Sans nom",
          image: actEl.querySelector("img")?.src || "/static/img/no-image.jpg"
        });
      });
    });

    days.push({ day: dayIndex, slots: daySlots });
  });

  const slotsCompat = days[0]?.slots || { morning:[], noon:[], afternoon:[], evening:[] };

  const payload = {
    id: id || undefined,
    name,
    location,
    image: pickCoverFromDOMOrBasket(),
    basket: window.WishBasket.load(),
    days,
    days_json: JSON.stringify(days),
    slots: slotsCompat,
    updatedAt: Date.now(),
    source: 'ai'
  };

  const headers = { "Content-Type": "application/json" };
  const okServerResp = (data) => data && (data.status === "ok" || data.status === "updated" || data.id);

  try{
    let data;
    if (id){
      try{
        const r1 = await fetch(`/journeys/${id}`, { method:"PUT", headers, body: JSON.stringify(payload) });
        if (r1.ok) data = await r1.json();
      }catch{}
      if (!okServerResp(data)){
        const r2 = await fetch(`/journeys/save`, { method:"POST", headers, body: JSON.stringify({ ...payload, id }) });
        if (r2.ok) data = await r2.json();
      }
    }else{
      try{
        const r1 = await fetch(`/journeys/save`, { method:"POST", headers, body: JSON.stringify(payload) });
        if (r1.ok) data = await r1.json();
      }catch{}
      if (!okServerResp(data)){
        const r2 = await fetch(`/journeys`, { method:"POST", headers, body: JSON.stringify(payload) });
        if (r2.ok) data = await r2.json();
      }
    }

    if (okServerResp(data)){
      alert("Voyage sauvegardé !");
      window.location.href = "/creation";
      return;
    }
    throw new Error("save endpoints failed");

  }catch(err){
    console.warn("Serveur indisponible, fallback LocalStorage.", err);
  }

  const list = lsGetJourneys();
  if (id){
    const i = list.findIndex(x => String(x.id) === String(id));
    if (i >= 0) list[i] = { ...list[i], ...payload, id };
    else list.push({ ...payload, id });
  }else{
    const newId = "j_" + Date.now().toString(36);
    list.push({ ...payload, id: newId });
  }
  lsSetJourneys(list);
  alert("Voyage sauvegardé (local) !");
  window.location.href = "/creation";
}

document.addEventListener("DOMContentLoaded", () => {
  ensureDayOneWrapper();
  wireExistingSlots();

  const addDayBtn = document.getElementById("addDayBtn");
  if (addDayBtn){
    addDayBtn.addEventListener("click", () => {
      const day = nextDayIndex();
      const sec = createDaySection(day);
      sec.scrollIntoView({ behavior:"smooth", block:"start" });
    });
  }

  preloadIfEditing();

  const saveBtn = document.getElementById("saveJourneyBtn");
  if (saveBtn) saveBtn.addEventListener("click", saveJourney);

  document.addEventListener("keydown", (e) => {
    const isSave = (e.ctrlKey || e.metaKey) && e.key.toLowerCase() === "s";
    if (isSave){ e.preventDefault(); saveJourney(); }
  });

  const form = document.getElementById("createJourneyForm");
  if (form) form.addEventListener("submit", saveJourney);
});