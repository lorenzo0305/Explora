// --- /static/scripts/MesVoyages.js ---

const JOURNEYS_KEY = "wish_journeys_v1";

const FALLBACK_IMAGES = [
    '/static/img/baie_de_somme2.jpg',
    '/static/img/auvergne.jpg',
    '/static/img/montagne_france2.jpg',
    '/static/img/boeuf_bourguignon.jpg',
    '/static/img/semur_en_auxois.jpg',
    '/static/img/vtt.jpg'
];

function stringToHash(str) {
    let hash = 0;
    for (let i = 0; i < str.length; i++) { hash = str.charCodeAt(i) + ((hash << 5) - hash); }
    return Math.abs(hash);
}

function getShuffledFallbacks(journeyId) {
    let seed = stringToHash(String(journeyId || "default"));
    let arr = [...FALLBACK_IMAGES];
    for (let i = arr.length - 1; i > 0; i--) {
        seed = (seed * 9301 + 49297) % 233280;
        let rand = seed / 233280;
        let j = Math.floor(rand * (i + 1));
        [arr[i], arr[j]] = [arr[j], arr[i]];
    }
    return arr;
}

function showCustomConfirm(title, message, onConfirm) {
    let modal = document.getElementById('customConfirmModal');
    if (!modal) {
        modal = document.createElement('div');
        modal.id = 'customConfirmModal';
        modal.style.cssText = 'position:fixed;inset:0;background:rgba(28,28,28,0.6);display:none;align-items:center;justify-content:center;z-index:9999;padding:20px;';
        modal.innerHTML = `
            <div style="background:#FAF8F5;max-width:400px;width:100%;border-radius:16px;padding:32px;box-shadow:0 20px 60px rgba(0,0,0,0.3);text-align:center;position:relative;">
                <button id="ccClose" style="position:absolute;top:16px;right:16px;background:none;border:none;font-size:24px;cursor:pointer;color:#6b5f57;">×</button>
                <div style="width:50px;height:50px;border-radius:50%;background:rgba(255,111,97,0.1);color:#FF6F61;display:flex;align-items:center;justify-content:center;margin:0 auto 16px;">
                    <svg viewBox="0 0 24 24" width="24" height="24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="3 6 5 6 21 6"></polyline><path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6m3 0V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2"></path></svg>
                </div>
                <h3 id="ccTitle" style="font-family:'Cormorant Garamond',serif;font-size:24px;margin:0 0 8px;color:#1C1C1C;">Titre</h3>
                <p id="ccMessage" style="margin:0 0 24px;color:#6b5f57;font-size:15px;line-height:1.5;font-family:'Lora',serif;">Message</p>
                <div style="display:flex;gap:12px;justify-content:center;">
                    <button id="ccCancel" type="button" style="background:none;border:1px solid #D4C3B3;color:#6b5f57;padding:10px 24px;border-radius:8px;cursor:pointer;font-weight:700;font-family:'Montserrat',sans-serif;font-size:11px;text-transform:uppercase;letter-spacing:0.5px;transition:background 0.2s;">Annuler</button>
                    <button id="ccConfirm" type="button" style="background:#FF6F61;border:none;color:#fff;padding:10px 24px;border-radius:8px;cursor:pointer;font-weight:700;font-family:'Montserrat',sans-serif;font-size:11px;text-transform:uppercase;letter-spacing:0.5px;transition:background 0.2s;">Supprimer</button>
                </div>
            </div>
        `;
        document.body.appendChild(modal);

        modal.querySelector('#ccCancel').addEventListener('mouseover', function() { this.style.background = '#F0EDE9'; });
        modal.querySelector('#ccCancel').addEventListener('mouseout', function() { this.style.background = 'none'; });
        modal.querySelector('#ccConfirm').addEventListener('mouseover', function() { this.style.background = '#E85A4D'; });
        modal.querySelector('#ccConfirm').addEventListener('mouseout', function() { this.style.background = '#FF6F61'; });
    }

    modal.querySelector('#ccTitle').textContent = title;
    modal.querySelector('#ccMessage').textContent = message;
    modal.style.display = 'flex';

    const closeIt = () => { modal.style.display = 'none'; cleanup(); };
    const confirmIt = () => { closeIt(); if(onConfirm) onConfirm(); };

    const btnClose = modal.querySelector('#ccClose');
    const btnCancel = modal.querySelector('#ccCancel');
    const btnConfirm = modal.querySelector('#ccConfirm');

    const cleanup = () => {
        btnClose.removeEventListener('click', closeIt);
        btnCancel.removeEventListener('click', closeIt);
        btnConfirm.removeEventListener('click', confirmIt);
    };

    btnClose.addEventListener('click', closeIt);
    btnCancel.addEventListener('click', closeIt);
    btnConfirm.addEventListener('click', confirmIt);
}

const lsGetJourneys = () => { try { return JSON.parse(localStorage.getItem(JOURNEYS_KEY) || "[]"); } catch { return []; } };
const lsSetJourneys = (arr) => { try { localStorage.setItem(JOURNEYS_KEY, JSON.stringify(arr)); } catch { } };

function activitiesCount(j) {
    if (!Array.isArray(j?.plan)) return 0;
    return j.plan.reduce((acc, d) => {
        if (Array.isArray(d?.matin) || Array.isArray(d?.aprem)) {
            return acc + (Array.isArray(d.matin) ? d.matin.length : 0) + (Array.isArray(d.aprem) ? d.aprem.length : 0);
        }
        const s = d?.slots || {};
        const c = (arr) => Array.isArray(arr) ? arr.length : (arr ? Object.values(arr).length : 0);
        return acc + c(s.morning) + c(s.noon) + c(s.afternoon) + c(s.evening);
    }, 0);
}

function metaText(j) {
    const d = Array.isArray(j?.plan) ? j.plan.length : 1;
    const a = activitiesCount(j);
    return (j.location ? j.location + " • " : "") + d + (d > 1 ? " jours" : " jour") + " • " + a + (a > 1 ? " activités" : " activité");
}

function pickCover(j) {
    // 1. Si le voyage a une VRAIE cover spécifiquement assignée (ex: sauvegardé via l'Éditeur manuel)
    if (j?.cover && !j.cover.includes('no-image') && !j.cover.includes('appareil_photo') && !j.cover.includes('no-img')) {
        return j.cover;
    }
    
    // 2. SINON : on n'utilise PAS les activités, on prend nos belles photos de couverture de voyage !
    return getShuffledFallbacks(j?.id)[0];
}

function renderJourneys(journeys) {
    const listEl = document.getElementById("topicsList");
    const emptyEl = document.getElementById("emptyState");
    if (!listEl) return;
    listEl.innerHTML = "";

    if (!journeys.length) { emptyEl.style.display = "block"; return; }
    emptyEl.style.display = "none";

    const esc = (s) => String(s ?? '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');

    journeys.forEach((j) => {
        const item = document.createElement("div"); 
        item.className = "item";
        item.style.cursor = "pointer";
        
        const isEditor = j.source === 'editor' || j.source === 'Editor';
        const sourceText = isEditor ? "Voyage conçu manuellement" : "Voyage généré par IA";
        
        let cleanName = String(j.name || "Voyage").replace(/\s*&lt;br\s*\/?&gt;\s*/gi, ' - ').replace(/\s*<br\s*\/?>\s*/gi, ' - ');
        const formattedName = esc(cleanName);

        item.innerHTML = `
          <div class="left">
            <div class="thumb" style="background-image:url('${esc(pickCover(j))}')"></div>
            <div class="meta">
              <div class="name">${formattedName}</div>
              <div class="desc">${esc(metaText(j))}</div>
            </div>
          </div>
          <div class="actions-right">
            <button class="icon-btn delete-btn" title="Supprimer">
              <svg viewBox="0 0 24 24"><path d="M3 6h18"/><path d="M8 6v-2a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2"/><path d="M19 6l-1 14a2 2 0 0 1-2 2H8a2 2 0 0 1-2-2L5 6"/><path d="M10 11v6"/><path d="M14 11v6"/></svg>
            </button>
          </div>
          <div style="position: absolute; bottom: 12px; left: 0; width: 100%; text-align: center; pointer-events: none;">
            <span class="source-label" style="font-family: 'Montserrat', sans-serif; font-size: 7.5px; font-weight: 700; text-transform: uppercase; letter-spacing: 0.3px; color: #1C1C1C; opacity: 0.5;">
                ${sourceText}
            </span>
          </div>`;

        item.querySelector('.delete-btn').addEventListener('click', async (e) => {
            e.stopPropagation();
            showCustomConfirm(
                "Supprimer ce voyage ?", 
                `Êtes-vous sûr de vouloir supprimer définitivement « ${cleanName} » ?`, 
                () => {
                    lsSetJourneys(lsGetJourneys().filter(x => String(x.id) !== String(j.id)));
                    loadJourneys();
                }
            );
        });

        item.addEventListener("click", () => window.location.href = "/journeys/view/" + encodeURIComponent(j.id));
        listEl.appendChild(item);
    });
}

async function loadJourneys() {
    const cached = lsGetJourneys();
    renderJourneys(cached.sort((a, b) => new Date(b?.updatedAt || 0) - new Date(a?.updatedAt || 0)));
}

document.addEventListener("DOMContentLoaded", () => {
    loadJourneys();
});