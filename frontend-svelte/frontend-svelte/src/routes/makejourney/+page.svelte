<svelte:head>
  <title>WishAdventure — Création</title>
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
  <script src="/static/scripts/Creation.js" defer></script>
</svelte:head>

<div class="wrap">
  <div class="topbar">
    <h1 class="title">Création</h1>
    <button id="newJourneyBtn" class="new-btn" type="button">＋ Nouveau</button>
  </div>

  <div id="journeyList" class="grid"></div>
  <p id="emptyState" class="empty" style="display:none;">Aucun voyage pour l’instant.</p>
</div>

<nav class="bottom-nav">
  <a href="/" class="bouton">
    <button class="boutonNav">
      <svg class="nav-icon" viewBox="0 0 24 24"><path d="M12 3l9 8h-3v9h-5v-6H11v6H6v-9H3l9-8z"/></svg>
      Accueil
    </button>
  </a>
  <a href="/Destinations" class="bouton">
    <button class="boutonNav">
      <svg class="nav-icon" viewBox="0 0 24 24"><circle cx="11" cy="11" r="6"></circle><path d="M20 20l-3.5-3.5" stroke="white" stroke-width="2" stroke-linecap="round" fill="none"/></svg>
      Exploration
    </button>
  </a>
  <a href="/topics" class="bouton">
    <button class="boutonNav">
      <svg class="nav-icon" viewBox="0 0 24 24"><path d="M5 4h11a2 2 0 0 1 2 2v12H7a2 2 0 0 1-2-2V4z"/><rect x="3" y="6" width="2" height="12" rx="1" ry="1"/><path d="M8 8h7M8 11h7M8 14h5" stroke="white" stroke-width="2" fill="none" stroke-linecap="round"/></svg>
      Carnet
    </button>
  </a>
  <a href="/makejourney" class="bouton">
    <button class="boutonNav active">
      <svg class="nav-icon" viewBox="0 0 24 24">
        <path d="M12 2a6 6 0 0 0-6 6c0 4.5 6 12 6 12s6-7.5 6-12a6 6 0 0 0-6-6zm0 8.5A2.5 2.5 0 1 1 12 5.5a2.5 2.5 0 0 1 0 5z"/>
        <path d="M15.2 14.2l3.2 3.2-2.3 0.5-1.4 1.4-0.5-2.3 1-1z"/>
      </svg>
      Création
    </button>
  </a>
</nav>

<style>
  :global(body){ background:#121212; color:#fff; }
  .wrap{ max-width:1100px; margin:24px auto; padding:0 16px; }
  .topbar{ display:flex; gap:12px; align-items:center; justify-content:space-between; margin-bottom:16px; }
  .title{ font-size:clamp(22px,3vw,32px); font-weight:700; margin:0; }
  .new-btn{ border:none; background:#FF6000; color:#190028; font-weight:800; padding:10px 14px; border-radius:10px; }
  .empty{ opacity:.8; padding:16px 0; }

  .grid{ display:grid; grid-template-columns: repeat(2, minmax(0,1fr)); gap:16px; }
  @media (min-width: 768px){ .grid{ grid-template-columns: repeat(3, minmax(0,1fr)); } }
  @media (min-width: 1200px){ .grid{ grid-template-columns: repeat(4, minmax(0,1fr)); } }

  /* On protège les éléments générés par le JS avec :global() */
  :global(.journey-card) {
    position:relative;
    background:#1e1e1e; border-radius:12px; overflow:hidden; cursor:pointer;
    transition: transform .08s ease, box-shadow .2s ease;
    display:flex; flex-direction:column;
  }
  :global(.journey-card:hover) { transform: translateY(-2px); box-shadow:0 10px 24px rgba(0,0,0,.35); }
  :global(.journey-card img) { width:100%; height:160px; object-fit:cover; display:block; }
  :global(.journey-card .info) { padding:10px 12px; display:flex; flex-direction:column; gap:4px; }
  :global(.journey-card .name) { font-weight:700; font-size:15px; color:#fff; }
  :global(.journey-card .meta) { font-size:12px; color:#bbb; }

  :global(.trash-btn) {
    position:absolute; top:8px; right:8px;
    width:28px; height:28px; border:none; border-radius:6px;
    background:rgba(0,0,0,.45);
    display:flex; align-items:center; justify-content:center;
    cursor:pointer; z-index:2;
  }
  :global(.trash-btn:hover) { background:rgba(0,0,0,.6); transform:translateY(-1px); }
  :global(.trash-btn svg) { width:16px; height:16px; fill:#ff4d4d; }

  nav.bottom-nav{ position:sticky; bottom:0; left:0; right:0; }
  .nav-icon{width:18px;height:18px;vertical-align:-3px;margin-right:6px;fill:#fff}
</style>