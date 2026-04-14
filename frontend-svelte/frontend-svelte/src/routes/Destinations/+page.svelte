<svelte:head>
  <title>WishAdventure — Exploration</title>
  <link href="https://fonts.googleapis.com/css2?family=Poppins:wght@300;500;600;700&display=swap" rel="stylesheet">
  <script src="/static/scripts/Exploration.js" defer></script>
</svelte:head>

<div class="app">
  <div class="statusbar">
    <div class="status-row">
      <div class="time" id="clock">--:--</div>
      <div class="status"><div></div><div></div><div></div><div></div></div>
    </div>

    <div class="discover-row">
      <div class="discover-left">
        <div class="logo-dot"></div>
        <div class="discover-title">Découvrir</div>
      </div>
      <div aria-hidden="true" style="width:32px;height:32px;display:flex;align-items:center;justify-content:center;">
        <svg viewBox="0 0 24 24" width="24" height="24" fill="#FF6000">
          <path d="M21 21l-4.35-4.35M10.5 18a7.5 7.5 0 1 1 0-15 7.5 7.5 0 0 1 0 15Z" stroke="#FF6000" stroke-width="2" fill="none" stroke-linecap="round"/>
        </svg>
      </div>
    </div>

    <div class="search-area">
      <div class="search-wrap">
        <div class="search-icon" aria-hidden="true"></div>
        <input id="search" type="text" placeholder="Recherchez une activité..." autocomplete="off" />
      </div>

      <div class="actions">
        <button id="likesIcon" class="like-btn" type="button" title="Mes Favoris">
          ❤️ <span id="likesCount" hidden>0</span>
        </button>
        <div id="floatingLikes" aria-live="polite"></div>

        <button id="basketIcon" class="basket-btn" type="button" title="Panier">
          🧺 <span id="basketCount" hidden>0</span>
        </button>
        <div id="floatingBasket" aria-live="polite"></div>
      </div>

      <div id="results" class="results" role="listbox" aria-label="Résultats de recherche"></div>
    </div>
  </div>

  <div class="section">
    <h2>Choisissez une région</h2>
    <div class="cards-grid" id="regions-grid"></div>
  </div>

  <div class="section">
    <h2>Votre mode de vacances ?</h2>
    <div class="cards-grid" id="modes-grid"></div>
  </div>

  <div class="section">
    <h2>Découvrez une nouvelle ville ?</h2>
    <div class="cards-grid" id="cities-grid"></div>
  </div>
</div>

<nav class="bottom-nav">
  <a href="/" class="bouton">
    <button class="boutonNav">
      <svg class="nav-icon" viewBox="0 0 24 24"><path d="M12 3l9 8h-3v9h-5v-6H11v6H6v-9H3z"/></svg>
      Accueil
    </button>
  </a>
  <a href="/Destinations" class="bouton">
    <button class="boutonNav active">
      <svg class="nav-icon" viewBox="0 0 24 24" aria-hidden="true">
        <circle cx="11" cy="11" r="6"></circle>
        <path d="M20 20l-3.5-3.5" stroke="white" stroke-width="2" stroke-linecap="round" fill="none"/>
      </svg>
      Exploration
    </button>
  </a>
  <a href="/topics" class="bouton">
    <button class="boutonNav">
      <svg class="nav-icon" viewBox="0 0 24 24">
        <path d="M5 4h11a2 2 0 0 1 2 2v12H7a2 2 0 0 1-2-2V4z"/>
        <rect x="3" y="6" width="2" height="12" rx="1" ry="1"/>
        <path d="M8 8h7M8 11h7M8 14h5" stroke="white" stroke-width="2" fill="none" stroke-linecap="round"/>
      </svg>
      Carnet
    </button>
  </a>
  <a href="/makejourney" class="bouton">
    <button class="boutonNav">
      <svg class="nav-icon" viewBox="0 0 24 24">
        <path d="M12 2a6 6 0 0 0-6 6c0 4.5 6 12 6 12s6-7.5 6-12a6 6 0 0 0-6-6zm0 8.5A2.5 2.5 0 1 1 12 5.5a2.5 2.5 0 0 1 0 5z"/>
        <path d="M15.2 14.2l3.2 3.2-2.3 0.5-1.4 1.4-0.5-2.3 1-1z"/>
      </svg>
      Création
    </button>
  </a>
</nav>

<style>
  :global(:root) {
    --bg:#190028;
    --accent:#FF6000;
    --text:#F9F0F7;
    --text-dim:rgba(249,240,247,.80);
    --card:#1e1e1e;
    --nav-h:64px;
    --content-max: 1100px;
    --page-pad: clamp(12px, 3vw, 24px);
  }

  :global(*) { box-sizing:border-box }
  
  :global(html), :global(body) { height:100% }
  
  :global(body) {
    margin:0; background:var(--bg); color:var(--text);
    font-family:Poppins,system-ui,-apple-system,Segoe UI,Roboto,Inter,Arial;
    min-height:100vh;
  }

  .app{
    width:100vw; min-height:100vh;
    padding: clamp(40px,6vh,56px) var(--page-pad) calc(var(--nav-h) + env(safe-area-inset-bottom) + var(--page-pad));
    display:flex; flex-direction:column; align-items:center; gap:clamp(12px,2.5vh,20px);
    overflow:hidden; border-radius:20px;
    background:#190028;
  }

  /* Barre de statut + en-tête "Découvrir" */
  .statusbar{
    width:100%; max-width:var(--content-max);
    display:flex; flex-direction:column; align-items:center; gap:clamp(12px,2vh,16px);
  }
  .status-row{
    width:100%;
    display:flex; justify-content:space-between; align-items:center;
    padding:10px 22px 0 22px;
  }
  .time{ font:600 clamp(15px,1.7vw,17px)/40px Inter,Poppins,sans-serif; color:#F9F0F7; letter-spacing:.35px }
  .status{
    width:78px; height:12px; display:flex; align-items:flex-end; gap:2px;
  }
  .status div{ width:3px; border-radius:1px; background:#F9F0F7 }
  .status div:nth-child(1){ height:5px }
  .status div:nth-child(2){ height:7px }
  .status div:nth-child(3){ height:9px }
  .status div:nth-child(4){ height:12px }

  .discover-row{
    width:100%; max-width:var(--content-max);
    display:flex; align-items:center; justify-content:space-between; gap:12px;
    padding: 0 var(--page-pad);
  }
  .discover-left{ display:flex; align-items:center; gap:12px }
  .logo-dot{ width:25px; height:25px; background:var(--accent); border-radius:4px }
  .discover-title{ font-size: clamp(22px,4.5vw,32px); font-weight:600; text-transform:capitalize }

  /* Recherche + mini actions (panier + favoris) */
  .search-area{
    width:min(820px, 100%);
    position:relative;
    padding: 0 var(--page-pad);
    display:flex; align-items:center; gap:10px; justify-content:space-between;
  }
  .search-wrap{
    flex:1;
    background:#F9F0F7; border-radius:10px; padding:7px 11px;
    display:flex; align-items:center; gap:7px;
  }
  .search-icon{ width:25px; height:25px; background:#190028; border-radius:4px; flex:0 0 25px }
  #search{
    flex:1; border:none; outline:none; background:transparent;
    font-size:15px; color:#190028;
  }

  .actions{ position:relative; display:flex; flex-direction:column; gap:8px; align-items:flex-end; }

  /* Panier */
  .basket-btn{
    background:rgba(255,255,255,.08); color:#fff; border:none;
    padding:6px 10px; border-radius:8px; font-weight:600; font-size:12px; line-height:1;
    cursor:pointer; display:flex; align-items:center; gap:6px;
  }
  #basketCount{
    font-size:11px; background:var(--accent); color:#190028; border-radius:999px; padding:2px 6px; font-weight:800;
  }
  #floatingBasket{
    position:absolute; top: calc(100% + 8px); right:0;
    width: min(420px, 80vw);
    max-height: min(60vh, 520px);
    overflow:auto;
    background:var(--card); border-radius:12px; padding:10px; display:none; z-index:20;
    box-shadow:0 16px 36px rgba(0,0,0,.45);
  }
  #floatingBasket h4{ margin:6px 0 8px; font-size:14px; opacity:.9 }
  .basket-empty{ color:var(--text-dim); font-size:13px; margin:6px 0; }
  .basket-item{
    display:flex; align-items:center; gap:8px; padding:6px; border-radius:8px;
    background:rgba(255,255,255,.04); margin-bottom:6px;
  }
  .basket-item img{ width:36px; height:36px; border-radius:6px; object-fit:cover }
  .basket-item .bi-name{ font-size:13px; font-weight:600 }
  .basket-item .bi-meta{ font-size:11px; opacity:.75 }
  .basket-item .bi-remove{
    margin-left:auto; background:rgba(255,255,255,.08); color:#fff; border:none;
    padding:4px 8px; border-radius:6px; font-size:11px; cursor:pointer;
  }
  .basket-footer{ display:flex; gap:8px; justify-content:flex-end; margin-top:8px; }
  .basket-footer a, .basket-footer button{
    background:var(--accent); color:#190028; border:none; border-radius:8px; padding:6px 10px; font-weight:800; cursor:pointer;
  }

  /* --- Styles pour les Favoris (Cœur) --- */
  .like-btn {
    background: rgba(255, 255, 255, .08); color: #fff; border: none;
    padding: 6px 10px; border-radius: 8px; font-weight: 600; font-size: 12px; line-height: 1;
    cursor: pointer; display: flex; align-items: center; gap: 6px;
  }
  .like-btn.active { color: #ff4081; }
  #likesCount {
    font-size: 11px; background: #ff4081; color: #fff; border-radius: 999px; padding: 2px 6px; font-weight: 800;
  }
  #floatingLikes {
    position: absolute; top: calc(100% + 8px); right: 0;
    width: min(420px, 80vw); max-height: min(60vh, 520px); overflow: auto;
    background: var(--card); border-radius: 12px; padding: 10px; display: none; z-index: 20;
    box-shadow: 0 16px 36px rgba(0,0,0,.45);
  }
  #floatingLikes h4 { margin: 6px 0 8px; font-size: 14px; opacity: .9; }
  .likes-empty { color: var(--text-dim); font-size: 13px; margin: 6px 0; }
  .bi-like-remove {
    margin-left: auto; background: transparent; color: #ff4081; border: 1px solid rgba(255,255,255,0.1);
    padding: 4px 8px; border-radius: 6px; font-size: 14px; cursor: pointer;
  }
  /* Bouton coeur individuel */
  .fav-action {
    background: rgba(255,255,255,0.1); border:none; border-radius:50%;
    width: 28px; height: 28px; cursor: pointer; display: flex; align-items: center; justify-content: center;
    font-size: 14px; color: #ccc; transition: all 0.2s;
  }
  .fav-action.active { color: #ff4081; background: rgba(255, 64, 129, 0.15); transform: scale(1.1); }
  .fav-action:hover { background: rgba(255,255,255,0.2); }


  /* Résultats recherche */
  .results{
    position:absolute; top: calc(100% + 6px); left: 0; right: 0;
    display:none;
    background:#0f0a18; border:1px solid rgba(255,255,255,.12);
    border-radius:10px; overflow:auto;
    max-height:min(60vh, 520px);
    z-index: 10;
  }
  .result-item{
    display:flex; align-items:center; gap:10px; padding:10px 12px;
    background:#fdfeff; color:#000; cursor:pointer; justify-content:space-between;
  }
  .result-left{ display:flex; align-items:center; gap:10px; min-width:0; }
  .result-item + .result-item{ border-top:1px solid #e9ecef }
  .result-item:hover{ background:#e9ecef }
  .result-thumb{
    width:60px; height:60px; border-radius:8px; object-fit:cover; flex:0 0 60px;
  }
  .result-name{ font-weight:600; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; max-width:46vw; }
  .no-res{ padding:10px 12px; background:#fdfeff; color:#000 }

  .result-add-btn{
    flex:0 0 auto;
    background:#0f5132; color:#fff; border:none; padding:8px 10px;
    border-radius:8px; font-weight:700; font-size:12px;
    cursor:pointer;
  }
  .result-add-btn:active{ transform:translateY(1px) }
  .results-loader, .results-end{
    padding:10px 12px; background:#fdfeff; color:#000; text-align:center; font-size:13px;
    border-top:1px solid #e9ecef;
  }

  /* Sections */
  .section{
    width:100%; max-width:var(--content-max);
    display:flex; flex-direction:column; align-items:center; gap:16px;
    margin-top:16px; padding: 0 var(--page-pad);
  }
  .section h2{
    margin:0; font-size: clamp(18px,3.3vw,22px); font-weight:600; text-align:center;
  }

  /* Grille responsive pour les cartes */
  .cards-grid{
    width:100%;
    display:grid;
    grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
    gap:14px;
  }
  .card-box{
    height: clamp(80px, 18vw, 120px);
    padding:5px 5px 5px 10px;
    border-radius:10px; display:flex; align-items:center; justify-content:flex-end; gap:20px;
    position:relative; overflow:hidden;
    transition: transform .15s ease, box-shadow .15s ease;
    background: linear-gradient(135deg, rgba(255,255,255,.06), rgba(255,255,255,.03));
    border:1px solid rgba(255,255,255,.06);
    cursor:pointer;
  }
  .card-box:hover{ transform: translateY(-2px); box-shadow:0 6px 18px rgba(0,0,0,.18) }
  .card-box.disabled{ opacity:.5; cursor:not-allowed; }
  .card-box.disabled:hover{ transform:none; box-shadow:none; }

  /* Titre CENTRÉ dans la carte */
  .card-name{
    position:absolute;
    left:50%;
    top:50%;
    transform: translate(-50%,-50%);
    font-size: clamp(16px, 3.2vw, 20px);
    font-weight:600;
    color:#F9F0F7;
    pointer-events:none;
    opacity:.95;
    white-space:nowrap;
    text-align:center;
  }
  .card-thumb{
    width: clamp(60px, 9vw, 80px); height: clamp(60px, 9vw, 80px);
    border-radius:5px; background:rgba(44,17,37,.10); flex:0 0 auto;
    background-size: cover; background-position: center;
  }

  /* Bottom nav */
  nav.bottom-nav{
    position: fixed;
    left:0; right:0; bottom:0;
    height:var(--nav-h);
    display:flex; gap:8px; padding:10px;
    background:rgba(0,0,0,.25); backdrop-filter:blur(6px);
    border-top:1px solid rgba(255,255,255,.06);
  }
  .boutonNav{
    flex:1; border:none; border-radius:12px; padding:10px 8px;
    background:rgba(255,255,255,.06); color:#fff; font-weight:600; cursor:pointer;
    display:flex; align-items:center; justify-content:center; gap:6px;
  }
  .boutonNav.active{ background:var(--accent); color:#190028 }
  .nav-icon{ width:18px; height:18px; vertical-align:-3px; fill:#fff; }

  /* Tweaks breakpoints */
  @media (min-width: 640px){
    .logo-dot{ width:28px; height:28px }
    .search-icon{ width:28px; height:28px }
  }
  @media (min-width: 1024px){
    .cards-grid{ grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); }
    .result-name{ max-width:60vw; }
  }
</style>