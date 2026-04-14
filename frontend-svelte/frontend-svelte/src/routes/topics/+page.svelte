<svelte:head>
  <title>WishAdventure — Carnet</title>
  <link href="https://fonts.googleapis.com/css2?family=Poppins:wght@300;500;600;700&display=swap" rel="stylesheet">
  <script src="/static/scripts/Carnet.js" defer></script>
</svelte:head>

<div class="app-frame">
  <div class="content-panel">
    <div class="content-inner">

      <div class="top-title">
        <div class="left">
          <div class="logo"></div>
          <div class="title">Mon carnet</div>
        </div>
        
        <div class="actions">
          <button id="likesIcon" class="action-btn" type="button" title="Mes Favoris">
            ❤️ <span id="likesCount" class="badge pink" hidden>0</span>
          </button>
          <div id="floatingLikes" class="floating-panel" aria-live="polite"></div>

          <button id="basketIcon" class="action-btn" type="button" title="Panier">
            🧺 <span id="basketCount" class="badge" hidden>0</span>
          </button>
          <div id="floatingBasket" class="floating-panel" aria-live="polite"></div>
        </div>
      </div>

      <div>
        <div class="section-title">Téléchargements</div>
        <div id="topicsList" class="list"></div>
        <p id="emptyState" class="empty" style="display:none;">Aucun voyage pour l’instant.</p>
      </div>

    </div>
  </div>

  <nav class="bottom-nav">
    <a href="/" class="bouton"><button class="boutonNav"><svg class="nav-icon" viewBox="0 0 24 24"><path d="M12 3l9 8h-3v9h-5v-6H11v6H6v-9H3z"/></svg>Accueil</button></a>
    <a href="/Destinations" class="bouton"><button class="boutonNav"><svg class="nav-icon" viewBox="0 0 24 24"><circle cx="11" cy="11" r="6"></circle><path d="M20 20l-3.5-3.5" stroke="white" stroke-width="2" stroke-linecap="round" fill="none"/></svg>Exploration</button></a>
    <a href="/topics" class="bouton"><button class="boutonNav active"><svg class="nav-icon" viewBox="0 0 24 24"><path d="M5 4h11a2 2 0 0 1 2 2v12H7a2 2 0 0 1-2-2V4z"/><rect x="3" y="6" width="2" height="12" rx="1" ry="1"/><path d="M8 8h7M8 11h7M8 14h5" stroke="white" stroke-width="2" fill="none" stroke-linecap="round"/></svg>Carnet</button></a>
    <a href="/makejourney" class="bouton"><button class="boutonNav"><svg class="nav-icon" viewBox="0 0 24 24"><path d="M12 2a6 6 0 0 0-6 6c0 4.5 6 12 6 12s6-7.5 6-12a6 6 0 0 0-6-6zm0 8.5A2.5 2.5 0 1 1 12 5.5a2.5 2.5 0 0 1 0 5z"/><path d="M15.2 14.2l3.2 3.2-2.3 0.5-1.4 1.4-0.5-2.3 1-1z"/></svg>Création</button></a>
  </nav>
</div>

<style>
  :global(:root){
    --bg:#190028;
    --accent:#FF6000;
    --text:#F9F0F7;
    --text-dim:rgba(255,255,255,.70);
    --card:rgba(255,255,255,.04);
    --nav-h:64px;
  }
  :global(*) {box-sizing:border-box}
  :global(html), :global(body) {height:100%}
  :global(body) {
    margin:0; background:#0f0a18; color:var(--text);
    font-family:Poppins,system-ui,-apple-system,Segoe UI,Roboto,Inter,Arial;
    min-height:100vh;
  }

  .app-frame{
    width:100vw; height:100vh;
    background:var(--bg);
    position:relative; overflow:hidden;
    display:flex; flex-direction:column;
  }
  .content-panel{
    padding: 28px 20px calc(var(--nav-h) + 24px) 20px;
    display:flex; flex-direction:column; gap:16px; flex:1; overflow:auto;
    scrollbar-width:thin;
  }
  .content-inner{
    width:100%; max-width: 960px; margin: 0 auto;
    display:flex; flex-direction:column; gap:12px;
  }

  .top-title{ display:flex; align-items:center; justify-content:space-between; }
  .top-title .left{ display:flex; align-items:center; gap:12px; }
  .top-title .logo{ width:25px; height:25px; background:var(--accent); border-radius:4px; flex:0 0 25px; }
  .top-title .title{ font-size:30px; font-weight:600; color:var(--text); }

  /* Actions (Panier / Coeur) en haut à droite */
  .actions{ position:relative; display:flex; align-items:center; gap:8px; }
  .action-btn{
    background:rgba(255,255,255,.08); color:#fff; border:none;
    padding:6px 10px; border-radius:8px; font-weight:600; font-size:12px; line-height:1;
    cursor:pointer; display:flex; align-items:center; gap:6px;
  }
  .action-btn.active { color: #ff4081; }
  .badge{ font-size:11px; background:var(--accent); color:#190028; border-radius:999px; padding:2px 6px; font-weight:800; }
  .badge.pink{ background:#ff4081; color:#fff; }

  /* Panels flottants - Protégés car le JS ajoute des trucs dedans */
  :global(.floating-panel) {
    position:absolute; top:calc(100% + 8px); right:0;
    width:min(300px, 80vw); max-height:min(50vh, 400px); overflow:auto;
    background:#1e1e1e; border-radius:12px; padding:10px; display:none; z-index:100;
    box-shadow:0 16px 36px rgba(0,0,0,.5); border:1px solid rgba(255,255,255,.1);
  }
  :global(.floating-panel h4) { margin:6px 0 8px; font-size:14px; opacity:.9; border-bottom:1px solid rgba(255,255,255,.1); padding-bottom:6px; }
  :global(.panel-empty) { color:var(--text-dim); font-size:13px; margin:6px 0; font-style:italic; }
  :global(.panel-item) {
    display:flex; align-items:center; gap:8px; padding:6px; border-radius:8px;
    background:rgba(255,255,255,.04); margin-bottom:6px;
  }
  :global(.panel-item img) { width:36px; height:36px; border-radius:6px; object-fit:cover }
  :global(.panel-item .pi-name) { font-size:13px; font-weight:600; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; max-width:140px; }
  :global(.panel-item .pi-remove) {
    margin-left:auto; background:rgba(255,255,255,.08); color:#fff; border:none;
    padding:4px 8px; border-radius:6px; font-size:11px; cursor:pointer;
  }
  :global(.panel-footer) { display:flex; justify-content:flex-end; margin-top:8px; }
  :global(.panel-footer a) { background:var(--accent); color:#190028; border-radius:8px; padding:6px 10px; font-weight:700; text-decoration:none; font-size:12px; }

  .section-title{ font-size:13px; font-weight:500; color:var(--text); }

  .list{ display:flex; flex-direction:column; gap:8px; }
  
  /* Protégé car ajouté par Carnet.js */
  :global(.item) {
    display:flex; align-items:center; justify-content:space-between;
    min-height:60px; border-radius:6px; padding:6px 6px 6px 0;
    cursor:pointer; user-select:none;
  }
  :global(.item:hover) { background:rgba(255,255,255,.02); }
  :global(.item .left) { display:flex; align-items:center; gap:16px; }
  :global(.thumb) {
    width:60px; height:60px; border-radius:5px; background:#333; flex:0 0 60px;
    background-size:cover; background-position:center;
    box-shadow:0 0 0 1px rgba(255,255,255,.06) inset;
  }
  :global(.meta) { display:flex; flex-direction:column; }
  :global(.name) { font-size:15px; font-weight:700; color:var(--text); }
  :global(.desc) { font-size:13px; font-weight:500; color:var(--text-dim); }

  :global(.actions-right) { display:flex; align-items:center; gap:8px; }
  :global(.icon-btn) {
    width:30px; height:30px; border-radius:6px;
    display:flex; align-items:center; justify-content:center;
    border:1px solid rgba(255,255,255,.12);
    background:transparent; cursor:pointer; flex:0 0 30px;
  }
  :global(.icon-btn:hover) { background:rgba(255,255,255,.06); border-color:rgba(255,255,255,.22); }
  :global(.icon-btn:focus) { outline:2px solid rgba(255,255,255,.35); outline-offset:2px; }
  :global(.icon-btn svg) { width:16px; height:16px; fill:none; stroke:#F9F0F7; stroke-width:2; }
  :global(.edit-btn svg) { stroke:#FFBC66; }
  :global(.delete-btn svg) { stroke:#F9F0F7; }

  .empty{ font-size:13px; color:var(--text-dim); }

  nav.bottom-nav{
    position:absolute; left:0; right:0; bottom:0;
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
</style>