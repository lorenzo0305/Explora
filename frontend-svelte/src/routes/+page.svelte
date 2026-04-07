<div class="app-frame">
  <div class="content-panel" style="padding-top: 0; padding-left: 0; padding-right: 0;">

    <div style="position: relative; width: 100%; height: 35vh; min-height: 250px;">
      <img src="/static/img/accueil.jpg" style="width: 100%; height: 100%; object-fit: cover; display: block;" alt="Accueil">
      <div style="position: absolute; inset: 0; background: rgba(0,0,0,0.3); display: flex; align-items: center; justify-content: center;">
        <h1 style="color: #FFF; font-size: 3.5rem; font-weight: 800; letter-spacing: 3px; text-shadow: 0 4px 15px rgba(0,0,0,0.6); margin: 0;">
          EXPLORA</h1>
      </div>
    </div>

    <div class="content-inner" style="padding: 20px;">

      <div class="header-row" style="display: none;">
        <div class="header-greeting">
          <svg class="avatar-icon" viewBox="0 0 24 24">
            <path d="M12 12c2.21 0 4-1.79 4-4s-1.79-4-4-4-4 1.79-4 4 1.79 4 4 4zm0 2c-2.67 0-8 1.34-8 4v2h16v-2c0-2.66-5.33-4-8-4z" />
          </svg>
          <div class="greeting-text">Bonjour</div>
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

      <div class="presentation-panel">
        <div class="title">Bienvenue sur Explora <br>Nous créons votre voyage de rêve !</div>
        <div class="desc">
          Découvrez des destinations inoubliables, planifiées sur mesure selon vos envies. Laissez-nous vous guider
          vers votre prochaine aventure.
        </div>
      </div>

      <div class="how-it-works-panel">
        <div class="hiw-title">Comment ça marche ?</div>
        <div class="hiw-steps">
          <div class="hiw-step">
            <div class="hiw-icon">💡</div>
            <div class="hiw-text">
              <strong>1. Allez dans l'onglet Creation</strong>
            </div>
          </div>
          <div class="hiw-step">
            <div class="hiw-icon">🗺️</div>
            <div class="hiw-text">
              <strong>2. Entrez vos critères de préférences pour votre voyage</strong>
            </div>
          </div>
          <div class="hiw-step">
            <div class="hiw-icon">🚀</div>
            <div class="hiw-text">
              <strong>3. Appuyez sur soumettre et laisser la magie opérer</strong>
            </div>
          </div>
        </div>
      </div>

      <div class="section">
        <div class="section-title">Récemment vus !</div>
        <div id="recentList" class="featured-list"></div>
        <p id="recentEmpty" class="section-subtitle" style="display:none;opacity:.8;">Aucun voyage pour le moment.</p>
      </div>

    </div>
  </div>

  <nav class="bottom-nav" style="position: relative; z-index: 10;">
    <a href="/" class="bouton"><button class="boutonNav active"><svg class="nav-icon" viewBox="0 0 24 24">
          <path d="M12 3l9 8h-3v9h-5v-6H11v6H6v-9H3z" />
        </svg>Accueil</button></a>
    <a href="/Destinations" class="bouton"><button class="boutonNav"><svg class="nav-icon" viewBox="0 0 24 24">
          <circle cx="11" cy="11" r="6"></circle>
          <path d="M20 20l-3.5-3.5" stroke="white" stroke-width="2" stroke-linecap="round" fill="none" />
        </svg>Exploration</button></a>
    <a href="/topics" class="bouton"><button class="boutonNav"><svg class="nav-icon" viewBox="0 0 24 24">
          <path d="M5 4h11a2 2 0 0 1 2 2v12H7a2 2 0 0 1-2-2V4z" />
          <rect x="3" y="6" width="2" height="12" rx="1" ry="1" />
          <path d="M8 8h7M8 11h7M8 14h5" stroke="white" stroke-width="2" fill="none" stroke-linecap="round" />
        </svg>Carnet</button></a>
    <a href="/makejourney" class="bouton"><button class="boutonNav"><svg class="nav-icon" viewBox="0 0 24 24">
          <path d="M12 2a6 6 0 0 0-6 6c0 4.5 6 12 6 12s6-7.5 6-12a6 6 0 0 0-6-6zm0 8.5A2.5 2.5 0 1 1 12 5.5a2.5 2.5 0 0 1 0 5z" />
          <path d="M15.2 14.2l3.2 3.2-2.3 0.5-1.4 1.4-0.5-2.3 1-1z" />
        </svg>Création</button></a>
  </nav>
</div>

<style>
  :root {
    --bg: #FFF6F0;
    --accent: #9CEEFF;
    --text: hsl(0, 100%, 0%);
    --text-dim: rgba(207, 207, 207, .70);
    --card: rgba(255, 255, 255, .04);
    --nav-h: 64px;
  }

  * {
    box-sizing: border-box
  }

  .app-frame {
    width: 100vw;
    height: 100vh;
    background: var(--bg);
    position: relative;
    overflow: hidden;
    display: flex;
    flex-direction: column;
    /* J'ai ajouté ces propriétés depuis ton ancien "body" pour que ça marche bien ici */
    color: var(--text);
    font-family: Merienda, system-ui, -apple-system, Segoe UI, Roboto, Inter, Arial;
  }

  .content-panel {
    padding: 28px 20px calc(var(--nav-h) + 24px) 20px;
    display: flex;
    flex-direction: column;
    gap: 32px;
    flex: 1;
    overflow: auto;
    scrollbar-width: thin;
  }

  .content-inner {
    width: 100%;
    max-width: 960px;
    margin: 0 auto;
    display: flex;
    flex-direction: column;
    gap: 32px;
  }

  /* Header : Salutation à gauche, Actions à droite */
  .header-row {
    display: flex;
    align-items: center;
    justify-content: space-between;
    margin-top: 20px;
    margin-left: 10px;
    margin-right: 10px;
  }

  .header-greeting {
    display: flex;
    align-items: center;
    gap: 12px;
  }

  .avatar-icon {
    width: 40px;
    height: 40px;
    border-radius: 50%;
    background-color: var(--accent);
    fill: white;
    padding: 8px;
    flex-shrink: 0;
  }

  .greeting-text {
    font-size: 24px;
    font-weight: 600;
    color: var(--text);
  }

  /* Actions (Panier / Coeur) */
  .actions {
    position: relative;
    display: flex;
    align-items: center;
    gap: 8px;
  }

  .action-btn {
    background: rgba(255, 255, 255, .08);
    color: #fff;
    border: none;
    padding: 6px 10px;
    border-radius: 8px;
    font-weight: 600;
    font-size: 12px;
    line-height: 1;
    cursor: pointer;
    display: flex;
    align-items: center;
    gap: 6px;
  }

  .action-btn.active {
    color: #ff4081;
  }

  .badge {
    font-size: 11px;
    background: var(--accent);
    color: #190028;
    border-radius: 999px;
    padding: 2px 6px;
    font-weight: 800;
  }

  .badge.pink {
    background: #ff4081;
    color: #fff;
  }

  /* Panels flottants */
  .floating-panel {
    position: absolute;
    top: calc(100% + 8px);
    right: 0;
    width: min(300px, 80vw);
    max-height: min(50vh, 400px);
    overflow: auto;
    background: #1e1e1e;
    border-radius: 12px;
    padding: 10px;
    display: none;
    z-index: 100;
    box-shadow: 0 16px 36px rgba(0, 0, 0, .5);
    border: 1px solid rgba(255, 255, 255, .1);
  }

  .floating-panel h4 {
    margin: 6px 0 8px;
    font-size: 14px;
    opacity: .9;
    border-bottom: 1px solid rgba(255, 255, 255, .1);
    padding-bottom: 6px;
  }

  .panel-empty {
    color: var(--text-dim);
    font-size: 13px;
    margin: 6px 0;
    font-style: italic;
  }

  .panel-item {
    display: flex;
    align-items: center;
    gap: 8px;
    padding: 6px;
    border-radius: 8px;
    background: rgba(255, 255, 255, .04);
    margin-bottom: 6px;
  }

  .panel-item img {
    width: 36px;
    height: 36px;
    border-radius: 6px;
    object-fit: cover
  }

  .panel-item .pi-name {
    font-size: 13px;
    font-weight: 600;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
    max-width: 140px;
  }

  .panel-item .pi-remove {
    margin-left: auto;
    background: rgba(255, 255, 255, .08);
    color: #fff;
    border: none;
    padding: 4px 8px;
    border-radius: 6px;
    font-size: 11px;
    cursor: pointer;
  }

  .panel-footer {
    display: flex;
    justify-content: flex-end;
    margin-top: 8px;
  }

  .panel-footer a {
    background: var(--accent);
    color: #190028;
    border-radius: 8px;
    padding: 6px 10px;
    font-weight: 700;
    text-decoration: none;
    font-size: 12px;
  }

  .presentation-panel {
    background: #9CEEFF;
    border-radius: 20px;
    padding: 24px;
    margin-top: -10px;
    box-shadow: 0 10px 40px rgba(0, 0, 0, 0.08);
    text-align: center;
    position: relative;
    z-index: 5;
  }

  .presentation-panel .title {
    font-size: 22px;
    font-weight: 800;
    color: var(--text);
    line-height: 1.3;
    margin-bottom: 12px;
  }

  .presentation-panel .desc {
    font-size: 14px;
    color: var(--text);
    opacity: 0.75;
    line-height: 1.6;
  }

  .how-it-works-panel {
    background: #FFF3C2;
    border-radius: 20px;
    padding: 24px;
    box-shadow: 0 10px 40px rgba(0, 0, 0, 0.05);
    margin-top: 10px;
  }

  .hiw-title {
    font-size: 20px;
    font-weight: 800;
    color: var(--text);
    margin-bottom: 20px;
    text-align: center;
  }

  .hiw-steps {
    position: relative;
    display: flex;
    flex-direction: row;
    justify-content: space-between;
    gap: 16px;
  }

  .hiw-step:not(:last-child)::before {
    content: "";
    position: absolute;
    top: 23px;
    left: calc(50% + 34px);
    right: calc(-50% + 22px);
    height: 2px;
    background: var(--text);
    opacity: 0.15;
    z-index: 1;
  }

  .hiw-step:not(:last-child)::after {
    content: "";
    position: absolute;
    top: 19px;
    right: calc(-50% + 16px);
    border-top: 5px solid transparent;
    border-bottom: 5px solid transparent;
    border-left: 6px solid var(--text);
    opacity: 0.15;
    z-index: 1;
  }

  .hiw-step {
    position: relative;
    display: flex;
    flex-direction: column;
    align-items: center;
    gap: 16px;
    z-index: 2;
    background: transparent;
    padding: 0;
    border-radius: 0;
    flex: 1;
    text-align: center;
  }

  .hiw-icon {
    font-size: 24px;
    background: white;
    width: 48px;
    height: 48px;
    border-radius: 50%;
    display: flex;
    align-items: center;
    justify-content: center;
    box-shadow: 0 4px 12px rgba(0, 0, 0, 0.08);
    flex-shrink: 0;
  }

  .hiw-text {
    display: flex;
    flex-direction: column;
    gap: 4px;
    background: white;
    padding: 14px 16px;
    border-radius: 12px;
    width: 100%;
    box-shadow: 0 2px 10px rgba(0, 0, 0, 0.04);
  }

  .hiw-text strong {
    font-size: 15px;
    color: var(--text);
  }

  .hiw-text span {
    font-size: 13px;
    color: var(--text);
    opacity: 0.7;
    line-height: 1.4;
  }

  .section {
    display: flex;
    flex-direction: column;
    align-items: center;
    gap: 16px;
  }

  .section-title {
    font-size: 20px;
    font-weight: 600;
    color: var(--text);
    text-align: center;
    margin-top: 24px;
  }

  /* Grille activités */
  .proposals-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
    gap: 16px;
    width: 100%;
  }

  .proposal-item {
    display: flex;
    flex-direction: column;
    align-items: center;
    gap: 10px;
    min-height: 200px;
    padding: 8px;
    border-radius: 8px;
    color: #F9F0F7;
    background: rgba(255, 255, 255, .00);
  }

  .proposal-item .thumb {
    width: 150px;
    height: 150px;
    border-radius: 7.5px;
    flex-shrink: 0;
    background-size: cover;
    background-position: center;
    box-shadow: 0 0 0 1px rgba(255, 255, 255, .06) inset;
  }

  .proposal-item .name {
    font-size: 15px;
    font-weight: 600;
    text-align: center;
    max-width: 150px;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
  }

  .proposal-item.placeholder .thumb {
    background: linear-gradient(180deg, #2b2137 0%, #120c19 100%);
  }

  .proposal-item.placeholder .name {
    opacity: .85
  }

  /* Listes horizontales */
  .featured-list {
    display: flex;
    gap: 20px;
    overflow-x: auto;
    padding-bottom: 10px;
  }

  .featured-item {
    width: 150px;
    flex: 0 0 auto;
    display: flex;
    flex-direction: column;
    gap: 12px;
    cursor: pointer;
  }

  .featured-item .thumb {
    width: 150px;
    height: 150px;
    border-radius: 7.5px;
    background-size: cover;
    background-position: center;
  }

  .featured-item .name {
    font-size: 18px;
    font-weight: 700;
    color: var(--text);
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
  }

  .featured-item .comment {
    height: 50px;
    padding: 5px;
    background: rgba(249, 240, 247, .15);
    border-radius: 10.5px;
    color: rgba(249, 240, 247, .60);
    font-size: 9px;
    font-weight: 600;
    line-height: 1.5;
    overflow: hidden;
    text-overflow: ellipsis;
  }

  .featured-item.placeholder .name {
    opacity: .95
  }

  .stars {
    display: flex;
    gap: 4px;
  }

  .stars .star {
    width: 15px;
    height: 15px;
    background-color: rgba(255, 255, 255, .7);
    clip-path: polygon(50% 0%, 61.8% 38.2%, 100% 38.2%, 69.1% 61.8%, 80.9% 100%, 50% 76.4%, 19.1% 100%, 30.9% 61.8%, 0% 38.2%, 38.2% 38.2%);
  }

  .stars .star.active {
    background-color: #FFD700;
  }

  nav.bottom-nav {
    position: absolute;
    left: 0;
    right: 0;
    bottom: 0;
    height: var(--nav-h);
    display: flex;
    gap: 8px;
    padding: 10px;
    background: rgba(0, 0, 0, .25);
    backdrop-filter: blur(6px);
    border-top: 1px solid rgba(255, 255, 255, .06);
  }

  .boutonNav {
    flex: 1;
    border: none;
    border-radius: 12px;
    padding: 10px 8px;
    background: rgba(255, 255, 255, .06);
    color: #fff;
    font-weight: 600;
    cursor: pointer;
    display: flex;
    align-items: center;
    justify-content: center;
    gap: 6px;
  }

  .boutonNav.active {
    background: var(--accent);
    color: #190028
  }

  .nav-icon {
    width: 18px;
    height: 18px;
    vertical-align: -3px;
    fill: #fff;
  }
</style>