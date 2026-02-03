(function (global){
  "use strict";

  function asArray(v){ return Array.isArray(v) ? v : (v==null ? [] : [v]); }
  function firstString(){
    for (let i=0;i<arguments.length;i++){
      const v = arguments[i];
      if (!v) continue;
      if (typeof v === 'string' && v.trim()) return v.trim();
      if (Array.isArray(v)){
        for (const e of v){
          if (typeof e === 'string' && e.trim()) return e.trim();
          if (e && typeof e === 'object'){
            const s = e['@value'] || e.value || e.url || e['@id'];
            if (typeof s === 'string' && s.trim()) return s.trim();
          }
        }
      }
      if (v && typeof v === 'object'){
        const s = v['@value'] || v.value || v.url || v['@id'];
        if (typeof s === 'string' && s.trim()) return s.trim();
      }
    }
    return '';
  }

  function isImageUrl(u){
    return typeof u === 'string' && /^https?:\/\//i.test(u) &&
           /\.(jpe?g|png|webp|gif)(\?|#|$)/i.test(u);
  }

  // Essaye d’extraire une image d’un objet riche (DataTourisme, Schema.org, etc.)
  function extractFromObject(obj){
    if (!obj || typeof obj !== 'object') return '';

    // 1) Champs directs usuels
    const directKeys = ['image','photo','thumbnail','picture','cover','logo',
                        'https://schema.org/image','https://schema.org/logo'];
    for (const k of directKeys){
      const cand = firstString(obj[k]);
      if (isImageUrl(cand)) return cand;
    }

    // 2) DataTourisme: has(Main)Representation → ebucore:hasRelatedResource → ebucore:locator
    const dtRepKeys = [
      'https://www.datatourisme.fr/ontology/core#hasMainRepresentation',
      'https://www.datatourisme.fr/ontology/core#hasRepresentation',
      'hasMainRepresentation','hasRepresentation'
    ];
    for (const rk of dtRepKeys){
      const reps = asArray(obj[rk]);
      for (const rep of reps){
        const rels = [].concat(asArray(rep?.['ebucore:hasRelatedResource']),
                               asArray(rep?.hasRelatedResource));
        for (const r of rels){
          const locs = [].concat(asArray(r?.['ebucore:locator']),
                                 asArray(r?.locator));
          for (const loc of locs){
            if (typeof loc === 'string' && isImageUrl(loc)) return loc;
            if (loc && typeof loc==='object'){
              const u = firstString(loc.url, loc['@id'], loc['@value']);
              if (isImageUrl(u)) return u;
            }
          }
        }
      }
    }

    // 3) Parcourir les tableaux d’objets à la recherche de contentUrl/url
    for (const v of Object.values(obj)){
      if (Array.isArray(v)){
        for (const it of v){
          if (it && typeof it==='object'){
            const u = firstString(it.contentUrl, it.url, it['@id'],
                                  it['https://schema.org/contentUrl'],
                                  it['https://schema.org/url']);
            if (isImageUrl(u)) return u;
          }
        }
      }
    }

    // 4) Heuristique sur les clés contenant "image|photo|thumbnail|picture|illustration"
    for (const [k,v] of Object.entries(obj)){
      if (!/image|photo|thumbnail|picture|illustration|logo/i.test(k)) continue;
      const cand = firstString(v);
      if (isImageUrl(cand)) return cand;
    }

    return '';
  }

  function getBestImage(item, opts={}){
    const def = opts.default || '/static/img/no-image.jpg';
    if (!item) return def;

    // a) si l’API a déjà "image", prends-la
    if (isImageUrl(item.image)) return item.image;

    // b) sinon, tente d’extraire depuis l’objet tel quel
    const fromObj = extractFromObject(item);
    if (isImageUrl(fromObj)) return fromObj;

    // c) parfois l’objet est sous item.raw ou item.data
    const fromRaw = extractFromObject(item.raw || item.data);
    if (isImageUrl(fromRaw)) return fromRaw;

    return def;
  }

  // Attache un fallback propre sur <img>
  function attachImgFallback(img, placeholder='/static/img/no-image.jpg'){
    if (!img) return;
    img.addEventListener('error', function onErr(){
      if (img.dataset._fallbackApplied) return;
      img.dataset._fallbackApplied = '1';
      img.src = placeholder;
    }, { once:true });
  }

  global.WishMedia = { getBestImage, attachImgFallback };
})(window);
