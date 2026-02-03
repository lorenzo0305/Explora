// --- Gestion panier ---
function getBasket() {
    return JSON.parse(localStorage.getItem("basket") || "[]");
}

function setBasket(basket) {
    localStorage.setItem("basket", JSON.stringify(basket));
    updateBasketCount();
    renderBasket();
}

function updateBasketCount() {
    const badge = document.getElementById("basketCount");
    if (badge) {
        const count = getBasket().length;
        badge.textContent = count;
        badge.hidden = count === 0;
    }
}

function renderBasket() {
    const container = document.getElementById("basketContent");
    if (!container) return;

    const basket = getBasket();
    container.innerHTML = basket.length === 0 ? "<p>Aucune activité ajoutée.</p>" : "";

    basket.forEach(act => {
        const row = document.createElement("div");
        row.className = "d-flex align-items-center mb-2 p-1";
        row.style.background = "#1e1e1e";
        row.style.borderRadius = "6px";
        row.style.color = "white";

        // ⚡ Image
        const img = document.createElement("img");
        img.src = act.image || "/static/img/no-image.jpg";
        img.alt = act.name;
        img.style.width = "50px";
        img.style.height = "50px";
        img.style.objectFit = "cover";
        img.style.borderRadius = "6px";
        img.style.marginRight = "10px";
        row.appendChild(img);

        // ⚡ Nom
        const span = document.createElement("span");
        span.textContent = act.name;
        row.appendChild(span);

        // ⚡ Bouton suppression
        const btn = document.createElement("button");
        btn.className = "btn btn-sm btn-danger ms-auto";
        btn.textContent = "❌";
        btn.addEventListener("click", () => {
            setBasket(getBasket().filter(a => a.id !== act.id));
        });
        row.appendChild(btn);

        container.appendChild(row);
    });
}

// --- Ajouter au panier ---
function addToBasket(item) {
    let basket = getBasket();
    if (!basket.find(a => a.id === item.id)) {
        basket.push(item);  // ⚡ Important : item doit contenir 'image'
        setBasket(basket);
        alert(item.name + " a été ajouté au panier !");
    }
}

// --- Recherche ---
document.getElementById("search").addEventListener("input", async function () {
    const query = this.value.trim();
    const resultsList = document.getElementById("results");
    resultsList.innerHTML = "";

    if (query.length < 1) return;

    try {
        const res = await fetch(`/search?query=${encodeURIComponent(query)}`);
        const data = await res.json();

        if (data.length === 0) {
            resultsList.innerHTML = "<li class='list-group-item'>Aucun résultat</li>";
            return;
        }

        data.forEach(item => {
            const li = document.createElement("li");
            li.className = "list-group-item d-flex align-items-center";

            const img = document.createElement("img");
            img.src = item.image || "/static/img/no-image.jpg";
            img.alt = item.name;
            img.style.width = "60px";
            img.style.height = "60px";
            img.style.objectFit = "cover";
            img.style.borderRadius = "8px";
            img.style.marginRight = "10px";
            li.appendChild(img);

            const span = document.createElement("span");
            span.textContent = item.name;
            span.style.flexGrow = "1";
            li.appendChild(span);

            const btn = document.createElement("button");
            btn.className = "btn btn-sm btn-success";
            btn.textContent = "➕";
            btn.addEventListener("click", () => addToBasket(item));
            li.appendChild(btn);

            resultsList.appendChild(li);
        });
    } catch (error) {
        console.error("Erreur lors de la recherche :", error);
        resultsList.innerHTML = "<li class='list-group-item text-danger'>Erreur de chargement</li>";
    }
});

// --- Initialisation ---
document.addEventListener("DOMContentLoaded", () => {
    updateBasketCount();
    renderBasket();
});
