document.addEventListener("DOMContentLoaded", function () {
    const logo = document.getElementById("logoWishAdv");

    if (logo) {
        logo.style.cursor = "pointer"; // Change le curseur au survol
        logo.addEventListener("click", function () {
            window.location.href = "/index.html"; // Redirection vers index.html
        });

        // Optionnel : effet visuel au survol
        logo.addEventListener("mouseover", () => {
            logo.style.opacity = "0.7";
            logo.style.transition = "opacity 0.3s";
        });

        logo.addEventListener("mouseout", () => {
            logo.style.opacity = "1";
        });
    }
});

