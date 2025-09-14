document.querySelectorAll(".service-card").forEach(card => {
    card.addEventListener("click", async () => {
        console.log("helki");
        const id = card.dataset.id; // optional: pass some info
        try {
            const res = await fetch("/api/your-endpoint", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ serviceId: id })
            });
            const data = await res.json();
            console.log("Backend response:", data);
        } catch (err) {
            console.error("Request failed:", err);
        }
    });
});
