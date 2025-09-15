document.addEventListener("DOMContentLoaded", () => {
    const form = document.getElementById("signupForm");
    if (!form) return;

    const [usernameInput, passwordInput, confirmInput] = form.querySelectorAll("input");
    const errorBox = document.getElementById("loginError"); // reuse error box id

    function showError(msg) {
        errorBox.textContent = msg;
        errorBox.style.display = "block";
    }

    function clearError() {
        errorBox.textContent = "";
        errorBox.style.display = "none";
    }

    form.addEventListener("submit", async (e) => {
        e.preventDefault();
        clearError();

        const username = usernameInput.value.trim();
        const password = passwordInput.value;
        const confirm = confirmInput.value;

        // (1) Empty password
        if (!password) return showError("Password cannot be empty.");

        // (2) Empty username
        if (!username) return showError("Username cannot be empty.");

        // (3) Invalid characters (utils.js whitelist)
        if (!isAlphanumeric(username) || !isAlphanumeric(password)) {
            return showError("Only letters, numbers, and '-' are allowed.");
        }

        // (4) Username length > 10
        if (username.length > 10) return showError("Username must not exceed 10 characters.");

        // (5) Password length > 10
        if (password.length > 10) return showError("Password must not exceed 10 characters.");

        // (6) Confirm password check
        if (password !== confirm) return showError("Passwords do not match.");

        try {
            const res = await apiPost("/user_signup", { username, password });

            if (res.status === "OK") {
                saveCredentials(username, password);
                window.location.href = "/";
            } else if (res.status === "ERR") {
                return showError("Username is already taken.");
            } else {
                return showError(res.message || "Unexpected server response.");
            }
        } catch (err) {
            console.error("Signup error:", err);
            return showError("Server error. Please try again later.");
        }
    });

    const loginLink = document.getElementById("loginLink");
    if (loginLink) {
        loginLink.addEventListener("click", (e) => {
            e.preventDefault();
            window.location.href = "/login";
        });
    }
});
