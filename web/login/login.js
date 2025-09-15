document.addEventListener("DOMContentLoaded", () => {
    const form = document.getElementById("loginForm");
    if (!form) return;

    const [usernameInput, passwordInput] = form.querySelectorAll("input");
    const errorBox = document.getElementById("loginError");

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

        // (1) Empty password
        if (!password) return showError("Password cannot be empty.");

        // (2) Empty username
        if (!username) return showError("Username cannot be empty.");

        // (3) Invalid special characters
        if (!isAlphanumeric(username) || !isAlphanumeric(password)) {
            return showError("Invalid characters used. Only letters, numbers, underscore are allowed.");
        }

        // (4) Username length > 10
        if (username.length > 10) return showError("Username must not exceed 10 characters.");

        // (5) Password length > 10
        if (password.length > 10) return showError("Password must not exceed 10 characters.");

        try {
            const res = await apiPost("/user_login", { username, password });

            if (res.status === "OK") {
                saveCredentials(username, password);
                window.location.href = "/";
            } else if (res.status === "ERR") {
                // (6) Incorrect credentials
                return showError("Incorrect username or password.");
            } else {
                return showError(res.message || "Unexpected server response.");
            }
        } catch (err) {
            console.error("Login error:", err);
            return showError("Server error. Please try again later.");
        }
    });

    const signupLink = document.getElementById("signupLink");
    if (signupLink) {
        signupLink.addEventListener("click", (e) => {
            e.preventDefault();
            window.location.href = "/signup";
        });
    }
});
