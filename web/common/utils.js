
function lerp(a, b, t) {
	return a + (b - a) * t;
}


function isAlphanumeric(str) {
    if (!str) return false;
    for (let i = 0; i < str.length; i++) {
        const code = str.charCodeAt(i);
        // 0-9
        if (code >= 48 && code <= 57) continue;
        // A-Z
        if (code >= 65 && code <= 90) continue;
        // a-z
        if (code >= 97 && code <= 122) continue;
        // -
        if (code === 45) continue;
        if (code === 95) continue;
        return false; // invalid char found
    }
    return true;
}

function px(n) {
	return isNaN(n) ? 0 : n;
}

function randomChoice(arr) {
	return arr[Math.floor(Math.random() * arr.length)];
}

function delay(fn, ms) {
	return setTimeout(fn, ms);
}

  document.addEventListener("DOMContentLoaded", () => {
    const brand = document.querySelector(".nav-brand");
    if (brand) {
      brand.style.cursor = "pointer"; // show it's clickable
      brand.addEventListener("click", () => {
        window.location.href = "/"; // go to homepage
      });
    }
  });



const BACKEND_URL = "http://localhost:8000";
// ================== API HELPERS ==================
async function apiPost(path, data) {
    const res = await fetch(`${BACKEND_URL}${path}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(data)
    });
    return res.json();
}

async function apiGet(path, params) {
    const res = await fetch(`${BACKEND_URL}${path}?${new URLSearchParams(params)}`);
    return res.json();
}



// ================== AUTH HELPERS ==================
const AUTH_KEY = "certumtree_auth"; // where we store creds in localStorage

function saveCredentials(username, password) {
    localStorage.setItem(AUTH_KEY, JSON.stringify({ username, password }));
}

function clearCredentials() {
    localStorage.removeItem(AUTH_KEY);
}

function getCredentials() {
    const raw = localStorage.getItem(AUTH_KEY);
    return raw ? JSON.parse(raw) : null;
}

async function tryAutoLogin() {
    const creds = getCredentials();
    if (!creds) return false; // nothing saved

    try {
        const res = await apiPost("/user_login", creds);
        if (res.success) {
            console.log("Auto-login successful:", res.user);
            return res.user;
        } else {
            console.warn("Auto-login failed");
            clearCredentials();
            return false;
        }
    } catch (err) {
        console.error("Auto-login error:", err);
        return false;
    }
}

// Run auto-login on page load
document.addEventListener("DOMContentLoaded", async () => {
    const user = await tryAutoLogin();
    if (user) {
        // Example: update UI
        const info = document.querySelector(".user-info");
        if (info) info.textContent = user.email || user.username;
    }
});

async function logout() {
    clearCredentials();
    //await apiPost("/logout", {}); // optional, if backend supports
    window.location.href = "/login";
}
