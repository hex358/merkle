
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



const BACKEND_URL = "";
// ================== API HELPERS ==================
// ================== ROUTE GUARD ==================
document.addEventListener("DOMContentLoaded", () => {
    const creds = getCredentials();
    const path = window.location.pathname;

    const loggedIn = !!creds;
    const guestOnly = ["/login", "/signup"];
    const protectedOnly = ["/dashboard"];

    if (!loggedIn && protectedOnly.includes(path)) {
        // not logged in but trying to access a protected page
        window.location.href = "/login";
    }
    if (loggedIn && guestOnly.includes(path)) {
        // already logged in but trying to access login/signup
        window.location.href = "/dashboard";
    }
});




// Unified request helper so GET/POST behave identically
async function apiRequest(method, path, data = null, opts = {}) {
    const isGet = method === "GET";
    const qs = isGet && data ? `?${new URLSearchParams(data)}` : "";
    const url = `${BACKEND_URL}${path}${qs}`;

    const init = {
        method,
        headers: {
            "Accept": "application/json",
            ...(isGet ? {} : { "Content-Type": "application/json" }),
            ...(opts.headers || {})
        },
        body: isGet ? undefined : JSON.stringify(data ?? {}),
        // keep the request alive even if a navigation starts
        keepalive: true,
        // send/accept cookies if you're on a different origin
        credentials: BACKEND_URL ? "include" : "same-origin",
        cache: "no-store",
        redirect: "follow",
        signal: opts.signal
    };

    const res = await fetch(url, init);
    let json = null;
    try { json = await res.json(); } catch { json = {}; } // always consume body
    console.log(path, data);
    console.log(json);
    return json;
}

const apiPost = (path, data, opts) => apiRequest("POST", path, data, opts);
const apiGet  = (path, params, opts) => apiRequest("GET", path, params, opts);





// ================== AUTH HELPERS ==================
const AUTH_KEY = "certumtree_auth"; // where we store creds in localStorage

function saveCredentials(username, password) {
    localStorage.setItem(AUTH_KEY, JSON.stringify({ username, password }));
}

function clearCredentials() {
    localStorage.removeItem(AUTH_KEY);
    const protectedOnly = ["/dashboard"];
    const path = window.location.pathname;

    if (protectedOnly.includes(path)) {
        // not logged in but trying to access a protected page
        window.location.href = "/login";
    }
}

function getCredentials() {
    const raw = localStorage.getItem(AUTH_KEY);
    console.log(raw);
    return raw ? JSON.parse(raw) : null;
}

async function tryAutoLogin() {
    const creds = getCredentials();
    console.log("TRYING");
    if (!creds) return false; // nothing saved

    try {
        const res = await apiPost("/user_login", creds);
        if (res.status === "OK") {
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
    window.location.href = "/";
}
