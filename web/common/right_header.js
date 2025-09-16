document.addEventListener("DOMContentLoaded", () => {
  const target = document.querySelector(".rightHeader");
  if (target) {
    if (getCredentials() !== null) {
      const username = getCredentials().username;
      target.innerHTML = `
        <ul class="nav-menu">
          <li><a href="/services" class="nav-link-bar">Search Services</a></li>
          <li><a href="/dashboard" class="nav-link-bar">API Dashboard</a></li>
          <li><button type="button" class="btnflat btnflat-secondary" id="logoutBtn">Logout</button></li>
          <li><span class="nav-username">${username}</span></li>
        </ul>
        <style>
.nav-username {
  color: rgba(120,120,120,255);          /* slightly darker than white */
  font-weight: normal;     /* not bold */
}


        </style>
      `;
    } else {
      target.innerHTML = `
        <ul class="nav-menu">
          <li><a href="/services" class="nav-link-bar">Search Services</a></li>
          <li><button type="button" class="btnflat btnflat-secondary" id="loginBtn">Login</button></li>
        </ul>
      `;
    }

    // Redirect on click
    const loginBtn = target.querySelector("#loginBtn");
    if (loginBtn) {
      loginBtn.addEventListener("click", () => {
        window.location.assign("/login");
      });
    }

    const logoutBtn = target.querySelector("#logoutBtn");
    if (logoutBtn) {
      logoutBtn.addEventListener("click", () => {
        window.location.assign("/");
        logout();
      });
    }
  }
});
