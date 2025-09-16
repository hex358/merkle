document.addEventListener("DOMContentLoaded", () => {
  const target = document.querySelector(".rightHeader");
  if (target) {
    target.innerHTML = `
      <ul class="nav-menu">
        <li><a href="services" class="nav-link-bar">Service List</a></li>
        <li><a href="dashboard" class="nav-link-bar">Access API</a></li>
        <li><button type="button" class="btnflat btnflat-secondary" id="loginBtn">Login</button></li>
      </ul>
    `;

    // Redirect on click
    const loginBtn = target.querySelector("#loginBtn");
    if (loginBtn) {
      loginBtn.addEventListener("click", () => {
        window.location.assign("/login");
      });
    }
  }
});
