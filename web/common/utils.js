
function lerp(a, b, t) {
	return a + (b - a) * t;
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