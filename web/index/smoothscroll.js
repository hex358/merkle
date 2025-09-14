let current = 0, target = 0;
const ease = 0.25;
let smoothWrapper, spacer;

function measureAndSyncHeight() {
	const header = document.querySelector('.header');
	const main = document.querySelector('main');
	const footer = document.querySelector('footer');

	const headerH = header ? header.offsetHeight : 0;
	const mainH = main ? main.offsetHeight : 0;
	const footerH = footer ? footer.offsetHeight : 0;

	let footerMT = 0, footerMB = 0;
	if (footer) {
		const cs = getComputedStyle(footer);
		footerMT = px(parseFloat(cs.marginTop));
		footerMB = px(parseFloat(cs.marginBottom));
	}

	const contentHeight = mainH + footerH + footerMT + footerMB;
	const spacerHeight = Math.max(0, contentHeight - headerH);
	spacer.style.height = `${spacerHeight}px`;
}

function raf() {
	target = window.scrollY;
	current = lerp(current, target, ease);
	smoothWrapper.style.transform = `translate3d(0, ${-current}px, 0)`;
	canvas.style.transform = `translateY(${current * 0.5}px)`;
	requestAnimationFrame(raf);
}

window.addEventListener("load", () => {
	smoothWrapper = document.querySelector(".scroll-wrapper");
	spacer = document.getElementById("scroll-spacer");
	measureAndSyncHeight();
	window.addEventListener("resize", measureAndSyncHeight);
	requestAnimationFrame(raf);
});
