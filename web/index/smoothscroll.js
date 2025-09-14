/* Smooth-scroll (optimized measure) */
(function () {
	// cleanup prior instance
	if (window.smoothScrollCleanup) window.smoothScrollCleanup();

	let current = 0, target = 0;
	const ease = 0.25;

	// Cached DOM refs (queried once)
	const DOM = {
		header: null,
		main: null,
		footer: null,
		canvas: null,
		wrapper: null,
		spacer: null,
	};

	let rafId = null;
	let started = false;
	let ro = null;          // ResizeObserver
	let mo = null;          // MutationObserver (optional, for dynamic DOM)
	let measureQueued = false;
	let lastSpacerHeight = -1;

	// -------- measure (fast path) --------
	function measureNow() {
		if (!DOM.wrapper || !DOM.spacer) return;

		// Robust & cheap: measure total content height of the wrapper.
		// Triggers layout once; avoid repeating if unchanged.
		const h = DOM.wrapper.scrollHeight;
		if (h !== lastSpacerHeight) {
			lastSpacerHeight = h;
			DOM.spacer.style.height = h + "px";
		}
	}

	// Coalesce multiple triggers to one layout read/write per frame
	function scheduleMeasure() {
		if (measureQueued) return;
		measureQueued = true;
		requestAnimationFrame(() => {
			measureQueued = false;
			measureUsingNodes();
		});
	}

	// Optional: if you insist on header/main/footer math, use cached nodes:
	// (Kept for reference; not used because scrollHeight is faster & safer.)
	function measureUsingNodes() {
		if (!DOM.spacer) return;
		const headerH = DOM.header ? DOM.header.offsetHeight : 0;
		const mainH   = DOM.main   ? DOM.main.offsetHeight   : 0;
		const footerH = DOM.footer ? DOM.footer.offsetHeight : 0;

		// cache footer margins once (they rarely change)
		let footerMT = 0, footerMB = 0;
		if (DOM.footer && measureUsingNodes._footerMargins == null) {
			const cs = getComputedStyle(DOM.footer);
			measureUsingNodes._footerMargins = {
				mt: parseFloat(cs.marginTop) || 0,
				mb: parseFloat(cs.marginBottom) || 0,
			};
		}
		if (measureUsingNodes._footerMargins) {
			footerMT = measureUsingNodes._footerMargins.mt;
			footerMB = measureUsingNodes._footerMargins.mb;
		}

		const spacerH = Math.max(0, (mainH + footerH + footerMT + footerMB) - headerH);
		if (spacerH !== lastSpacerHeight) {
			lastSpacerHeight = spacerH;
			DOM.spacer.style.height = spacerH + "px";
		}
	}

	// -------- raf loop --------
	function raf() {
		target = window.scrollY || document.documentElement.scrollTop || 0;
		current += (target - current) * ease;

		if (DOM.wrapper) {
			DOM.wrapper.style.transform = `translate3d(0, ${-current}px, 0)`;
		}
		if (DOM.canvas) {
			DOM.canvas.style.transform = `translateY(${current * 0.5}px)`;
		}
		rafId = requestAnimationFrame(raf);
	}

	// -------- start/init --------
	function start() {
		if (started) return;
		started = true;

		// initial measure
		measureUsingNodes();

		// window resizes → re-measure (coalesced)
		window.addEventListener("resize", scheduleMeasure);

		// observe wrapper box size changes (fonts/images/content flow)
		try {
			ro = new ResizeObserver(scheduleMeasure);
			ro.observe(DOM.wrapper);
		} catch (_) {}

		// (optional) observe DOM mutations if content is injected/removed dynamically
		try {
			mo = new MutationObserver(scheduleMeasure);
			mo.observe(DOM.wrapper, { childList: true, subtree: true });
		} catch (_) {}

		rafId = requestAnimationFrame(raf);
	}

	function tryInit() {
		// Cache all selectors ONCE
		DOM.wrapper = DOM.wrapper || document.querySelector(".scroll-wrapper");
		DOM.spacer  = DOM.spacer  || document.getElementById("scroll-spacer");
		DOM.header  = DOM.header  || document.querySelector(".header");
		DOM.main    = DOM.main    || document.querySelector("main");
		DOM.footer  = DOM.footer  || document.querySelector("footer");
		DOM.canvas  = DOM.canvas  || document.getElementById("hero-bg");

		if (DOM.wrapper && DOM.spacer) {
			start();
		} else {
			// Retry next frame until DOM is ready
			requestAnimationFrame(tryInit);
		}
	}

	if (document.readyState === "complete") {
		tryInit();
	} else {
		document.addEventListener("DOMContentLoaded", tryInit);
		window.addEventListener("load", tryInit); // ensure after fonts/images
	}

	// -------- cleanup --------
	window.smoothScrollCleanup = function () {
		if (rafId) { cancelAnimationFrame(rafId); rafId = null; }
		window.removeEventListener("resize", scheduleMeasure);
		if (ro) { try { ro.disconnect(); } catch(_){} ro = null; }
		if (mo) { try { mo.disconnect(); } catch(_){} mo = null; }
		started = false;
		lastSpacerHeight = -1;
	};

})();
