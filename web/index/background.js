// Cleanup old instance
if (window.backgroundAnimationCleanup) window.backgroundAnimationCleanup();

const canvas = document.getElementById("hero-bg");
const ctx = canvas.getContext("2d");

let width, height;
let animationId = null;
let isAnimating = false;

function resize() {
	width = canvas.width = canvas.offsetWidth;
	height = canvas.height = canvas.offsetHeight;
}
const resizeHandler = resize;
window.addEventListener("resize", resizeHandler);
resize();

const zoom = 1.2;
const numParticles = 60;
const particles = [];

for (let i = 0; i < numParticles; i++) {
	particles.push({
		x: Math.random() * width,
		y: Math.random() * height,
		vx: (Math.random() - 0.5) * 0.2,
		vy: (Math.random() - 0.5) * 0.2
	});
}

const maxDist2 = 150 * 150;

function animate() {
	if (!isAnimating) return; // stop drawing when paused

	ctx.clearRect(0, 0, width, height);
	ctx.save();
	ctx.translate(width / 2, height / 2);
	ctx.scale(zoom, zoom);
	ctx.translate(-width / 2, -height / 2);

	// Connections
	for (let i = 0; i < numParticles; i++) {
		const pi = particles[i];
		for (let j = i + 1; j < numParticles; j++) {
			const pj = particles[j];
			const dx = pi.x - pj.x;
			const dy = pi.y - pj.y;
			const dist2 = dx * dx + dy * dy;

			if (dist2 < maxDist2) {
				const dist = Math.sqrt(dist2);
				const alpha = Math.min(1.0, (1 - dist / 150) * 1.5);

				ctx.beginPath();
				ctx.moveTo(pi.x, pi.y);
				ctx.lineTo(pj.x, pj.y);
				ctx.strokeStyle = `rgba(100,50,100,${alpha})`;
				ctx.stroke();
			}
		}
	}

	// Particles
	ctx.fillStyle = "rgba(0,70,80,1)";
	for (let i = 0; i < numParticles; i++) {
		const p = particles[i];
		p.x += p.vx;
		p.y += p.vy;

		if (p.x < 0) p.x = width;
		else if (p.x > width) p.x = 0;
		if (p.y < 0) p.y = height;
		else if (p.y > height) p.y = 0;

		ctx.beginPath();
		ctx.arc(p.x, p.y, 3, 0, Math.PI * 2);
		ctx.fill();
	}

	ctx.restore();
	animationId = requestAnimationFrame(animate);
}

// Start animation
function startAnimation() {
	if (!isAnimating) {
		isAnimating = true;
		animate();
	}
}

// Stop animation
function stopAnimation() {
	isAnimating = false;
	if (animationId) {
		cancelAnimationFrame(animationId);
		animationId = null;
	}
}

// Auto toggle based on scroll
window.addEventListener("scroll", () => {
	if (window.scrollY > 500) {
		stopAnimation();
	} else {
		startAnimation();
	}
});

// Start initially
startAnimation();

// Cleanup
window.backgroundAnimationCleanup = function () {
	stopAnimation();
	window.removeEventListener("resize", resizeHandler);
	window.removeEventListener("scroll", scrollHandler);
};
