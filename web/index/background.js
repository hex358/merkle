const canvas = document.getElementById("hero-bg");
const ctx = canvas.getContext("2d");

let width, height;
function resize() {
	width = canvas.width = canvas.offsetWidth;
	height = canvas.height = canvas.offsetHeight;
}
window.addEventListener("resize", resize);
resize();

let zoom = 1.2;
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

function animate() {
	ctx.clearRect(0, 0, width, height);
	ctx.save();
	ctx.translate(width / 2, height / 2);
	ctx.scale(zoom, zoom);
	ctx.translate(-width / 2, -height / 2);

	for (let i = 0; i < numParticles; i++) {
		for (let j = i + 1; j < numParticles; j++) {
			const dx = particles[i].x - particles[j].x;
			const dy = particles[i].y - particles[j].y;
			const dist = Math.sqrt(dx * dx + dy * dy);
			if (dist < 150) {
				const alpha = Math.min(1.0, (1 - dist / 150) * 1.5);
				ctx.beginPath();
				ctx.moveTo(particles[i].x, particles[i].y);
				ctx.lineTo(particles[j].x, particles[j].y);
				ctx.strokeStyle = `rgba(100, 50, 100, ${alpha})`;
				ctx.stroke();
			}
		}
	}

	for (let p of particles) {
		p.x += p.vx;
		p.y += p.vy;
		if (p.x < 0) p.x = width;
		if (p.x > width) p.x = 0;
		if (p.y < 0) p.y = height;
		if (p.y > height) p.y = 0;

		ctx.beginPath();
		ctx.arc(p.x, p.y, 3, 0, Math.PI * 2);
		ctx.fillStyle = "rgba(0, 70, 80, 1)";
		ctx.fill();
	}
	ctx.restore();
	requestAnimationFrame(animate);
}
animate();
