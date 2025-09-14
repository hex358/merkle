const text = "No Fakes. Ever.";
const typewriterElement = document.getElementById("typewriter-text");

const GLYPHS = ['0', '1', '#', '@', '$', '%', '&', '*', '+', '=', '-', '.'];

let currentDisplay = Array(text.length).fill(' ');
let settledIndex = -1;
let animationFrame = 0;
let cyclesPerChar = 2;
let animationDelay = 20;
let finalShimmerComplete = false;

function initializeTypewriter() {
	const tempElement = document.createElement('span');
	tempElement.textContent = text;
	tempElement.style.visibility = 'hidden';
	tempElement.style.position = 'absolute';
	tempElement.className = typewriterElement.className;
	document.body.appendChild(tempElement);

	const finalWidth = tempElement.offsetWidth;
	document.body.removeChild(tempElement);

	typewriterElement.style.minWidth = `${finalWidth}px`;
	typewriterElement.style.display = 'inline-block';
	typewriterElement.textContent = currentDisplay.join('');
}

function animateMerkleChain() {
	if (settledIndex < text.length - 1) {
		const charIndex = Math.floor(animationFrame / cyclesPerChar);
		const cycleInChar = animationFrame % cyclesPerChar;

		if (charIndex < text.length) {
			for (let i = 0; i <= settledIndex; i++) currentDisplay[i] = text[i];

			if (cycleInChar < cyclesPerChar - 1) {
				currentDisplay[charIndex] = GLYPHS[Math.floor(Math.random() * GLYPHS.length)];
			} else {
				currentDisplay[charIndex] = text[charIndex];
				settledIndex = charIndex;
			}

			for (let i = charIndex + 1; i < text.length; i++) {
				currentDisplay[i] = GLYPHS[Math.floor(Math.random() * GLYPHS.length)];
			}

			typewriterElement.textContent = currentDisplay.join('');
			animationFrame++;
			setTimeout(animateMerkleChain, animationDelay);
		}
	} else if (!finalShimmerComplete) {
		setTimeout(performFinalShimmer, 300);
	}
}

function performFinalShimmer() {
	let shimmerFrames = 5;
	let currentShimmerFrame = 0;

	function shimmer() {
		if (currentShimmerFrame < shimmerFrames) {
			for (let i = 0; i < text.length; i++) {
				if (Math.random() > 0.3) {
					currentDisplay[i] = GLYPHS[Math.floor(Math.random() * GLYPHS.length)];
				} else {
					currentDisplay[i] = text[i];
				}
			}
			typewriterElement.textContent = currentDisplay.join('');
			currentShimmerFrame++;
			setTimeout(shimmer, 40);
		} else {
			typewriterElement.textContent = text;
			finalShimmerComplete = true;
			setTimeout(startFadeInAnimations, 200);
		}
	}
	shimmer();
}

function startFadeInAnimations() {
	const elements = [
		{ selector: '.hero-description', delay: 200 },
		{ selector: '.offerings-section', delay: 600 },
		{ selector: '.services-section', delay: 1000 },
		{ selector: '.footer', delay: 1200 }
	];

	elements.forEach(({ selector, delay }) => {
		setTimeout(() => {
			const element = document.querySelector(selector);
			if (element) element.classList.add('fade-in');
		}, delay);
	});

	setTimeout(() => {
		document.querySelectorAll('.service-card').forEach((card, i) => {
			setTimeout(() => card.classList.add('fade-in'), i * 100);
		});
	}, 1200);

	setTimeout(() => {
		document.querySelectorAll('.offering-card').forEach((card, i) => {
			setTimeout(() => card.classList.add('fade-in'), i * 150);
		});
	}, 800);
}

initializeTypewriter();
setTimeout(animateMerkleChain, 160);
