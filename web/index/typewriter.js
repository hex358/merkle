// Clear any existing timeouts
if (window.typewriterTimeouts) {
    window.typewriterTimeouts.forEach(timeout => clearTimeout(timeout));
}
window.typewriterTimeouts = [];

const text = "No Fakes. Ever.";
const typewriterElement = document.getElementById("typewriter-text");

if (!typewriterElement) {
    console.warn("Typewriter element not found");
} else {
    const GLYPHS = ['0', '1', '#', '@', '$', '%', '&', '*', '+', '=', '-', '.'];

    let currentDisplay = Array(text.length).fill(' ');
    let settledIndex = -1;
    let animationFrame = 0;
    let cyclesPerChar = 2;
    let animationDelay = 20;
    let finalShimmerComplete = false;
    let isAnimating = false;

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
        if (!isAnimating) return;
        
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
                
                const timeout = setTimeout(animateMerkleChain, animationDelay);
                window.typewriterTimeouts.push(timeout);
            }
        } else if (!finalShimmerComplete) {
            const timeout = setTimeout(performFinalShimmer, 300);
            window.typewriterTimeouts.push(timeout);
        }
    }

    function performFinalShimmer() {
        if (!isAnimating) return;
        
        let shimmerFrames = 5;
        let currentShimmerFrame = 0;

        function shimmer() {
            if (!isAnimating) return;
            
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
                
                const timeout = setTimeout(shimmer, 40);
                window.typewriterTimeouts.push(timeout);
            } else {
                typewriterElement.textContent = text;
                finalShimmerComplete = true;
                isAnimating = false;
                
                const timeout = setTimeout(startFadeInAnimations, 200);
                window.typewriterTimeouts.push(timeout);
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
            const timeout = setTimeout(() => {
                const element = document.querySelector(selector);
                if (element) element.classList.add('fade-in');
            }, delay);
            window.typewriterTimeouts.push(timeout);
        });

        const timeout1 = setTimeout(() => {
            document.querySelectorAll('.service-card').forEach((card, i) => {
                const timeout = setTimeout(() => card.classList.add('fade-in'), i * 100);
                window.typewriterTimeouts.push(timeout);
            });
        }, 1200);
        window.typewriterTimeouts.push(timeout1);

        const timeout2 = setTimeout(() => {
            document.querySelectorAll('.offering-card').forEach((card, i) => {
                const timeout = setTimeout(() => card.classList.add('fade-in'), i * 150);
                window.typewriterTimeouts.push(timeout);
            });
        }, 800);
        window.typewriterTimeouts.push(timeout2);
    }

    // Initialize and start
    initializeTypewriter();
    isAnimating = true;
    const startTimeout = setTimeout(animateMerkleChain, 160);
    window.typewriterTimeouts.push(startTimeout);
}