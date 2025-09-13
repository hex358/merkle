const canvas = document.getElementById("hero-bg");
const ctx = canvas.getContext("2d");

const text = "No Fakes. Ever.";
const typewriterElement = document.getElementById("typewriter-text");

// Hash-like glyphs for the Merkle chain effect
const GLYPHS = ['0', '1', '#', '@', '$', '%', '&', '*', '+', '=', '-', '.'];

// Animation state
let currentDisplay = Array(text.length).fill(' ');
let settledIndex = -1;
let animationFrame = 0;
let cyclesPerChar = 2;
let animationDelay = 20;
let finalShimmerComplete = false;

// Set initial width based on final text
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

// Main Merkle chain animation
function animateMerkleChain() {
    if (settledIndex < text.length - 1) {
        // Determine which character we're currently settling
        const charIndex = Math.floor(animationFrame / cyclesPerChar);
        const cycleInChar = animationFrame % cyclesPerChar;
        
        if (charIndex < text.length) {
            // Update display array
            for (let i = 0; i <= settledIndex; i++) {
                currentDisplay[i] = text[i]; // Keep settled characters
            }
            
            // Jitter the current character being settled
            if (cycleInChar < cyclesPerChar - 1) {
                currentDisplay[charIndex] = GLYPHS[Math.floor(Math.random() * GLYPHS.length)];
            } else {
                // Settle this character
                currentDisplay[charIndex] = text[charIndex];
                settledIndex = charIndex;
            }
            
            // Jitter all unsettled characters to the right
            for (let i = charIndex + 1; i < text.length; i++) {
                currentDisplay[i] = GLYPHS[Math.floor(Math.random() * GLYPHS.length)];
            }
            
            typewriterElement.textContent = currentDisplay.join('');
            animationFrame++;
            setTimeout(animateMerkleChain, animationDelay);
        }
    } else if (!finalShimmerComplete) {
        // Final verification shimmer
        setTimeout(performFinalShimmer, 300);
        
    }
}

// Final shimmer effect - one last pass of noise then clean reveal
function performFinalShimmer() {
    let shimmerFrames = 5;
    let currentShimmerFrame = 0;
    
    function shimmer() {
        if (currentShimmerFrame < shimmerFrames) {
            // Create noise across the whole word
            for (let i = 0; i < text.length; i++) {
                if (Math.random() > 0.3) { // 70% chance of noise per character
                    currentDisplay[i] = GLYPHS[Math.floor(Math.random() * GLYPHS.length)];
                } else {
                    currentDisplay[i] = text[i];
                }
            }
            typewriterElement.textContent = currentDisplay.join('');
            currentShimmerFrame++;
            setTimeout(shimmer, 40);
        } else {
            // Final clean reveal
            typewriterElement.textContent = text;
            finalShimmerComplete = true;
            
            // Start fade-in animations after text is complete
            setTimeout(startFadeInAnimations, 200);
        }
    }
    
    shimmer();
}

// Fade-in animation system
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
            if (element) {
                element.classList.add('fade-in');
            }
        }, delay);
    });
    
    // Animate service cards individually with stagger
    setTimeout(() => {
        const serviceCards = document.querySelectorAll('.service-card');
        serviceCards.forEach((card, index) => {
            setTimeout(() => {
                card.classList.add('fade-in');
            }, index * 100);
        });
    }, 1200);
    
    // Animate offering cards with stagger
    setTimeout(() => {
        const offeringCards = document.querySelectorAll('.offering-card');
        offeringCards.forEach((card, index) => {
            setTimeout(() => {
                card.classList.add('fade-in');
            }, index * 150);
        });
    }, 800);
}

// Initialize and start the Merkle chain animation
initializeTypewriter();
setTimeout(animateMerkleChain, 160);

// Canvas background animation
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

// Smooth scroll implementation
let current = 0, target = 0;
const ease = 0.25;
let smoothWrapper, spacer;

function lerp(a, b, t) { return a + (b - a) * t; }
function px(n) { return isNaN(n) ? 0 : n; }

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