
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
