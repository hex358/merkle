

// --- core hashing helpers ---

function kief(...args) {
    const ctx = blake2bInit(8); // 16-byte digest
    for (let a of args) {
        if (a instanceof Uint8Array) {
            blake2bUpdate(ctx, a);
        } else if (typeof a === "string") {
            blake2bUpdate(ctx, new TextEncoder().encode(a));
        } else {
            throw new Error("Unsupported type for kief arg: " + typeof a);
        }
    }
    return blake2bFinal(ctx); // Uint8Array
}

function hexToBytes(hex) {
    if (hex.startsWith("0x")) hex = hex.slice(2);
    const out = new Uint8Array(hex.length / 2);
    for (let i = 0; i < out.length; i++) {
        out[i] = parseInt(hex.substr(i * 2, 2), 16);
    }
    return out;
}

function bytesToHex(arr) {
    return Array.from(arr, b => b.toString(16).padStart(2, "0")).join("");
}

function bytesEqual(a, b) {
    if (a.length !== b.length) return false;
    for (let i = 0; i < a.length; i++) {
        if (a[i] !== b[i]) return false;
    }
    return true;
}

// --- verification helper (port of Python client_check) ---
function client_check(bundle) {
    if (bundle.status !== 1) {
        return false;
    }

    const expected = hexToBytes(bundle.global_root);
    let h = hexToBytes(bundle.h);

    for (const [sibHex, wasLeft] of bundle.proof) {
        const sib = hexToBytes(sibHex);
        h = wasLeft ? kief(sib, h) : kief(h, sib);
    }

    const left_roots = bundle.left_roots || [];
    const right_roots = bundle.right_roots || bundle.other_roots || [];

    if (left_roots.length > 0) {
        let acc = hexToBytes(left_roots[0]);
        for (let i = 1; i < left_roots.length; i++) {
            acc = kief(acc, hexToBytes(left_roots[i]));
        }
        h = kief(acc, h);
    }

    for (const r of right_roots) {
        h = kief(h, hexToBytes(r));
    }

    return bytesEqual(h, expected);
}
