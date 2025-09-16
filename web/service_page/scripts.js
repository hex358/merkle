// ================== SERVICE MANAGER ==================
const ServiceManager = {
    currentService: {
        name: "",
        description: "",
        rootHash: ""
    },

    loadServiceData: async function(serviceName = null) {
        if (serviceName) {
            this.currentService.name = serviceName;
            try { document.title = `Service: ${serviceName} — CertumTree`; } catch {}
        }
        if (!this.currentService.name) return;

        try {
            // --- fetch metadata ---
            const metaRes = await apiPost("/get_service_metadata", {
                service_name: this.currentService.name
            });

            if (metaRes?.status === "OK" && metaRes.metadata) {
                this.currentService.description = metaRes.metadata.description || "";
                renderServiceDescriptions(metaRes);
            } else {
                console.warn("No metadata (or ERR) for", this.currentService.name, metaRes);
                this.currentService.description = "";
                renderServiceDescriptions({ metadata: {} });
            }

            // --- fetch root hash ---
            const rootRes = await apiPost("/get_root_hash", {
                service_name: this.currentService.name
            });

            if (rootRes?.status === "OK" && rootRes.global_root) {
                this.currentService.rootHash = rootRes.global_root;
            } else {
                console.warn("No root hash (or ERR) for", this.currentService.name, rootRes);
                this.currentService.rootHash = "";
            }
        } catch (err) {
            console.error("Failed to load service data:", err);
        }

        this.updateUI();
    },

    updateUI: function() {
        const { name, description, rootHash } = this.currentService;
        const nameEl = document.getElementById("serviceName");
        const descEl = document.getElementById("serviceDescription");
        const rootEl = document.getElementById("rootHash");
        if (nameEl) nameEl.textContent = name || "";
        if (descEl) descEl.textContent = description || "";
        if (rootEl) rootEl.textContent = rootHash || "";
    }
};

// ================== VERIFICATION ==================
const VerificationManager = {
    currentDataType: "text",
    selectedFile: null,

    init: function() {
        this.setupEventListeners();
        this.setupFileHandling();
    },

    setupEventListeners: function() {
        document.querySelectorAll(".data-type-btn").forEach(btn => {
            btn.addEventListener("click", (e) => {
                this.switchDataType(e.target.dataset.type);
            });
        });

        const verifyBtn = document.getElementById("verifyBtn");
        if (verifyBtn) {
            verifyBtn.addEventListener("click", () => this.startVerification());
        }
    },

    setupFileHandling: function() {
        const dropZone = document.getElementById("fileDropZone");
        const fileInput = document.getElementById("fileInput");
        if (!dropZone || !fileInput) return;

        dropZone.addEventListener("click", () => fileInput.click());

        fileInput.addEventListener("change", (e) => {
            if (e.target.files.length > 0) this.handleFile(e.target.files[0]);
        });

        dropZone.addEventListener("dragover", (e) => {
            e.preventDefault();
            dropZone.classList.add("drag-over");
        });

        dropZone.addEventListener("dragleave", () => {
            dropZone.classList.remove("drag-over");
        });

        dropZone.addEventListener("drop", (e) => {
            e.preventDefault();
            dropZone.classList.remove("drag-over");
            if (e.dataTransfer.files.length > 0) this.handleFile(e.dataTransfer.files[0]);
        });
    },

    switchDataType: function(type) {
        document.querySelectorAll(".data-type-btn").forEach(btn => btn.classList.remove("active"));
        const activeBtn = document.querySelector(`[data-type="${type}"]`);
        if (activeBtn) activeBtn.classList.add("active");

        const textInput = document.getElementById("textInput");
        const fileDropZone = document.getElementById("fileDropZone");

        if (type === "text") {
            if (textInput) textInput.style.display = "block";
            if (fileDropZone) fileDropZone.classList.add("hidden");
            this.hideFileInfo();
        } else {
            if (textInput) textInput.style.display = "none";
            if (fileDropZone) fileDropZone.classList.remove("hidden");
        }

        this.currentDataType = type;
        this.hideResults();
    },

    handleFile: function(file) {
        const maxSize = 10 * 1024 * 1024;
        if (file.size > maxSize) {
            alert("File is too large. Maximum allowed size is 10 MB.");
            this.hideFileInfo();
            return;
        }

        this.selectedFile = file;
        const nameEl = document.getElementById("fileName");
        const sizeEl = document.getElementById("fileSize");
        const infoEl = document.getElementById("fileInfo");
        if (nameEl) nameEl.textContent = file.name;
        if (sizeEl) sizeEl.textContent = this.formatFileSize(file.size);
        if (infoEl) infoEl.style.display = "block";
        this.hideResults();
    },

    hideFileInfo: function() {
        const infoEl = document.getElementById("fileInfo");
        if (infoEl) infoEl.style.display = "none";
        this.selectedFile = null;
    },

    formatFileSize: function(bytes) {
        if (bytes === 0) return "0 Bytes";
        const k = 1024;
        const sizes = ["Bytes", "KB", "MB", "GB"];
        const i = Math.floor(Math.log(bytes) / Math.log(k));
        return `${parseFloat((bytes / Math.pow(k, i)).toFixed(2))} ${sizes[i]}`;
    },

    startVerification: function() {
        if (this.currentDataType === "text") {
            const textValue = (document.getElementById("textInput")?.value || "").trim();
            if (!textValue) return;
        } else if (this.currentDataType === "file") {
            if (!this.selectedFile) return;
        }

        this.showProcessing();
        this.hideResults();
        this.completeVerification().finally(() => this.hideProcessing());
    },

    showProcessing: function() {
        const btn = document.getElementById("verifyBtn");
        const spinner = document.getElementById("processingIndicator");
        if (btn) btn.disabled = true;
        if (spinner) spinner.style.display = "block";
    },

    hideProcessing: function() {
        const btn = document.getElementById("verifyBtn");
        const spinner = document.getElementById("processingIndicator");
        if (btn) btn.disabled = false;
        if (spinner) spinner.style.display = "none";
    },

    completeVerification: async function() {
        try {
            let blobBytes;
            if (this.currentDataType === "text") {
                const textValue = (document.getElementById("textInput")?.value || "").trim();
                blobBytes = window.kief(textValue);
            } else {
                const buffer = await this.selectedFile.arrayBuffer();
                blobBytes = window.kief(new Uint8Array(buffer));
            }

            const proofData = await apiPost("/check_blob", {
                service_name: ServiceManager.currentService.name,
                blob_hash: window.bytesToHex(blobBytes)
            });

            // Guard against ERR / missing bundle
            if (!proofData || proofData.status !== "OK" || !proofData.bundle) {
                console.warn("check_blob returned no bundle:", proofData);
                this.showResults(false, {
                    status: proofData?.status || "ERR",
                    message: proofData?.message || "Verification failed or service not found."
                });
                return;
            }

            const isVerified = window.client_check(proofData.bundle);
            this.showResults(isVerified, proofData);
        } catch (err) {
            console.error("Verification failed:", err);
            this.showResults(false, { status: "ERR", message: "Verification failed" });
        }
    },

    showResults: function(isVerified, proofData) {
        const resultsSection = document.getElementById("resultsSection");
        const resultStatus = document.getElementById("resultStatus");
        const resultSummary = document.getElementById("resultSummary");
        const proofDisplay = document.getElementById("proofDisplay");
        const downloadBtn = document.getElementById("downloadProofBtn");
        if (!resultsSection || !resultStatus || !resultSummary || !proofDisplay || !downloadBtn) return;

        if (isVerified) {
            resultStatus.textContent = "✅ Data Verified";
            resultStatus.className = "result-status verified";
            resultSummary.textContent =
                "Your data has been successfully verified against the blockchain records. The cryptographic proof confirms its integrity and authenticity.";
        } else {
            resultStatus.textContent = "❌ Data Compromised";
            resultStatus.className = "result-status compromised";
            resultSummary.textContent =
                (proofData?.message) || "Your data could not be verified. It may have been modified or is not registered.";
        }

        proofDisplay.textContent = JSON.stringify(proofData, null, 2);

        downloadBtn.style.display = "inline-block";
        downloadBtn.onclick = () => {
            const blob = new Blob([JSON.stringify(proofData, null, 2)], { type: "application/json" });
            const url = URL.createObjectURL(blob);
            const safeName = (ServiceManager.currentService.name || "service").replace(/[^\w\-]+/g, "_");
            const a = document.createElement("a");
            a.href = url;
            a.download = `proof_${safeName}_${Date.now()}.json`;
            a.click();
            URL.revokeObjectURL(url);
        };

        // If server returned global_root, reflect it (optional; /check_blob may not return it)
        if (proofData.global_root) {
            ServiceManager.currentService.rootHash = proofData.global_root;
            ServiceManager.updateUI();
        }

        resultsSection.style.display = "block";
       resultsSection.scrollIntoView({ behavior: "smooth", block: "start" });
    },

    hideResults: function() {
        const resultsSection = document.getElementById("resultsSection");
        if (resultsSection) resultsSection.style.display = "none";
    },

    generateDummyProof: function(ok) {
        return { ok, ts: Date.now() };
    }
};

// ================== API ==================
const API = {
    getRootHash: async function() {
        return apiPost("/get_root_hash", {
            service_name: ServiceManager.currentService.name
        });
    }
};

// ================== UTILS ==================
const Utils = {
    getServiceNameFromURL: function() {
        const url = new URL(window.location.href);

        if (url.searchParams.has("service")) {
            return url.searchParams.get("service");
        }

        const parts = url.pathname.split("/").filter(Boolean);
        if (parts.length >= 2 && parts[0] === "service") {
            return decodeURIComponent(parts[1]);
        }

        return "alice.myservice";
    },

    copyToClipboard: async function(text) {
        try {
            await navigator.clipboard.writeText(text);
            return true;
        } catch (err) {
            console.error("Failed to copy text: ", err);
            return false;
        }
    }
};

// ================== ROOT HASH MANAGER ==================
const RootHashManager = {
    intervalId: null,

    init: function() {
        const rootHashElement = document.getElementById("rootHash");
        if (rootHashElement) {
            rootHashElement.addEventListener("click", this.copyRootHash);
            rootHashElement.style.cursor = "pointer";
            rootHashElement.title = "Click to copy root hash";
        }
        this.startAutoRefresh();
    },

    copyRootHash: async function() {
        const rootHash = document.getElementById("rootHash")?.textContent || "";
        await Utils.copyToClipboard(rootHash);
    },

    startAutoRefresh: function() {
        this.refreshOnce();
        if (this.intervalId) clearInterval(this.intervalId);
        this.intervalId = setInterval(() => this.refreshOnce(), 2 * 60 * 1000);
    },

    refreshOnce: async function() {
        try {
            const res = await API.getRootHash();
            const newRootHash = res?.global_root;
            if (newRootHash) {
                ServiceManager.currentService.rootHash = newRootHash;
                ServiceManager.updateUI();
            }
        } catch (err) {
            console.error("Failed to refresh root hash:", err);
        }
    }
};

// ================== RENDER METADATA TABLE ==================
function renderServiceDescriptions(descriptions) {
    const container = document.getElementById("serviceDescriptions");
    if (!container) return;

    container.innerHTML = "";
    const meta = descriptions?.metadata || {};
    const keys = Object.keys(meta);

    if (keys.length === 0) {
        ServiceManager.currentService.description = "";
        ServiceManager.updateUI();
        return;
    }

    keys.forEach((key) => {
        const value = meta[key];

        if (key === "description") {
            ServiceManager.currentService.description = value;
            return;
        }

        const row = document.createElement("div");
        row.className = "description-row";

        const keyEl = document.createElement("div");
        keyEl.className = "description-key";
        keyEl.textContent = key;

        const valueEl = document.createElement("div");
        valueEl.className = "description-value";
        valueEl.textContent = value;

        row.appendChild(keyEl);
        row.appendChild(valueEl);
        container.appendChild(row);
    });

    ServiceManager.updateUI();
}

// ================== INIT ==================
document.addEventListener("DOMContentLoaded", async () => {
    const serviceName = Utils.getServiceNameFromURL();
    await ServiceManager.loadServiceData(serviceName);

    VerificationManager.init();
    RootHashManager.init();

    const textInput = document.getElementById("textInput");
    if (textInput) textInput.focus();
});

window.addEventListener("popstate", async () => {
    const serviceName = Utils.getServiceNameFromURL();
    await ServiceManager.loadServiceData(serviceName);
});
