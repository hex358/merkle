
// ================== STATE ==================
let services = {}; // local cache synced with backend
let currentService = null;
let tokenVisible = false;


// ================== INIT ==================
async function init() {
    await loadServicesFromBackend();
    renderServiceList();
    updateMainContent();
}

// ================== LOAD FROM BACKEND ==================
async function loadServicesFromBackend() {
    try {
        const response = await apiGet("/list_services", {});
        if (response.status === "OK") {
            response.services.forEach(svc => {
                services[svc.service_name] = {
                    token: svc.token,         // may be hidden or real
                    metadata: svc.metadata
                };
            });
        }
    } catch (err) {
        console.error("Failed to load services:", err);
    }
}



// ================== SERVICE LIST ==================
function renderServiceList() {
    const serviceList = document.getElementById('serviceList');
    serviceList.innerHTML = '';

    Object.keys(services).forEach(serviceName => {
        const serviceItem = document.createElement('div');
        serviceItem.className = `service-item ${currentService === serviceName ? 'active' : ''}`;
        serviceItem.onclick = () => selectService(serviceName);
        serviceItem.innerHTML = `<div><div class="service-name">${serviceName}</div></div>`;
        serviceList.appendChild(serviceItem);
    });
}

async function selectService(serviceName) {
    currentService = serviceName;
    tokenVisible = false;

    // fetch metadata from backend
    const response = await apiGet("/get_service_metadata", { service_name: serviceName });
    if (response.status === "OK") {
        services[serviceName].metadata = response.metadata;
    }

    renderServiceList();
    updateMainContent();
    renderMetadata();

    setTimeout(() => {
        const showTokenBtn = document.querySelector('button[onclick="toggleToken()"]');
        if (showTokenBtn) showTokenBtn.textContent = 'Show Token';
    }, 0);
}

// ================== MAIN CONTENT ==================
function updateMainContent() {
    const emptyState = document.getElementById('emptyState');
    const serviceDetails = document.getElementById('serviceDetails');
    const tokenDisplay = document.getElementById('tokenDisplay');

    if (Object.keys(services).length === 0) {
        emptyState.style.display = 'block';
        serviceDetails.classList.remove('active');
    } else if (currentService) {
        emptyState.style.display = 'none';
        serviceDetails.classList.add('active');

        document.getElementById('serviceTitle').textContent = currentService;
        document.getElementById('serviceSubtitle').innerHTML = `Manage <i>${currentService}</i> API configuration`;

        tokenDisplay.textContent = services[currentService].token;
        tokenDisplay.classList.toggle('visible', tokenVisible);
    } else {
        emptyState.style.display = 'block';
        serviceDetails.classList.remove('active');
    }
}

// ================== SERVICE CREATION ==================
function showNewServiceModal() {
    document.getElementById('newServiceModal').classList.add('active');
    document.getElementById('newServiceName').value = '';
    document.getElementById('serviceNameError').style.display = 'none';
}

async function createService() {
    tokenVisible = false;
    document.getElementById('show-tok').textContent = "Show Token";

    const serviceName = document.getElementById('newServiceName').value.trim();
    const errorDiv = document.getElementById('serviceNameError');
    if (!serviceName) return;

    const response = await apiPost("/register_service", {
        service_name: serviceName,
        metadata: {
            "description": "API service for " + serviceName,
            "environment": "development",
            "rate_limit": "1000"
        }
    });

    if (response.status === "ERR") {
        errorDiv.style.display = 'block';
        return;
    }

    services[serviceName] = {
        token: response.service_token,
        metadata: {
            "description": "API service for " + serviceName,
            "environment": "development",
            "rate_limit": "1000"
        }
    };

    currentService = serviceName;
    closeModal('newServiceModal');
    renderServiceList();
    updateMainContent();
    renderMetadata();
}

// ================== TOKEN MGMT ==================
async function toggleToken(event) {
    tokenVisible = !tokenVisible;
    const tokenDisplay = document.getElementById('tokenDisplay');
    const button = event.target;

    if (tokenVisible && currentService) {
        const serviceAtRequest = currentService; // snapshot
        try {
            const response = await apiPost("/get_token", { "service_name": serviceAtRequest, "password": "dummy", "username": "dummy" });
            if (response.status === "OK" && currentService === serviceAtRequest) {
                services[serviceAtRequest].token = response.token;
                tokenDisplay.textContent = response.token;
            } else if (currentService !== serviceAtRequest) {
                console.log("⚠️ Ignoring token response for stale service:", serviceAtRequest);
            } else {
                console.error("❌ Failed to fetch token:", response.message);
                tokenVisible = false;
            }
        } catch (err) {
            console.error("❌ Error fetching token:", err);
            tokenVisible = false;
        }
    }

    tokenDisplay.classList.toggle('visible', tokenVisible);
    button.textContent = tokenVisible ? 'Hide Token' : 'Show Token';
}


async function regenerateToken() {
    if (currentService) {
        const response = await apiPost("/update_token", {
            username: "dummy", password: "dummy"
        });
        if (response.status === "OK") {
            services[currentService].token = response.token;
            updateMainContent();
            if (tokenVisible) {
                document.getElementById('tokenDisplay').textContent = services[currentService].token;
            }
        }
    }
}

function copyToken() {
    if (currentService && tokenVisible) {
        navigator.clipboard.writeText(services[currentService].token);
    }
}

// ================== METADATA ==================
function renderMetadata() {
    if (!currentService) return;

    const metadataGrid = document.getElementById('metadataGrid');
    metadataGrid.innerHTML = '';
    const metadata = services[currentService].metadata;

    Object.entries(metadata).forEach(([key, value]) => {
        addMetadataFieldToDOM(key, value, 'String');
    });
}

function addMetadataField() {
    addMetadataFieldToDOM('', '', 'String');
}

function autoResize(el) {
    el.style.height = "auto";
    el.style.height = (el.scrollHeight) + "px";
}

function addMetadataFieldToDOM(key = '', value = '', type = 'String') {
    const metadataGrid = document.getElementById('metadataGrid');
    const fieldDiv = document.createElement('div');
    fieldDiv.className = 'metadata-field';

    fieldDiv.innerHTML = `
        <input type="text" class="field-input" value="${key}" placeholder="Field name" onchange="updateMetadata()">
        <textarea class="field-textarea" placeholder="Field value"
            oninput="autoResize(this); updateMetadata()">${value}</textarea>
        <button class="btn btn-ghost-danger btn-small" onclick="removeMetadataField(this)">Remove</button>
    `;

    metadataGrid.appendChild(fieldDiv);
    const ta = fieldDiv.querySelector('.field-textarea');
    ta.value = value ?? '';
    setTimeout(() => { autoResize(ta); }, 6);
}

function removeMetadataField(button) {
    button.parentElement.remove();
    updateMetadata();
}

function updateMetadata() {
    if (!currentService) return;

    const fields = document.querySelectorAll('.metadata-field');
    const newMetadata = {};

    fields.forEach(field => {
        const inputs = field.querySelectorAll('.field-input');
        const textarea = field.querySelector('.field-textarea');

        const key = inputs[0].value.trim();
        const value = textarea.value.trim();
        if (key) newMetadata[key] = value;
    });

    services[currentService].metadata = newMetadata;
}

async function saveMetadata() {
    updateMetadata();
    if (!currentService) return;

    const response = await apiPost("/update_service", {
        "service_name": "currentService",
        "username": "dummy",
        "password": "dummy",
        "metadata": services[currentService].metadata
    });

    if (response.status === "OK") {
        console.log("✅ Metadata saved");
    } else {
        console.error("❌ Failed to update metadata:", response.message);
    }
}

// ================== DELETION ==================
function showDeleteModal() {
    if (currentService) {
        document.getElementById('deleteServiceName').textContent = currentService;
        document.getElementById('deleteModal').classList.add('active');
    }
}

async function confirmDelete() {
    if (currentService) {
        const response = await apiPost("/delete_service", { service_name: currentService });
        if (response.status === "OK") {
            delete services[currentService];
            currentService = null;
            closeModal('deleteModal');
            renderServiceList();
            updateMainContent();
        }
    }
}

// ================== MODALS ==================
function closeModal(modalId) {
    document.getElementById(modalId).classList.remove('active');
}

document.addEventListener('keydown', function(e) {
    if (e.key === 'Escape') {
        const activeModal = document.querySelector('.modal-overlay.active');
        if (activeModal) activeModal.classList.remove('active');
    }
});

document.querySelectorAll('.modal-overlay').forEach(overlay => {
    overlay.addEventListener('click', function(e) {
        if (e.target === overlay) overlay.classList.remove('active');
    });
});

// ================== START ==================
init();
