// ================== STATE ==================
let services = {}; // local cache synced with backend
let currentService = null;
let tokenVisible = false;


// ================== INIT ==================
async function init() {
    await loadServicesFromBackend();
    renderServiceList();

    // restore last active service if possible
    const savedService = localStorage.getItem("activeService");
    if (savedService && services[savedService]) {
        await selectService(savedService, { skipSave: true });
    } else {
        updateMainContent();
    }
}

// ================== LOAD FROM BACKEND ==================
async function loadServicesFromBackend() {
    try {
        console.log({"username": getCredentials().username});
        const response = await apiPost("/get_my_services", {"username": getCredentials().username});
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

async function selectService(serviceName, opts = {}) {
    currentService = serviceName;
    tokenVisible = false;

    // store in localStorage unless explicitly skipped
    if (!opts.skipSave) {
        localStorage.setItem("activeService", serviceName);
    }

    // fetch metadata from backend
    const response = await apiPost("/get_service_metadata", { service_name: getCredentials().username + "." + serviceName });
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
    document.getElementById('show-tok').textContent = "Show Token";

    if (Object.keys(services).length === 0) {
        emptyState.style.display = 'block';
        serviceDetails.classList.remove('active');
    } else if (currentService) {
        emptyState.style.display = 'none';
        serviceDetails.classList.add('active');

        document.getElementById('serviceTitle').textContent = currentService;
        document.getElementById('serviceSubtitle').innerHTML = `Manage API configuration`;

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
    errorDiv.style.display = 'none';
    errorDiv.textContent = '';

    if (!serviceName) return;

    // ✅ validate alphanumeric
    if (!isAlphanumeric(serviceName)) {
        errorDiv.textContent = "Service name must be alphanumeric (letters and numbers only).";
        errorDiv.style.display = 'block';
        return;
    }

    // ✅ validate max length 12
    if (serviceName.length > 12) {
        errorDiv.textContent = "Service name must be at most 12 characters long.";
        errorDiv.style.display = 'block';
        return;
    }

    const response = await apiPost("/register_service", {
        service_name: serviceName,
        username: getCredentials().username,
        password: getCredentials().password,
    });

    if (response.status === "ERR") {
        errorDiv.textContent = "Service name already exists. Please choose a different name.";
        errorDiv.style.display = 'block';
        return;
    }

    // ✅ success
    services[serviceName] = {
        token: response.service_token,
    };

    currentService = serviceName;
    localStorage.setItem("activeService", serviceName);

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
            const response = await apiPost("/get_token", { "service_name": serviceAtRequest,
            "password": getCredentials().password, "username": getCredentials().username });
            if (response.status === "OK" && currentService === serviceAtRequest) {
                services[serviceAtRequest].token = response.token;
                tokenDisplay.textContent = response.token;
            } else if (currentService !== serviceAtRequest) {
                console.log("⚠Ignoring token response for stale service:", serviceAtRequest);
            } else {
                console.error("Failed to fetch token:", response.message);
                tokenVisible = false;
            }
        } catch (err) {
            console.error("Error fetching token:", err);
            tokenVisible = false;
        }
    }

    tokenDisplay.classList.toggle('visible', tokenVisible);
    button.textContent = tokenVisible ? 'Hide Token' : 'Show Token';
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

    // ✅ Show Save button when metadata is changed
    const saveBtn = document.getElementById('saveMetadataBtn');
    if (saveBtn) {
        saveBtn.style.display = 'inline-block';
        saveBtn.textContent = "Save Changes";
        saveBtn.disabled = false;
    }
}




async function saveMetadata() {
    updateMetadata();
    if (!currentService) return;

    const saveBtn = document.getElementById('saveMetadataBtn');
    if (!saveBtn) return;

    saveBtn.disabled = true;
    saveBtn.textContent = "Saving..";

    const response = await apiPost("/update_service", {
        "service_name": currentService,
        "username": getCredentials().username,
        "password": getCredentials().password,
        "metadata": services[currentService].metadata
    });

    if (response.status === "OK") {
        saveBtn.textContent = "Saved!";
        setTimeout(() => {
            saveBtn.style.display = "none";  // ✅ hide after success
            saveBtn.disabled = false;
        }, 500);
    } else {
        saveBtn.textContent = "Failed ;(";
        setTimeout(() => {
            saveBtn.textContent = "Save Changes"; // reset but keep visible
            saveBtn.disabled = false;
        }, 500);
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
        const response = await apiPost("/delete_service", { service_name: currentService, password: getCredentials().password, username: getCredentials().username });
        if (response.status === "OK") {
            delete services[currentService];
            localStorage.removeItem("activeService");
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





// ================== START ==================
init();
