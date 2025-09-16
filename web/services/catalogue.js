// Mock API function - replace with your actual implementation
async function apiPostMock(endpoint, data) {
    // Simulate API delay
    await new Promise(resolve => setTimeout(resolve, Math.random() * 300 + 200));

    // Mock fuzzy search data generator
    const generateMockServices = (page, number, filter) => {
        const allMockNames = [
            'google/gemini', 'google/gmail', 'google/drive', 'google/maps',
            'openai/gpt-4', 'openai/whisper', 'openai/dall-e', 'openai/codex',
            'anthropic/claude', 'anthropic/constitutional-ai',
            'microsoft/copilot', 'microsoft/azure', 'microsoft/teams',
            'meta/llama', 'meta/prophet', 'meta/react',
            'github/actions', 'github/copilot', 'github/api',
            'aws/lambda', 'aws/s3', 'aws/ec2', 'aws/rds',
            'stripe/payments', 'stripe/billing', 'stripe/connect',
            'mongodb/atlas', 'mongodb/compass', 'mongodb/realm',
            'docker/registry', 'docker/compose', 'docker/swarm',
            'kubernetes/api', 'kubernetes/ingress', 'kubernetes/helm'
        ];

        let filteredNames = allMockNames;

        // Server-side fuzzy search simulation
        if (filter && filter.trim()) {
            const query = filter.toLowerCase().trim();
            filteredNames = allMockNames.filter(name => {
                const nameLower = name.toLowerCase();
                // Exact matches get priority
                if (nameLower.includes(query)) return true;

                // Fuzzy matching - allow for typos and partial matches
                const [org, service] = name.split('/');
                const orgLower = org.toLowerCase();
                const serviceLower = service.toLowerCase();

                // Check if query matches org or service with some fuzziness
                return (
                    orgLower.includes(query) ||
                    serviceLower.includes(query) ||
                    this.fuzzyMatch(query, orgLower) ||
                    this.fuzzyMatch(query, serviceLower)
                );
            });

            // Sort by relevance (exact matches first)
            filteredNames.sort((a, b) => {
                const aLower = a.toLowerCase();
                const bLower = b.toLowerCase();
                const aExact = aLower.includes(query);
                const bExact = bLower.includes(query);

                if (aExact && !bExact) return -1;
                if (!aExact && bExact) return 1;
                return 0;
            });
        }

        // Pagination
        const startIndex = page * number;
        const endIndex = startIndex + number;
        const paginatedNames = filteredNames.slice(startIndex, endIndex);

        const services = paginatedNames.map((name, index) => {
            const [org, service] = name.split('/');
            return {
                id: startIndex + index,
                name: name,
                org: org,
                service: service
            };
        });

        return {
            services,
            hasMore: endIndex < filteredNames.length,
            total: filteredNames.length,
            page: page,
            query: filter
        };
    };

    // Simple fuzzy matching helper
    this.fuzzyMatch = (query, target) => {
        if (!query || !target) return false;
        if (query.length > target.length) return false;

        let queryIndex = 0;
        for (let i = 0; i < target.length && queryIndex < query.length; i++) {
            if (target[i] === query[queryIndex]) {
                queryIndex++;
            }
        }

        return queryIndex === query.length;
    };

    if (endpoint === 'get_results') {
        return generateMockServices(data.page, data.number, data.filter);
    }

    throw new Error('Unknown endpoint');
}

class ServiceList {
    constructor() {
        this.currentPage = 0;
        this.itemsPerPage = 12;
        this.currentFilter = '';
        this.isLoading = false;
        this.hasMore = true;
        this.services = [];
        this.searchTimeout = null;

        this.initializeElements();
        this.setupEventListeners();
        this.loadServices();
    }

    initializeElements() {
        this.searchInput = document.getElementById('searchInput');
        this.servicesGrid = document.getElementById('servicesGrid');
        this.loadingIndicator = document.getElementById('loadingIndicator');
        this.noResults = document.getElementById('noResults');
        this.resultsInfo = document.getElementById('resultsInfo');
    }

    setupEventListeners() {
        // Search input with debounce
        this.searchInput.addEventListener('input', (e) => {
            clearTimeout(this.searchTimeout);
            this.searchTimeout = setTimeout(() => {
                this.handleSearch(e.target.value);
            }, 300);
        });

        // Infinite scroll
        window.addEventListener('scroll', () => {
            if (this.shouldLoadMore()) {
                this.loadServices();
            }
        });
    }

    shouldLoadMore() {
        const scrollTop = window.pageYOffset;
        const windowHeight = window.innerHeight;
        const documentHeight = document.documentElement.scrollHeight;

        return (
            !this.isLoading &&
            this.hasMore &&
            scrollTop + windowHeight >= documentHeight - 1000
        );
    }

    async handleSearch(query) {
        this.currentFilter = query;
        this.resetPagination();
        await this.loadServices();
    }

    resetPagination() {
        this.currentPage = 0;
        this.hasMore = true;
        this.services = [];
        this.servicesGrid.innerHTML = '';
    }

    async loadServices() {
        if (this.isLoading || !this.hasMore) return;

        this.isLoading = true;
        this.showLoading(true);

        try {
            const response = await apiPost('list_services', {
                page_id: this.currentPage,
                num_results: this.itemsPerPage,
                filter: this.currentFilter,
                username: getCredentials() ? getCredentials().username : "",
            });

            // For search queries, reset the grid on first page
            if (this.currentPage === 0) {
                this.services = [];
                this.servicesGrid.innerHTML = '';
            }

            this.services.push(...response.services);
            this.hasMore = response.hasMore;
            this.currentPage++;

            this.renderServices(response.services);
            this.updateResultsInfo(response);

            if (this.services.length === 0) {
                this.showNoResults(true);
            } else {
                this.showNoResults(false);
            }

        } catch (error) {
            console.error('Failed to load services:', error);
            this.updateResultsInfo(null, 'Error loading services');
        } finally {
            this.isLoading = false;
            this.showLoading(false);
        }
    }

    renderServices(services) {
        services.forEach(service => {
            const serviceCard = this.createServiceCard(service);
            this.servicesGrid.appendChild(serviceCard);
        });
    }

    createServiceCard(service) {
        const card = document.createElement('div');
        card.className = 'service-card';
        card.innerHTML = `
            <div class="service-name">
                <dark>${service.org}</dark>/${service.service}
            </div>
        `;

        card.addEventListener('click', () => {
            this.handleServiceClick(service);
        });

        return card;
    }

    handleServiceClick(service) {
        // Handle service card click - you can implement navigation here
        console.log('Clicked service:', service);
        // window.location.href = `/services/${service.org}/${service.service}`;
    }

updateResultsInfo(responseOrMessage = null, errorMessage = null) {
    if (errorMessage) {
        this.resultsInfo.textContent = errorMessage;
        return;
    }

    // If string message passed directly
    if (typeof responseOrMessage === "string") {
        this.resultsInfo.textContent = responseOrMessage;
        return;
    }

    // If response object
    if (responseOrMessage && typeof responseOrMessage === "object") {
        const filterText = this.currentFilter ? ` matching "${this.currentFilter}"` : '';
        this.resultsInfo.textContent =
            `Showing ${this.services.length} of ${responseOrMessage.total} services${filterText}`;
        return;
    }

    // Default when no services
    if (this.services.length === 0 && !this.isLoading) {
        this.resultsInfo.textContent = "No services found";
    }
}

    showLoading(show) {
        this.loadingIndicator.style.display = show ? 'flex' : 'none';
    }

    showNoResults(show) {
        this.noResults.style.display = show ? 'block' : 'none';
    }
}

// Initialize the service list when the page loads
document.addEventListener('DOMContentLoaded', () => {
    new ServiceList();
});