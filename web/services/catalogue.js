class ServiceList {
	constructor() {
		this.itemsPerPage = 12;
		this.currentFilter = '';
		this.isLoading = false;
		this.hasMore = true;
		this.services = [];
		this.nextPageId = "1";
		this.requestGen = 0;

		this.loadScheduled = false; // coalesce triggers
		this.rafId = null;          // throttle scroll/resize
		this.lastPageUsed = null;   // monotonic guard

		this.initializeElements();
		this.setupEventListeners();
		this.setupInfiniteObserver();

		// kick off initial batch via scheduler (not direct) to avoid re-entrancy
		this.scheduleLoad();
	}

	getScrollParent(el) {
		let p = el?.parentElement;
		while (p) {
			const s = getComputedStyle(p);
			if (/(auto|scroll|overlay)/.test(s.overflowY + s.overflowX + s.overflow)) {
				return p;
			}
			p = p.parentElement;
		}
		return null;
	}

	initializeElements() {
		this.searchInput = document.getElementById('searchInput');
		this.servicesGrid = document.getElementById('servicesGrid');
		this.loadingIndicator = document.getElementById('loadingIndicator');
		this.noResults = document.getElementById('noResults');
		this.resultsInfo = document.getElementById('resultsInfo');

		this.sentinel = document.createElement('div');
		this.sentinel.id = 'infinite-sentinel';
		this.sentinel.style.height = '1px';
		this.sentinel.style.width = '100%';
		this.servicesGrid.appendChild(this.sentinel);

		this.scrollRoot = this.getScrollParent(this.servicesGrid);

		const onPassiveCheck = () => {
			if (this.rafId) return;
			this.rafId = requestAnimationFrame(() => {
				this.rafId = null;
				this.scheduleLoad();
			});
		};
		(this.scrollRoot || window).addEventListener('scroll', onPassiveCheck, { passive: true });
		window.addEventListener('resize', onPassiveCheck, { passive: true });

		this.resizeObs = new ResizeObserver(() => this.scheduleLoad());
		this.resizeObs.observe(this.servicesGrid);
	}

	setupEventListeners() {
		let t = null;
		this.searchInput.addEventListener('input', (e) => {
			clearTimeout(t);
			t = setTimeout(() => this.handleSearch(e.target.value), 300);
		});
	}

	setupInfiniteObserver() {
		if (this.io) this.io.disconnect();
		this.io = new IntersectionObserver((entries) => {
			for (const entry of entries) {
				if (entry.isIntersecting) this.scheduleLoad();
			}
		}, {
			root: this.scrollRoot || null,
			rootMargin: '800px 0px 800px 0px',
			threshold: 0
		});
		this.io.observe(this.sentinel);
	}

	isSentinelVisible() {
		const rootEl = this.scrollRoot;
		const rect = this.sentinel.getBoundingClientRect();
		const vh = (rootEl ? rootEl.clientHeight : window.innerHeight) || 0;
		return rect.top < vh + 800 && rect.bottom > -800;
	}

	scheduleLoad() {
		if (this.loadScheduled || this.isLoading || !this.hasMore) return;
		if (!this.isSentinelVisible()) return;
		this.loadScheduled = true;
		queueMicrotask(async () => {
			this.loadScheduled = false;
			// re-check conditions right before loading
			if (!this.isLoading && this.hasMore && this.isSentinelVisible()) {
				await this.loadServices();
			}
		});
	}

	resetPagination() {
		this.hasMore = true;
		this.services = [];
		this.servicesGrid.innerHTML = '';

		this.nextPageId = "1";
		this.lastPageUsed = null;

		this.sentinel = document.createElement('div');
		this.sentinel.id = 'infinite-sentinel';
		this.sentinel.style.height = '1px';
		this.sentinel.style.width = '100%';
		this.servicesGrid.appendChild(this.sentinel);

		this.setupInfiniteObserver();
		this.scheduleLoad();
	}

	async handleSearch(query) {
		this.currentFilter = (query ?? '').trim();
		this.resetPagination();
	}

	_normalizeResponse(resp, batchLength) {
		const nextPageId = resp?.next_page_id ?? resp?.nextPageId ?? null;
		let hasMore;
		if (typeof resp?.hasMore === 'boolean') hasMore = resp.hasMore;
		else if (typeof resp?.has_more === 'boolean') hasMore = resp.has_more;
		else if (nextPageId != null) hasMore = true;
		else hasMore = batchLength === this.itemsPerPage;

		const total = resp?.total ?? resp?.total_count ?? resp?.count ?? undefined;
		return { nextPageId, hasMore, total };
	}

	async loadServices() {
		if (this.isLoading || !this.hasMore) return;
		this.isLoading = true;
		this.showLoading(true);
		const gen = ++this.requestGen;

		// remember which page we actually used
		const pageUsed = String(this.nextPageId || "1");
		this.lastPageUsed = pageUsed;

		try {
			const payload = {
				page_id: pageUsed,
				num_results: String(this.itemsPerPage),
				filter: this.currentFilter,
				username: ""
			};
			const response = await apiPost('list_services', payload);
			if (gen !== this.requestGen) return; // stale response

			const batch = Array.isArray(response?.services) ? response.services : [];
			this.renderServices(batch);
			this.services.push(...batch);

			const norm = this._normalizeResponse(response, batch.length);

			// Decide next page ID (monotonic, no-spin)
			let newNext = null;
			if (norm.nextPageId != null) {
				newNext = String(norm.nextPageId);
			} else {
				const n = Number(pageUsed);
				newNext = Number.isFinite(n) ? String(n + 1) : null;
			}

			// If server didn't advance page and we got nothing, stop.
			if (newNext === pageUsed && batch.length === 0) {
				this.hasMore = false;
			} else {
				this.nextPageId = newNext ?? pageUsed; // fallback to used page if parsing failed
				this.hasMore = !!norm.hasMore && batch.length >= 0; // norm.hasMore already accounts for per_page
			}

			this.updateResultsInfo({ total: norm.total });
			this.showNoResults(this.services.length === 0);

		} catch (err) {
			console.error('Failed to load services:', err);
			this.updateResultsInfo(null, 'Error loading services');
		} finally {
			this.isLoading = false;
			this.showLoading(false);

			// If viewport still not filled and there's more, schedule another (coalesced).
			if (this.hasMore) this.scheduleLoad();
		}
	}

	renderServices(batch) {
		for (const service of batch) {
			this.servicesGrid.insertBefore(this.createServiceCard(service), this.sentinel);
		}
		// keep sentinel last (no-op if already last)
		this.servicesGrid.appendChild(this.sentinel);
	}

	createServiceCard(service) {
		const card = document.createElement('div');
		card.className = 'service-card';
		console.log(service);
		const service_name = service.service_name.replace("_", ".");
		const org = service_name.split(".")[0];
		const name = service_name.split(".")[1];
		card.innerHTML = `
			<div class="service-name">
				<dark>${org}</dark>.${name}
			</div>`;
		card.addEventListener('click', () => this.handleServiceClick(service_name));
		return card;
	}

	handleServiceClick(service) {
	console.log("fkfk");
		window.location.href = BACKEND_URL + "/service/"+service;
	}

	updateResultsInfo(resp = null, errorMessage = null) {
		if (!this.resultsInfo) return;
		if (errorMessage) {
			this.resultsInfo.textContent = errorMessage;
			return;
		}
		const total = resp?.total;
		const filterText = this.currentFilter ? ` matching "${this.currentFilter}"` : '';
		if (typeof total === 'number') {
			this.resultsInfo.textContent = `Showing ${this.services.length} of ${total} services${filterText}`;
		} else {
			this.resultsInfo.textContent = `Showing ${this.services.length} services${filterText}`;
		}
	}

	showLoading(show) {
		if (this.loadingIndicator) this.loadingIndicator.style.display = show ? 'flex' : 'none';
	}
	showNoResults(show) {
		if (this.noResults) this.noResults.style.display = show ? 'block' : 'none';
	}
}

document.addEventListener('DOMContentLoaded', () => {
	new ServiceList();
});
