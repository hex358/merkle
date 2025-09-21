class ServiceList {
	constructor() {
		this.itemsPerPage = 12;
		this.currentFilter = '';
		this.isLoading = false;
		this.hasMore = true;
		this.services = [];
		this.nextPageId = "1";
		this.requestGen = 0;

		// Scroll-driven config
		this.prefetchPx = 250;     // how close to bottom counts as "near"
		this.prefillMax = 1;       // safety cap for initial prefill
		this.prefillCount = 0;
		this.initialPrefillActive = true; // prefill until scrollbar appears
		this.canTrigger = false;   // hysteresis gate

		this.loadScheduled = false;
		this.rafId = null;
		this.lastPageUsed = null;

		this.initializeElements();
		this.setupEventListeners();

		// Initial page
		this.scheduleLoad(true);
	}

	getScrollParent(el) {
		let p = el?.parentElement;
		while (p) {
			const s = getComputedStyle(p);
			if (/(auto|scroll|overlay)/.test(s.overflowY + s.overflowX + s.overflow)) return p;
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

		// Anchor to insert before; not used for visibility anymore
		this.sentinel = document.createElement('div');
		this.sentinel.id = 'infinite-sentinel';
		this.sentinel.style.height = '1px';
		this.sentinel.style.width = '100%';
		this.servicesGrid.appendChild(this.sentinel);

		// Choose a scroll root if a container scrolls; otherwise fall back later.
		this.scrollRoot = this.getScrollParent(this.servicesGrid);
	}

	setupEventListeners() {
		// --- unified throttled checker ---
		const onPassiveCheck = () => {
			if (this.rafId) return;
			this.rafId = requestAnimationFrame(() => {
				this.rafId = null;
				this.onScrollLikeEvent();
			});
		};
		this._onPassiveCheck = onPassiveCheck; // keep for potential cleanup

		// --- attach to ALL plausible scroll sources ---
		// 1) Explicit scroll container, if found
		if (this.scrollRoot) {
			this.scrollRoot.addEventListener('scroll', onPassiveCheck, { passive: true });
		}
		// 2) Window (covers typical document scrolling)
		window.addEventListener('scroll', onPassiveCheck, { passive: true });
		// 3) Document-level fallback (some engines fire here more consistently)
		document.addEventListener('scroll', onPassiveCheck, { passive: true });

		// --- other user-driven motion that may not emit 'scroll' on some targets ---
		window.addEventListener('wheel', onPassiveCheck, { passive: true });
		window.addEventListener('touchmove', onPassiveCheck, { passive: true });

		// Keyboard navigation (PgDn, Space, arrows, End/Home)
		window.addEventListener('keydown', (e) => {
			const keys = ['PageDown','PageUp','End','Home','ArrowDown','ArrowUp',' '];
			if (keys.includes(e.key)) onPassiveCheck();
		}, { passive: true });

		// Resize should retrigger near-bottom math
		window.addEventListener('resize', onPassiveCheck, { passive: true });

		// Content changes (cards appended) can change scrollHeight without a scroll event
		this._mo = new MutationObserver(() => onPassiveCheck());
		this._mo.observe(this.servicesGrid, { childList: true, subtree: false });

		// Post-layout kick: ensure at least one check after first paint
		setTimeout(() => onPassiveCheck(), 0);
		requestAnimationFrame(() => onPassiveCheck());

		// Search debounce
		let t = null;
		this.searchInput?.addEventListener('input', (e) => {
			clearTimeout(t);
			t = setTimeout(() => this.handleSearch(e.target.value), 300);
		});
	}

	// ===== Scroll math =====
	getScrollMetrics() {
		// Prefer the real scrolling element if window is the scroller
		const scrollingEl = document.scrollingElement || document.documentElement;

		if (this.scrollRoot) {
			return {
				scrollTop: this.scrollRoot.scrollTop,
				clientHeight: this.scrollRoot.clientHeight,
				scrollHeight: this.scrollRoot.scrollHeight
			};
		}
		return {
			scrollTop: window.scrollY ?? scrollingEl.scrollTop ?? 0,
			clientHeight: window.innerHeight ?? scrollingEl.clientHeight,
			scrollHeight: scrollingEl.scrollHeight
		};
	}

	isViewportFilled() {
		const { clientHeight, scrollHeight } = this.getScrollMetrics();
		return scrollHeight > clientHeight + 1;
	}

isNearBottom() {
	const scrollTop = window.scrollY || document.documentElement.scrollTop || 0;
	const clientHeight = window.innerHeight;
	const scrollHeight = document.documentElement.scrollHeight;

	return (scrollHeight - (scrollTop + clientHeight)) <= this.prefetchPx;
}


	onScrollLikeEvent() {
		// Hysteresis: re-arm when user is NOT near bottom
		if (!this.isNearBottom()) {
			this.canTrigger = true;
			return;
		}
		// If near bottom: allow load only if armed or during initial prefill
		if (this.canTrigger || this.initialPrefillActive) {
			this.canTrigger = false; // disarm until they scroll away again
			this.scheduleLoad(false);
		}
	}

	// ===== Lifecycle =====
	resetPagination() {
		this.hasMore = true;
		this.services = [];
		this.servicesGrid.innerHTML = '';

		this.nextPageId = "1";
		this.lastPageUsed = null;

		this.prefillCount = 0;
		this.initialPrefillActive = true;
		this.canTrigger = false;

		this.sentinel = document.createElement('div');
		this.sentinel.id = 'infinite-sentinel';
		this.sentinel.style.height = '1px';
		this.sentinel.style.width = '100%';
		this.servicesGrid.appendChild(this.sentinel);

		this.scheduleLoad(true);
		// After resetting, force a check once DOM mutates
		queueMicrotask(() => this.onScrollLikeEvent());
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

	scheduleLoad(force = false) {
		if (this.loadScheduled || this.isLoading || !this.hasMore) return;

		if (!force && !this.isNearBottom()) return;

		this.loadScheduled = true;
		queueMicrotask(async () => {
			this.loadScheduled = false;
			if (this.isLoading || !this.hasMore) return;
			if (force || this.isNearBottom()) {
				await this.loadServices();
			}
		});
	}

	async loadServices() {
		if (this.isLoading || !this.hasMore) return;
		this.isLoading = true;
		this.showLoading(true);
		const gen = ++this.requestGen;

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
			if (gen !== this.requestGen) return; // stale

			const batch = Array.isArray(response?.services) ? response.services : [];
			this.renderServices(batch);
			this.services.push(...batch);

			const norm = this._normalizeResponse(response, batch.length);

			let newNext = null;
			if (norm.nextPageId != null) {
				newNext = String(norm.nextPageId);
			} else {
				const n = Number(pageUsed);
				newNext = Number.isFinite(n) ? String(n + 1) : null;
			}

			if ((newNext === pageUsed || newNext == null) && batch.length === 0) {
				this.hasMore = false;
			} else {
				this.nextPageId = newNext ?? pageUsed;
				this.hasMore = !!norm.hasMore && batch.length > 0;
			}

			this.updateResultsInfo({ total: norm.total });
			this.showNoResults(this.services.length === 0);

		} catch (err) {
			console.error('Failed to load services:', err);
			this.updateResultsInfo(null, 'Error loading services');
		} finally {
			this.isLoading = false;
			this.showLoading(false);

			// INITIAL PREFILL: continue until scrollbar appears (with cap)
			if (this.hasMore && this.initialPrefillActive && !this.isViewportFilled()) {
				if (this.prefillCount < this.prefillMax) {
					this.prefillCount += 1;
					this.scheduleLoad(true);
					return;
				}
			}

			// Once viewport is filled OR we hit the cap, end initial prefill phase
			if (this.isViewportFilled() || this.prefillCount >= this.prefillMax) {
				this.initialPrefillActive = false;
				if (!this.isNearBottom()) this.canTrigger = true;
			}

			// Ensure a re-check after DOM has settled (covers rare no-scroll-event cases)
			queueMicrotask(() => this.onScrollLikeEvent());
		}
	}

	renderServices(batch) {
		for (const service of batch) {
			this.servicesGrid.insertBefore(this.createServiceCard(service), this.sentinel);
		}
		this.servicesGrid.appendChild(this.sentinel);
	}

	createServiceCard(service) {
		const card = document.createElement('div');
		card.className = 'service-card';
		const service_name = (service.service_name || '').replace("_", ".");
		const [org, name] = service_name.split(".");
		card.innerHTML = `
			<div class="service-name">
				<dark>${org ?? ''}</dark>.${name ?? ''}
			</div>`;
		card.addEventListener('click', () => this.handleServiceClick(service_name));
		return card;
	}

	handleServiceClick(service) {
		window.location.href = BACKEND_URL + "/service/" + service;
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
