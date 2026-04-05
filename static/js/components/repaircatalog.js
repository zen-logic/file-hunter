import API from '../api.js';

const RepairCatalog = {
    overlayEl: null,
    _pollTimer: null,
    _busy: false,

    init() {
        this.overlayEl = document.getElementById('repair-catalog-modal');
        document.getElementById('repair-done-close').addEventListener('click', () => this.close());
        document.getElementById('repair-choose-cancel').addEventListener('click', () => this.close());
        document.getElementById('repair-choose-start').addEventListener('click', () => this._startFromChoices());
    },

    open() {
        document.getElementById('repair-opt-partials').checked = true;
        document.getElementById('repair-opt-hashes').checked = true;
        document.getElementById('repair-opt-duplicates').checked = true;
        document.getElementById('repair-opt-sizes').checked = true;
        this._showStep('choose');
        this.overlayEl.classList.remove('hidden');
    },

    _startFromChoices() {
        const phases = [];
        if (document.getElementById('repair-opt-partials').checked) phases.push('partials');
        if (document.getElementById('repair-opt-hashes').checked) phases.push('hashes');
        if (document.getElementById('repair-opt-duplicates').checked) phases.push('duplicates');
        if (document.getElementById('repair-opt-sizes').checked) phases.push('sizes');
        if (phases.length === 0) return;
        this.start(phases);
    },

    async start(phases) {
        this._busy = true;
        this._showStep('progress');
        document.getElementById('repair-phase-label').textContent = 'Starting...';
        document.getElementById('repair-progress-fill').style.width = '0%';
        document.getElementById('repair-progress-text').textContent = '';

        const res = await API.post('/api/stats/repair', { phases });
        if (!res.ok) {
            this._showDone({ status: 'error', error: res.error || 'Failed to start repair' });
            return;
        }

        this._startPolling();
    },

    close() {
        if (this._busy) return;
        if (this._pollTimer) {
            clearInterval(this._pollTimer);
            this._pollTimer = null;
        }
        this.overlayEl.classList.add('hidden');
    },

    _showStep(step) {
        document.getElementById('repair-step-choose').classList.toggle('hidden', step !== 'choose');
        document.getElementById('repair-step-progress').classList.toggle('hidden', step !== 'progress');
        document.getElementById('repair-step-done').classList.toggle('hidden', step !== 'done');
    },

    _startPolling() {
        const fillEl = document.getElementById('repair-progress-fill');
        const textEl = document.getElementById('repair-progress-text');
        const phaseEl = document.getElementById('repair-phase-label');

        this._pollTimer = setInterval(async () => {
            const res = await API.get('/api/stats/repair-progress');
            if (!res.ok) return;

            const p = res.data;

            if (p.phase === 'pausing') {
                phaseEl.textContent = 'Pausing operations...';
                fillEl.style.width = '0%';
                textEl.textContent = 'Waiting for running operations to finish';
            } else if (p.phase === 'finding_partials') {
                phaseEl.textContent = 'Finding missing files';
                const pct = p.partials_scan_total > 0
                    ? Math.round((p.partials_scan_done / p.partials_scan_total) * 100)
                    : 0;
                fillEl.style.width = pct + '%';
                const parts = [`${p.partials_scan_done} / ${p.partials_scan_total} locations`];
                if (p.partials_found > 0) parts.push(`${p.partials_found.toLocaleString()} files found`);
                textEl.textContent = parts.join(' \u00b7 ');
            } else if (p.phase === 'hashing_partials') {
                phaseEl.textContent = 'Computing missing partials';
                const done = (p.partials_hashed || 0) + (p.partials_stale || 0) + (p.partials_errors || 0) + (p.partials_skipped || 0);
                const pct = p.partials_total > 0 ? Math.round((done / p.partials_total) * 100) : 0;
                fillEl.style.width = pct + '%';
                const parts = [`${done.toLocaleString()} / ${p.partials_total.toLocaleString()}`];
                if (p.partials_hashed > 0) parts.push(`${p.partials_hashed.toLocaleString()} hashed`);
                if (p.partials_stale > 0) parts.push(`${p.partials_stale.toLocaleString()} marked stale`);
                if (p.partials_skipped > 0) parts.push(`${p.partials_skipped.toLocaleString()} skipped (offline)`);
                if (p.partials_errors > 0) parts.push(`${p.partials_errors.toLocaleString()} error${p.partials_errors === 1 ? '' : 's'}`);
                textEl.textContent = parts.join(' \u00b7 ');
            } else if (p.phase === 'querying') {
                phaseEl.textContent = 'Finding duplicate group candidates';
                if (p.query_total > 0) {
                    const pct = Math.round((p.query_done / p.query_total) * 100);
                    fillEl.style.width = pct + '%';
                    textEl.textContent = `Searching ${p.query_done.toLocaleString()} / ${p.query_total.toLocaleString()} duplicate groups`;
                } else {
                    fillEl.style.width = '0%';
                    textEl.textContent = 'Querying database...';
                }
            } else if (p.phase === 'hashing') {
                phaseEl.textContent = 'Computing missing hashes';
                const done = p.hashed + (p.stale || 0) + p.errors + (p.skipped || 0);
                const pct = p.total > 0 ? Math.round((done / p.total) * 100) : 0;
                fillEl.style.width = pct + '%';
                const parts = [`${done.toLocaleString()} / ${p.total.toLocaleString()}`];
                if (p.hashed > 0) parts.push(`${p.hashed.toLocaleString()} hashed`);
                if (p.stale > 0) parts.push(`${p.stale.toLocaleString()} marked stale`);
                if (p.skipped > 0) parts.push(`${p.skipped.toLocaleString()} skipped (offline)`);
                if (p.errors > 0) parts.push(`${p.errors.toLocaleString()} error${p.errors === 1 ? '' : 's'}`);
                textEl.textContent = parts.join(' \u00b7 ');
            } else if (p.phase === 'dup_recount') {
                phaseEl.textContent = 'Recounting duplicates';
                const step = p.dup_step || '';
                if (step === 'discovery') {
                    const pct = p.dup_step_total > 0 ? Math.round((p.dup_step_done / p.dup_step_total) * 100) : 0;
                    fillEl.style.width = pct + '%';
                    textEl.textContent = `Counting duplicate groups: ${p.dup_step_done} / ${p.dup_step_total}`;
                } else if (step === 'reset') {
                    const pct = p.dup_step_total > 0 ? Math.round((p.dup_step_done / p.dup_step_total) * 100) : 0;
                    fillEl.style.width = pct + '%';
                    textEl.textContent = `Resetting counts: ${p.dup_step_done} / ${p.dup_step_total}`;
                } else if (step === 'write') {
                    const pct = p.dup_step_total > 0 ? Math.round((p.dup_step_done / p.dup_step_total) * 100) : 0;
                    fillEl.style.width = pct + '%';
                    textEl.textContent = `Writing: ${p.dup_step_done.toLocaleString()} / ${p.dup_step_total.toLocaleString()} rows`;
                } else if (step === 'catalog') {
                    const pct = p.dup_step_total > 0 ? Math.round((p.dup_step_done / p.dup_step_total) * 100) : 0;
                    fillEl.style.width = pct + '%';
                    textEl.textContent = `Syncing catalog: ${p.dup_step_done.toLocaleString()} / ${p.dup_step_total.toLocaleString()} files`;
                } else {
                    fillEl.style.width = '0%';
                    textEl.textContent = 'Starting...';
                }
            } else if (p.phase === 'sizes') {
                phaseEl.textContent = 'Recalculating sizes';
                const pct = p.locations_total > 0
                    ? Math.round((p.locations_done / p.locations_total) * 100)
                    : 0;
                fillEl.style.width = pct + '%';
                textEl.textContent =
                    `${p.locations_done} / ${p.locations_total} locations`;
            } else if (p.status === 'complete') {
                clearInterval(this._pollTimer);
                this._pollTimer = null;
                this._showDone(p);
            } else if (p.status === 'error') {
                clearInterval(this._pollTimer);
                this._pollTimer = null;
                this._showDone(p);
            }
        }, 500);
    },

    _showDone(p) {
        this._busy = false;
        const el = document.getElementById('repair-done-text');
        if (p.status === 'error') {
            el.innerHTML = `<span style="color:var(--color-status-error)">Repair failed: ${p.error || 'Unknown error'}</span>`;
        } else {
            const lines = ['Repair complete.'];
            if (p.partials_hashed > 0 || p.partials_stale > 0 || p.partials_skipped > 0 || p.partials_errors > 0) {
                lines.push(
                    `Partials: ${p.partials_hashed.toLocaleString()} computed` +
                    (p.partials_stale > 0 ? `, ${p.partials_stale.toLocaleString()} marked stale` : '') +
                    (p.partials_skipped > 0 ? `, ${p.partials_skipped.toLocaleString()} skipped (offline)` : '') +
                    (p.partials_errors > 0 ? `, <a href="#" class="repair-error-toggle">${p.partials_errors.toLocaleString()} errors</a>` : '')
                );
            }
            if (p.hashed > 0 || p.stale > 0 || p.skipped > 0 || p.errors > 0) {
                lines.push(
                    `Hashes: ${p.hashed.toLocaleString()} computed` +
                    (p.stale > 0 ? `, ${p.stale.toLocaleString()} marked stale` : '') +
                    (p.skipped > 0 ? `, ${p.skipped.toLocaleString()} skipped (offline)` : '') +
                    (p.errors > 0 ? `, <a href="#" class="repair-error-toggle">${p.errors.toLocaleString()} errors</a>` : '')
                );
            }
            if (p.dup_groups > 0) {
                lines.push(`Duplicates: ${p.dup_groups.toLocaleString()} groups recounted`);
            }
            if (p.locations_total > 0) {
                lines.push(`Sizes: ${p.locations_total} locations recalculated`);
            }

            // Error details expandable
            const errors = p.error_details || [];
            let errorHtml = '';
            if (errors.length > 0) {
                const errorLines = errors.map(e =>
                    `<div class="repair-error-line">${e.path}<br><span class="settings-hint">${e.error}</span></div>`
                ).join('');
                errorHtml = `<div class="repair-error-details hidden">${errorLines}</div>`;
            }

            el.innerHTML = lines.join('<br>') + errorHtml;

            // Wire up toggle
            el.querySelectorAll('.repair-error-toggle').forEach(link => {
                link.addEventListener('click', (e) => {
                    e.preventDefault();
                    const details = el.querySelector('.repair-error-details');
                    if (details) details.classList.toggle('hidden');
                });
            });
        }
        this._showStep('done');
    },
};

export default RepairCatalog;
