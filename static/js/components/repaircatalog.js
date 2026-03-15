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
        document.getElementById('repair-opt-hashes').checked = true;
        document.getElementById('repair-opt-duplicates').checked = true;
        document.getElementById('repair-opt-sizes').checked = true;
        this._showStep('choose');
        this.overlayEl.classList.remove('hidden');
    },

    _startFromChoices() {
        const phases = [];
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
            } else if (p.phase === 'querying') {
                phaseEl.textContent = 'Phase 1: Finding files to hash';
                fillEl.style.width = '0%';
                textEl.textContent = 'Querying database...';
            } else if (p.phase === 'hashing') {
                phaseEl.textContent = 'Phase 1: Computing missing hashes';
                const done = p.hashed + p.errors + (p.skipped || 0);
                const pct = p.total > 0 ? Math.round((done / p.total) * 100) : 0;
                fillEl.style.width = pct + '%';
                const parts = [];
                parts.push(`${done.toLocaleString()} / ${p.total.toLocaleString()}`);
                if (p.hashed > 0) parts.push(`${p.hashed.toLocaleString()} hashed`);
                if (p.skipped > 0) parts.push(`${p.skipped.toLocaleString()} skipped (offline)`);
                if (p.errors > 0) parts.push(`${p.errors.toLocaleString()} errors`);
                textEl.textContent = parts.join(' \u2014 ');
            } else if (p.phase === 'dup_recount') {
                phaseEl.textContent = 'Recounting duplicates';
                const pct = p.dup_hashes_total > 0
                    ? Math.round((p.dup_hashes_done / p.dup_hashes_total) * 100)
                    : 0;
                fillEl.style.width = pct + '%';
                textEl.textContent = p.dup_hashes_total > 0
                    ? `${p.dup_hashes_done.toLocaleString()} / ${p.dup_hashes_total.toLocaleString()} hashes`
                    : 'Querying hash counts...';
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
            if (p.hashed > 0 || p.skipped > 0 || p.errors > 0) {
                lines.push(
                    `Hashing: ${p.hashed.toLocaleString()} computed` +
                    (p.skipped > 0 ? `, ${p.skipped.toLocaleString()} skipped (offline)` : '') +
                    (p.errors > 0 ? `, ${p.errors.toLocaleString()} errors` : '')
                );
            }
            if (p.dup_hashes_total > 0) {
                lines.push(`Duplicates: ${p.dup_hashes_total.toLocaleString()} hashes recounted`);
            }
            if (p.locations_total > 0) {
                lines.push(`Sizes: ${p.locations_total} locations recalculated`);
            }
            el.innerHTML = lines.join('<br>');
        }
        this._showStep('done');
    },
};

export default RepairCatalog;
