import API from '../api.js';

const FastScan = {
    overlayEl: null,
    progressStep: null,
    doneStep: null,
    phaseEl: null,
    fillEl: null,
    textEl: null,
    doneTextEl: null,
    _pollTimer: null,

    init() {
        this.overlayEl = document.getElementById('fast-scan-modal');
        this.progressStep = document.getElementById('fast-scan-step-progress');
        this.doneStep = document.getElementById('fast-scan-step-done');
        this.phaseEl = document.getElementById('fast-scan-phase');
        this.fillEl = document.getElementById('fast-scan-progress-fill');
        this.textEl = document.getElementById('fast-scan-progress-text');
        this.doneTextEl = document.getElementById('fast-scan-done-text');

        document.getElementById('fast-scan-done-close').addEventListener('click', () => this.close());
        this.overlayEl.addEventListener('click', (e) => {
            // Only allow closing on overlay click when done
            if (e.target === this.overlayEl && !this.doneStep.classList.contains('hidden')) {
                this.close();
            }
        });
    },

    open() {
        this.progressStep.classList.remove('hidden');
        this.doneStep.classList.add('hidden');
        this.phaseEl.textContent = 'Starting...';
        this.fillEl.style.width = '0%';
        this.textEl.textContent = '';
        this.overlayEl.classList.remove('hidden');
        this._startPolling();
    },

    close() {
        this._stopPolling();
        this.overlayEl.classList.add('hidden');
    },

    _startPolling() {
        if (this._pollTimer) return;
        this._pollTimer = setInterval(async () => {
            const res = await API.get('/api/scan/fast/progress');
            if (!res.ok) return;
            const p = res.data;

            if (p.phase === 'pausing') {
                this.phaseEl.textContent = 'Pausing operations...';
                this.fillEl.style.width = '0%';
                this.textEl.textContent = '';
            } else if (p.phase === 'deleting') {
                this.phaseEl.textContent = 'Deleting existing data...';
                this.fillEl.style.width = '0%';
                this.textEl.textContent = '';
            } else if (p.phase === 'walking') {
                this.phaseEl.textContent = 'Walking filesystem';
                this.fillEl.style.width = '0%'; // indeterminate — no total known
                this.textEl.textContent = `${p.files_found.toLocaleString()} files found in ${p.folders_found.toLocaleString()} folders`;
            } else if (p.phase === 'hashing') {
                this.phaseEl.textContent = 'Hashing files';
                const pct = p.files_to_hash > 0 ? Math.round((p.files_hashed / p.files_to_hash) * 100) : 0;
                this.fillEl.style.width = pct + '%';
                this.textEl.textContent = `${p.files_hashed.toLocaleString()} / ${p.files_to_hash.toLocaleString()} (${pct}%)`;
            } else if (p.phase === 'recounting') {
                this.phaseEl.textContent = 'Recounting duplicates';
                const pct = p.files_to_hash > 0 ? Math.round((p.files_hashed / p.files_to_hash) * 100) : 0;
                this.fillEl.style.width = pct + '%';
                this.textEl.textContent = p.files_to_hash > 0
                    ? `${p.files_hashed.toLocaleString()} / ${p.files_to_hash.toLocaleString()} hashes (${pct}%)`
                    : 'Collecting hashes...';
            } else if (p.phase === 'rebuilding') {
                this.phaseEl.textContent = 'Rebuilding sizes';
                this.fillEl.style.width = '100%';
                this.textEl.textContent = '';
            } else if (p.status === 'complete') {
                this._showDone(p);
            } else if (p.status === 'error') {
                this._showError(p);
            }
        }, 500);
    },

    _stopPolling() {
        if (this._pollTimer) {
            clearInterval(this._pollTimer);
            this._pollTimer = null;
        }
    },

    _showDone(p) {
        this._stopPolling();
        this.progressStep.classList.add('hidden');
        this.doneStep.classList.remove('hidden');
        this.doneTextEl.innerHTML =
            `Fast Scan complete.<br>` +
            `${p.files_found.toLocaleString()} files in ` +
            `${p.folders_found.toLocaleString()} folders.`;
    },

    _showError(p) {
        this._stopPolling();
        this.progressStep.classList.add('hidden');
        this.doneStep.classList.remove('hidden');
        this.doneTextEl.innerHTML =
            `<span style="color:var(--color-status-error)">Fast Scan failed: ${p.error}</span>`;
    },
};

export default FastScan;
