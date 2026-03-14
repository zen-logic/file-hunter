import API from '../api.js';

const ImportCatalog = {
    overlayEl: null,
    _file: null,
    _tempPath: null,
    _meta: null,
    _agents: [],
    _locations: [],
    _pollTimer: null,

    init() {
        this.overlayEl = document.getElementById('import-catalog-modal');

        document.getElementById('btn-import-catalog').addEventListener('click', () => this.open());

        // Step 1: upload
        this._fileInput = document.createElement('input');
        this._fileInput.type = 'file';
        this._fileInput.accept = '.db';
        this._fileInput.addEventListener('change', () => this._onFileSelected());

        document.getElementById('import-file-browse').addEventListener('click', () => {
            this._fileInput.click();
        });
        document.getElementById('import-upload-btn').addEventListener('click', () => this._doUpload());
        document.getElementById('import-cancel-1').addEventListener('click', () => this.close());

        // Step 2: config
        document.getElementById('import-agent').addEventListener('change', () => this._onAgentChange());
        document.getElementById('import-location').addEventListener('change', () => this._onLocationChange());
        document.getElementById('import-run-btn').addEventListener('click', () => this._doImport());
        document.getElementById('import-cancel-2').addEventListener('click', () => this.close());

        // Step 3: done
        document.getElementById('import-done-close').addEventListener('click', () => this.close());

        // Close on overlay click / escape
        this.overlayEl.addEventListener('click', (e) => {
            if (e.target === this.overlayEl) this.close();
        });
        document.addEventListener('keydown', (e) => {
            if (e.key === 'Escape' && !this.overlayEl.classList.contains('hidden')) {
                this.close();
            }
        });
    },

    open() {
        this._file = null;
        this._tempPath = null;
        this._meta = null;
        this._showStep('upload');
        document.getElementById('import-file-name').value = '';
        document.getElementById('import-upload-btn').disabled = true;
        document.getElementById('import-upload-error').classList.add('hidden');
        this.overlayEl.classList.remove('hidden');
    },

    close() {
        if (this._pollTimer) {
            clearInterval(this._pollTimer);
            this._pollTimer = null;
        }
        this.overlayEl.classList.add('hidden');
    },

    _showStep(step) {
        document.getElementById('import-step-upload').classList.toggle('hidden', step !== 'upload');
        document.getElementById('import-step-config').classList.toggle('hidden', step !== 'config');
        document.getElementById('import-step-progress').classList.toggle('hidden', step !== 'progress');
        document.getElementById('import-step-done').classList.toggle('hidden', step !== 'done');
    },

    _onFileSelected() {
        const f = this._fileInput.files[0];
        if (!f) return;
        this._file = f;
        document.getElementById('import-file-name').value = f.name;
        document.getElementById('import-upload-btn').disabled = false;
    },

    async _doUpload() {
        const errEl = document.getElementById('import-upload-error');
        errEl.classList.add('hidden');

        const btn = document.getElementById('import-upload-btn');
        btn.disabled = true;
        btn.textContent = 'Uploading...';

        const formData = new FormData();
        formData.append('catalog', this._file);

        const token = localStorage.getItem('fh-token');
        const res = await fetch('/api/import-catalog/upload', {
            method: 'POST',
            headers: token ? { 'Authorization': `Bearer ${token}` } : {},
            body: formData,
        });
        const data = await res.json();

        btn.textContent = 'Upload';

        if (!data.ok) {
            errEl.textContent = data.error || 'Upload failed';
            errEl.classList.remove('hidden');
            btn.disabled = false;
            return;
        }

        this._tempPath = data.data.temp_path;
        this._meta = data.data.meta;
        this._agents = data.data.agents;
        this._locations = data.data.locations;

        this._populateConfig();
        this._showStep('config');
    },

    _populateConfig() {
        const m = this._meta;
        const infoEl = document.getElementById('import-catalog-info');
        infoEl.innerHTML =
            `<strong>${m.root_path}</strong><br>` +
            `${m.file_count.toLocaleString()} files, ${m.folder_count.toLocaleString()} folders` +
            `${m.hostname ? ' &mdash; ' + m.hostname : ''}`;

        // Agents
        const agentSel = document.getElementById('import-agent');
        agentSel.innerHTML = '';
        for (const a of this._agents) {
            const opt = document.createElement('option');
            opt.value = a.id;
            opt.textContent = a.name + (a.status === 'online' ? '' : ' (offline)');
            agentSel.appendChild(opt);
        }

        this._onAgentChange();
    },

    _onAgentChange() {
        const agentId = parseInt(document.getElementById('import-agent').value);
        const locSel = document.getElementById('import-location');
        locSel.innerHTML = '';

        // Filter locations for this agent
        const agentLocs = this._locations.filter(l => l.agent_id === agentId);

        // "Create new" option
        const newOpt = document.createElement('option');
        newOpt.value = '__new__';
        newOpt.textContent = '+ Create new location';
        locSel.appendChild(newOpt);

        for (const l of agentLocs) {
            const opt = document.createElement('option');
            opt.value = l.id;
            opt.textContent = `${l.name} (${l.root_path})`;
            locSel.appendChild(opt);
        }

        // Auto-select matching location by root_path
        if (this._meta) {
            const match = agentLocs.find(l => l.root_path === this._meta.root_path);
            if (match) {
                locSel.value = match.id;
            }
        }

        this._onLocationChange();
    },

    _onLocationChange() {
        const val = document.getElementById('import-location').value;
        const newFields = document.getElementById('import-new-loc-fields');

        if (val === '__new__') {
            newFields.classList.remove('hidden');
            // Pre-fill from catalog
            if (this._meta) {
                document.getElementById('import-new-loc-path').value = this._meta.root_path;
                const parts = this._meta.root_path.split('/');
                document.getElementById('import-new-loc-name').value = parts[parts.length - 1] || this._meta.root_path;
            }
        } else {
            newFields.classList.add('hidden');
        }
    },

    async _doImport() {
        const errEl = document.getElementById('import-config-error');
        errEl.classList.add('hidden');

        const locVal = document.getElementById('import-location').value;
        const agentId = parseInt(document.getElementById('import-agent').value);

        const body = { temp_path: this._tempPath };

        if (locVal === '__new__') {
            const name = document.getElementById('import-new-loc-name').value.trim();
            const path = document.getElementById('import-new-loc-path').value.trim();
            if (!name || !path) {
                errEl.textContent = 'Name and path are required';
                errEl.classList.remove('hidden');
                return;
            }
            body.agent_id = agentId;
            body.location_name = name;
            body.root_path = path;
        } else {
            body.location_id = parseInt(locVal);
        }

        const btn = document.getElementById('import-run-btn');
        btn.disabled = true;

        const res = await API.post('/api/import-catalog/run', body);
        if (!res.ok) {
            errEl.textContent = res.error || 'Import failed to start';
            errEl.classList.remove('hidden');
            btn.disabled = false;
            return;
        }

        this._showStep('progress');
        this._startPolling();
    },

    _startPolling() {
        const fillEl = document.getElementById('import-progress-fill');
        const textEl = document.getElementById('import-progress-text');
        let lastImported = 0;
        let lastTime = Date.now();

        this._pollTimer = setInterval(async () => {
            const res = await API.get('/api/import-catalog/progress');
            if (!res.ok) return;

            const p = res.data;
            const pct = p.files_total > 0
                ? Math.round((p.files_imported / p.files_total) * 100)
                : 0;

            fillEl.style.width = pct + '%';

            // Rate calculation
            const now = Date.now();
            const dt = (now - lastTime) / 1000;
            const rate = dt > 0 ? Math.round((p.files_imported - lastImported) / dt) : 0;
            lastImported = p.files_imported;
            lastTime = now;

            if (p.status === 'pausing') {
                textEl.textContent = 'Waiting for running operations to finish...';
                fillEl.style.width = '0%';
            } else if (p.status === 'recalculating') {
                textEl.textContent = `Recalculating sizes... (${p.files_imported.toLocaleString()} files imported)`;
                fillEl.style.width = '100%';
            } else if (p.status === 'running') {
                if (p.files_imported === 0 && p.folders_created > 0) {
                    textEl.textContent = `Creating folders... ${p.folders_created.toLocaleString()} created`;
                } else {
                    textEl.textContent =
                        `${p.files_imported.toLocaleString()} / ${p.files_total.toLocaleString()} files` +
                        (rate > 0 ? ` (${rate.toLocaleString()}/sec)` : '');
                }
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
        const el = document.getElementById('import-done-text');
        if (p.status === 'error') {
            el.innerHTML = `<span style="color:var(--color-status-error)">Import failed: ${p.error}</span>`;
        } else {
            el.innerHTML =
                `Import complete.<br>` +
                `${p.files_imported.toLocaleString()} files, ` +
                `${p.folders_created.toLocaleString()} folders imported.`;
        }
        this._showStep('done');
    },
};

export default ImportCatalog;
