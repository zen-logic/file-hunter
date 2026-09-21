import API from '../api.js';
import WS from '../ws.js';

const Update = {
    overlay: null,

    async open(savedKey, proActive) {
        if (this.overlay) return;

        const title = proActive ? 'Pro Updates' : 'Upgrade to Pro';
        const overlay = document.createElement('div');
        overlay.className = 'modal-overlay';
        overlay.id = 'update-modal';

        if (proActive) {
            overlay.innerHTML = this.proLayout(title, savedKey);
        } else {
            overlay.innerHTML = this.upgradeLayout(title, savedKey);
        }

        document.body.appendChild(overlay);
        this.overlay = overlay;

        // Close handlers
        overlay.addEventListener('click', (e) => {
            if (e.target === overlay) this.close();
        });
        document.getElementById('update-close').addEventListener('click', () => this.close());

        if (proActive) {
            this.bindProEvents();
            await this.showCurrentStatus();
        } else {
            this.bindUpgradeEvents();
        }
    },

    // ── Free mode: single "Upgrade" button ──────────────────────────

    upgradeLayout(title, savedKey) {
        return `
            <div class="modal-dialog" style="width:440px">
                <div class="settings-header">
                    <h2 class="modal-title">${title}</h2>
                    <button class="btn btn-sm" id="update-close">&times;</button>
                </div>
                <div id="update-body">
                    <div class="modal-field">
                        <label class="modal-label">License Key</label>
                        <input type="text" class="modal-input" id="update-key"
                               placeholder="xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
                               value="${this.esc(savedKey || '')}"
                               spellcheck="false" autocomplete="off">
                    </div>
                    <div style="margin-top:0.75rem">
                        <button class="btn btn-sm btn-primary" id="update-upgrade">Upgrade</button>
                    </div>
                    <div style="margin-top:0.75rem;display:flex;align-items:center;gap:0.5rem;font-size:var(--font-size-sm);color:var(--color-text-muted)">
                        <hr style="flex:1;border:none;border-top:1px solid var(--color-border)">
                        <span>or upload package</span>
                        <hr style="flex:1;border:none;border-top:1px solid var(--color-border)">
                    </div>
                    <div style="display:flex;align-items:center;gap:0.5rem;margin-top:0.5rem">
                        <input type="file" id="update-file" accept=".filehunter,.zip" style="font-size:var(--font-size-sm)">
                        <button class="btn btn-sm btn-primary hidden" id="update-upload">Upload</button>
                    </div>
                    <div id="update-status" class="hidden" style="margin-top:0.75rem;font-size:var(--font-size-sm)"></div>
                </div>
            </div>`;
    },

    bindUpgradeEvents() {
        document.getElementById('update-upgrade').addEventListener('click', () => this.upgrade());
        document.getElementById('update-file').addEventListener('change', (e) => {
            const uploadBtn = document.getElementById('update-upload');
            if (e.target.files.length) {
                uploadBtn.classList.remove('hidden');
            } else {
                uploadBtn.classList.add('hidden');
            }
        });
        document.getElementById('update-upload').addEventListener('click', () => this.uploadFile());
    },

    async upgrade() {
        const key = document.getElementById('update-key').value.trim();
        if (!key) {
            this.setStatus('Enter your license key.', 'error');
            return;
        }

        const btn = document.getElementById('update-upgrade');
        btn.disabled = true;

        // Save key, validate, then install — one click
        this.setStatus('Validating…', 'muted');
        await API.patch('/api/settings', { license_key: key });

        const check = await API.post('/api/update/check', { key });
        if (!check.ok) {
            this.setStatus(check.error || 'Invalid key.', 'error');
            btn.disabled = false;
            return;
        }

        this.setStatus('Downloading and installing…', 'muted');
        const res = await API.post('/api/update/install', { key });

        if (res.ok) {
            btn.classList.add('hidden');
            this.setStatus(res.data.message, 'success');
            this.showRestartButton();
        } else {
            btn.disabled = false;
            this.setStatus(res.error || 'Install failed.', 'error');
        }
    },

    async uploadFile() {
        const fileInput = document.getElementById('update-file');
        const file = fileInput.files[0];
        if (!file) {
            this.setStatus('Choose a .filehunter package first.', 'error');
            return;
        }

        const uploadBtn = document.getElementById('update-upload');
        const otherBtns = ['update-upgrade', 'update-check', 'update-install']
            .map(id => document.getElementById(id)).filter(Boolean);
        uploadBtn.disabled = true;
        otherBtns.forEach(b => b.disabled = true);
        this.setStatus('Uploading and installing…', 'muted');

        const form = new FormData();
        form.append('file', file);

        const token = localStorage.getItem('fh-token');
        try {
            const resp = await fetch('/api/update/upload', {
                method: 'POST',
                headers: token ? { 'Authorization': `Bearer ${token}` } : {},
                body: form,
            });
            const res = await resp.json();

            if (res.ok) {
                uploadBtn.classList.add('hidden');
                otherBtns.forEach(b => b.classList.add('hidden'));
                this.setStatus(res.data.message, 'success');
                this.showRestartButton();
            } else {
                uploadBtn.disabled = false;
                otherBtns.forEach(b => b.disabled = false);
                this.setStatus(res.error || 'Upload failed.', 'error');
            }
        } catch (e) {
            uploadBtn.disabled = false;
            otherBtns.forEach(b => b.disabled = false);
            this.setStatus('Upload failed — could not reach server.', 'error');
        }
    },

    // ── Pro mode: check + install ───────────────────────────────────

    proLayout(title, savedKey) {
        return `
            <div class="modal-dialog" style="width:440px">
                <div class="settings-header">
                    <h2 class="modal-title">${title}</h2>
                    <button class="btn btn-sm" id="update-close">&times;</button>
                </div>
                <div id="update-body">
                    <div class="modal-field">
                        <label class="modal-label">License Key</label>
                        <div class="settings-inline">
                            <input type="text" class="modal-input" id="update-key"
                                   placeholder="xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
                                   value="${this.esc(savedKey || '')}"
                                   spellcheck="false" autocomplete="off">
                            <button class="btn btn-sm" id="update-save-key">Save</button>
                        </div>
                    </div>
                    <div style="display:flex;gap:0.5rem;margin-top:0.75rem">
                        <button class="btn btn-sm btn-primary" id="update-check">Check for Updates</button>
                        <button class="btn btn-sm hidden" id="update-install">Install</button>
                    </div>
                    <div style="margin-top:0.75rem;display:flex;align-items:center;gap:0.5rem;font-size:var(--font-size-sm);color:var(--color-text-muted)">
                        <hr style="flex:1;border:none;border-top:1px solid var(--color-border)">
                        <span>or upload package</span>
                        <hr style="flex:1;border:none;border-top:1px solid var(--color-border)">
                    </div>
                    <div style="display:flex;align-items:center;gap:0.5rem;margin-top:0.5rem">
                        <input type="file" id="update-file" accept=".filehunter,.zip" style="font-size:var(--font-size-sm)">
                        <button class="btn btn-sm btn-primary hidden" id="update-upload">Upload</button>
                    </div>
                    <div id="update-status" class="hidden" style="margin-top:0.75rem;font-size:var(--font-size-sm)"></div>
                </div>
            </div>`;
    },

    bindProEvents() {
        document.getElementById('update-save-key').addEventListener('click', () => this.saveKey());
        document.getElementById('update-check').addEventListener('click', () => this.check());
        document.getElementById('update-install').addEventListener('click', () => this.install());
        document.getElementById('update-file').addEventListener('change', (e) => {
            const uploadBtn = document.getElementById('update-upload');
            if (e.target.files.length) {
                uploadBtn.classList.remove('hidden');
            } else {
                uploadBtn.classList.add('hidden');
            }
        });
        document.getElementById('update-upload').addEventListener('click', () => this.uploadFile());
    },

    async saveKey() {
        const key = document.getElementById('update-key').value.trim();
        const res = await API.patch('/api/settings', { license_key: key });
        if (res.ok) {
            this.setStatus('Key saved.', 'muted');
        } else {
            this.setStatus(res.error || 'Failed to save key.', 'error');
        }
    },

    async showCurrentStatus() {
        const res = await API.get('/api/pro/status');
        if (res.ok && res.data.active) {
            const ver = res.data.version ? ` (v${res.data.version})` : '';
            this.setStatus(`Pro installed${ver}`, 'success');
        }
    },

    async check() {
        const key = document.getElementById('update-key').value.trim();
        if (!key) {
            this.setStatus('Enter a license key first.', 'error');
            return;
        }

        this.setStatus('Checking…', 'muted');
        const checkBtn = document.getElementById('update-check');
        checkBtn.disabled = true;

        const res = await API.post('/api/update/check', { key });
        checkBtn.disabled = false;

        if (res.ok) {
            this.setStatus(`Available: v${res.data.version} (${res.data.filename})`, 'success');
            const installBtn = document.getElementById('update-install');
            installBtn.classList.remove('hidden');
            installBtn.dataset.version = res.data.version;
        } else {
            this.setStatus(res.error || 'Check failed.', 'error');
            document.getElementById('update-install').classList.add('hidden');
        }
    },

    async install() {
        const key = document.getElementById('update-key').value.trim();
        if (!key) return;

        const installBtn = document.getElementById('update-install');
        const checkBtn = document.getElementById('update-check');
        installBtn.disabled = true;
        checkBtn.disabled = true;
        this.setStatus('Downloading and installing…', 'muted');

        const res = await API.post('/api/update/install', { key });

        if (res.ok) {
            installBtn.classList.add('hidden');
            checkBtn.disabled = true;
            this.setStatus(res.data.message, 'success');
            this.showRestartButton();
        } else {
            installBtn.disabled = false;
            checkBtn.disabled = false;
            this.setStatus(res.error || 'Install failed.', 'error');
        }
    },

    // ── Shared ──────────────────────────────────────────────────────

    close() {
        if (this.overlay) {
            this.overlay.remove();
            this.overlay = null;
        }
    },

    showRestartButton() {
        const container = document.getElementById('update-body');
        if (!container || document.getElementById('update-restart')) return;

        const btn = document.createElement('button');
        btn.id = 'update-restart';
        btn.className = 'btn btn-sm btn-primary';
        btn.textContent = 'Restart Now';
        btn.style.marginTop = '0.75rem';
        btn.addEventListener('click', () => this.restart());
        container.appendChild(btn);
    },

    async restart() {
        const btn = document.getElementById('update-restart');
        if (btn) btn.disabled = true;
        this.setStatus('Restarting…', 'muted');

        // Register reconnect handler before triggering restart —
        // the server will die mid-request, so the fetch may throw
        WS.on('__open', () => location.reload());

        // Fallback: poll the server in case WS reconnect doesn't fire
        const pollUntilUp = () => {
            const poll = setInterval(async () => {
                try {
                    const res = await fetch('/api/auth/status');
                    if (res.ok) {
                        clearInterval(poll);
                        location.reload();
                    }
                } catch {
                    // Server still down — keep polling
                }
            }, 3000);
        };
        // Start polling after a short delay to let the server die first
        setTimeout(pollUntilUp, 2000);

        try {
            await API.post('/api/restart');
        } catch {
            // Expected — server killed the connection
        }
    },

    setStatus(msg, type) {
        const el = document.getElementById('update-status');
        if (!el) return;
        el.classList.remove('hidden');
        el.textContent = msg;
        el.style.color =
            type === 'error' ? 'var(--color-status-error)' :
            type === 'success' ? 'var(--color-status-success)' :
            'var(--color-text-muted)';
    },

    esc(s) {
        const d = document.createElement('div');
        d.textContent = s;
        return d.innerHTML;
    },
};

export default Update;
