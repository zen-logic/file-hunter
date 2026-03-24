import API from '../api.js';

const About = {
    _modal: null,
    _content: null,

    init() {
        this._modal = document.getElementById('about-modal');
        this._content = document.getElementById('about-content');

        document.getElementById('about-close').addEventListener('click', () => this.close());
        this._modal.addEventListener('click', (e) => {
            if (e.target === this._modal) this.close();
        });
        document.addEventListener('keydown', (e) => {
            if (e.key === 'Escape' && !this._modal.classList.contains('hidden')) {
                this.close();
            }
        });
    },

    async open() {
        let version = '…';
        let pro = false;
        const res = await API.get('/api/version');
        if (res.ok) { version = res.data.version; pro = res.data.pro; }

        this._content.innerHTML = `
            <div class="about-info">
                <div class="about-version">File Hunter v${this._esc(version)}${pro ? ' (Pro)' : ''}</div>
                <p class="about-desc">File cataloging and deduplication tool for managing large removable and archival storage.</p>
                <p class="about-links">
                    <a href="https://github.com/zen-logic/file-hunter" target="_blank" rel="noopener">GitHub</a>
                </p>
                <p class="about-copyright">&copy; 2026 <a href="https://zenlogic.co.uk" target="_blank" rel="noopener">Zen Logic Ltd.</a></p>
                <div id="about-update" style="margin-top: 1rem;">
                    <button id="about-check-update" class="btn btn-sm">Check for updates</button>
                </div>
            </div>
        `;

        this._content.querySelector('#about-check-update').addEventListener('click', () => this._checkForUpdate());
        this._modal.classList.remove('hidden');
    },

    async _checkForUpdate() {
        const el = document.getElementById('about-update');
        el.innerHTML = '<span style="color: var(--color-text-secondary);">Checking…</span>';

        const res = await API.get('/api/update/check-release');
        if (!res.ok) {
            el.innerHTML = `<span style="color: var(--color-error);">${this._esc(res.error || 'Could not check for updates')}</span>`;
            return;
        }

        const { current, latest, update_available } = res.data;
        if (!update_available) {
            el.innerHTML = `<span style="color: var(--color-text-secondary);">You are running the latest released version (v${this._esc(current)}).</span>`;
            return;
        }

        el.innerHTML = `
            <span>A new update is available: <strong>v${this._esc(latest)}</strong> (current: v${this._esc(current)})</span>
            <div style="margin-top: 0.5rem;">
                <button id="about-apply-update" class="btn btn-sm btn-primary">Install update</button>
            </div>
        `;
        el.querySelector('#about-apply-update').addEventListener('click', () => this._applyUpdate());
    },

    async _applyUpdate() {
        const el = document.getElementById('about-update');
        el.innerHTML = '<span style="color: var(--color-text-secondary);">Downloading and installing…</span>';

        const res = await API.post('/api/update/apply-release');
        if (!res.ok) {
            el.innerHTML = `<span style="color: var(--color-error);">${this._esc(res.error || 'Update failed')}</span>`;
            return;
        }

        el.innerHTML = '<span style="color: var(--color-text-secondary);">Update installed. Restarting server…</span>';
        this._waitForRestart();
    },

    _waitForRestart() {
        let attempts = 0;
        const poll = setInterval(async () => {
            attempts++;
            if (attempts > 30) {
                clearInterval(poll);
                const el = document.getElementById('about-update');
                if (el) el.innerHTML = '<span style="color: var(--color-error);">Server did not come back. Check the server manually.</span>';
                return;
            }
            try {
                const res = await fetch('/api/version', { signal: AbortSignal.timeout(2000) });
                if (res.ok) {
                    clearInterval(poll);
                    window.location.reload();
                }
            } catch (_) {
                // Server still down, keep polling
            }
        }, 2000);
    },

    close() {
        this._modal.classList.add('hidden');
    },

    _esc(s) {
        const d = document.createElement('div');
        d.textContent = s;
        return d.innerHTML;
    },
};

export default About;
