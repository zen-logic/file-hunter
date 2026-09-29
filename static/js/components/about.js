import API from '../api.js';
import WS from '../ws.js';
import { esc } from '../format.js';
import { wireModal } from './modal.js';

const About = {
    modal: null,
    content: null,

    init() {
        this.modal = document.getElementById('about-modal');
        this.content = document.getElementById('about-content');

        wireModal(this.modal, {
            close: () => this.close(),
            cancelBtn: document.getElementById('about-close'),
        });
    },

    async open() {
        let version = '…';
        let pro = false;
        const res = await API.get('/api/version');
        if (res.ok) { version = res.data.version; pro = res.data.pro; }

        this.content.innerHTML = `
            <div class="about-info">
                <div class="about-version">File Hunter v${esc(version)}${pro ? ' (Pro)' : ''}</div>
                <p class="about-desc">File cataloging and deduplication tool for managing large removable and archival storage.</p>
                <p class="about-links">
                    <a href="https://github.com/zen-logic/file-hunter" target="blank" rel="noopener">GitHub</a>
                </p>
                <p class="about-copyright">&copy; 2026 <a href="https://zenlogic.co.uk" target="blank" rel="noopener">Zen Logic Ltd.</a></p>
                <div id="about-update" style="margin-top: 1rem;">
                    <button id="about-check-update" class="btn btn-sm">Check for updates</button>
                </div>
            </div>
        `;

        this.content.querySelector('#about-check-update').addEventListener('click', () => this.checkForUpdate());
        this.modal.classList.remove('hidden');
    },

    async checkForUpdate() {
        const el = document.getElementById('about-update');
        el.innerHTML = '<span style="color: var(--color-text-secondary);">Checking…</span>';

        const res = await API.get('/api/update/check-release');
        if (!res.ok) {
            el.innerHTML = `<span style="color: var(--color-error);">${esc(res.error || 'Could not check for updates')}</span>`;
            return;
        }

        const { current, latest, update_available } = res.data;
        if (!update_available) {
            el.innerHTML = `<span style="color: var(--color-text-secondary);">You are running the latest version (v${esc(current)}).</span>`;
            return;
        }

        el.innerHTML = `
            <span>A new update is available: <strong>v${esc(latest)}</strong> (current: v${esc(current)})</span>
            <div style="margin-top: 0.5rem;">
                <button id="about-apply-update" class="btn btn-sm btn-primary">Install update</button>
            </div>
        `;
        el.querySelector('#about-apply-update').addEventListener('click', () => this.applyUpdate());
    },

    async applyUpdate() {
        const el = document.getElementById('about-update');
        el.innerHTML = '<span style="color: var(--color-text-secondary);">Downloading and installing…</span>';

        const res = await API.post('/api/update/apply-release');
        if (!res.ok) {
            el.innerHTML = `<span style="color: var(--color-error);">${esc(res.error || 'Update failed')}</span>`;
            return;
        }

        el.innerHTML = '<span style="color: var(--color-text-secondary);">Update installed. Restarting server…</span>';
        this.waitForRestart();
    },

    waitForRestart() {
        WS.on('__open', () => location.reload());
    },

    close() {
        this.modal.classList.add('hidden');
    },
};

export default About;
