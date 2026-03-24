import API from '../api.js';
import { loadThemeNames, applyTheme } from '../themes.js';

const Login = {
    _onAuthenticated: null,

    init(onAuthenticated) {
        this._onAuthenticated = onAuthenticated;
    },

    showSetup() {
        const screen = document.getElementById('login-screen');
        screen.classList.remove('hidden');
        document.getElementById('app').classList.add('hidden');
        screen.innerHTML = `
            <div class="login-card">
                <h1 class="login-title">Welcome to File Hunter</h1>
                <p class="login-subtitle">Create your admin account to get started.</p>
                <div class="login-field">
                    <label class="login-label" for="setup-username">Username</label>
                    <input type="text" class="modal-input" id="setup-username" autocomplete="username">
                </div>
                <div class="login-field">
                    <label class="login-label" for="setup-display">Display Name</label>
                    <input type="text" class="modal-input" id="setup-display" placeholder="Optional" autocomplete="name">
                </div>
                <div class="login-field">
                    <label class="login-label" for="setup-password">Password</label>
                    <input type="password" class="modal-input" id="setup-password" autocomplete="new-password">
                </div>
                <div class="login-field">
                    <label class="login-label" for="setup-confirm">Confirm Password</label>
                    <input type="password" class="modal-input" id="setup-confirm" autocomplete="new-password">
                </div>
                <p class="login-error hidden" id="setup-error"></p>
                <button class="btn btn-primary login-btn" id="setup-submit">Create Account</button>
                <div class="login-theme-row">
                    <label class="login-label" for="login-theme">Theme</label>
                    <select class="search-select" id="login-theme"></select>
                </div>
            </div>
        `;
        this._wireThemeSelector();
        const submit = document.getElementById('setup-submit');
        const inputs = screen.querySelectorAll('input');
        inputs.forEach(input => {
            input.addEventListener('keydown', (e) => {
                if (e.key === 'Enter') submit.click();
            });
        });
        submit.addEventListener('click', () => this._handleSetup());
        document.getElementById('setup-username').focus();
    },

    showLogin(serverName) {
        const screen = document.getElementById('login-screen');
        screen.classList.remove('hidden');
        document.getElementById('app').classList.add('hidden');
        const title = serverName ? `File Hunter — ${serverName}` : 'File Hunter';
        screen.innerHTML = `
            <div class="login-card">
                <h1 class="login-title">${this._esc(title)}</h1>
                <div class="login-field">
                    <label class="login-label" for="login-username">Username</label>
                    <input type="text" class="modal-input" id="login-username" autocomplete="username">
                </div>
                <div class="login-field">
                    <label class="login-label" for="login-password">Password</label>
                    <input type="password" class="modal-input" id="login-password" autocomplete="current-password">
                </div>
                <p class="login-error hidden" id="login-error"></p>
                <button class="btn btn-primary login-btn" id="login-submit">Log In</button>
            </div>
        `;
        const submit = document.getElementById('login-submit');
        const inputs = screen.querySelectorAll('input');
        inputs.forEach(input => {
            input.addEventListener('keydown', (e) => {
                if (e.key === 'Enter') submit.click();
            });
        });
        submit.addEventListener('click', () => this._handleLogin());
        document.getElementById('login-username').focus();
    },

    hide() {
        const screen = document.getElementById('login-screen');
        screen.classList.add('hidden');
        screen.innerHTML = '';
        document.getElementById('app').classList.remove('hidden');
    },

    async _handleSetup() {
        const username = document.getElementById('setup-username').value.trim();
        const displayName = document.getElementById('setup-display').value.trim();
        const password = document.getElementById('setup-password').value;
        const confirm = document.getElementById('setup-confirm').value;
        const errorEl = document.getElementById('setup-error');

        if (!username || !password) {
            this._showError(errorEl, 'Username and password are required.');
            return;
        }
        if (password !== confirm) {
            this._showError(errorEl, 'Passwords do not match.');
            return;
        }

        const submitBtn = document.getElementById('setup-submit');
        submitBtn.disabled = true;
        submitBtn.textContent = 'Creating…';

        const res = await API.post('/api/auth/setup', { username, password, displayName });
        if (res.ok) {
            localStorage.setItem('fh-token', res.data.token);
            this.hide();
            if (this._onAuthenticated) this._onAuthenticated(res.data.user);
        } else {
            submitBtn.disabled = false;
            submitBtn.textContent = 'Create Account';
            this._showError(errorEl, res.error || 'Setup failed.');
        }
    },

    async _handleLogin() {
        const username = document.getElementById('login-username').value.trim();
        const password = document.getElementById('login-password').value;
        const errorEl = document.getElementById('login-error');

        if (!username || !password) {
            this._showError(errorEl, 'Username and password are required.');
            return;
        }

        const submitBtn = document.getElementById('login-submit');
        submitBtn.disabled = true;
        submitBtn.textContent = 'Logging in…';

        const res = await API.post('/api/auth/login', { username, password });
        if (res.ok) {
            localStorage.setItem('fh-token', res.data.token);
            this.hide();
            if (this._onAuthenticated) this._onAuthenticated(res.data.user);
        } else {
            submitBtn.disabled = false;
            submitBtn.textContent = 'Log In';
            this._showError(errorEl, res.error || 'Login failed.');
        }
    },

    _showError(el, msg) {
        el.textContent = msg;
        el.classList.remove('hidden');
    },

    async _wireThemeSelector() {
        const sel = document.getElementById('login-theme');
        if (!sel) return;
        const themeNames = await loadThemeNames();
        const saved = localStorage.getItem('fh-theme') || 'default';
        const sorted = ['default', ...themeNames.filter(n => n !== 'default').sort()];
        for (const name of sorted) {
            const opt = document.createElement('option');
            opt.value = name;
            opt.textContent = name.replace(/-/g, ' ').replace(/\b\w/g, c => c.toUpperCase());
            sel.appendChild(opt);
        }
        if (themeNames.includes(saved)) sel.value = saved;
        sel.addEventListener('change', () => applyTheme(sel.value));
    },

    _esc(s) {
        const d = document.createElement('div');
        d.textContent = s;
        return d.innerHTML;
    },
};

export default Login;
