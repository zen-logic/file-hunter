import API from '../api.js';
import { loadThemes, applyTheme, isBuiltIn, clearThemeCache } from '../themes.js';
import ConfirmModal from './confirm.js';
import ThemeEditor from './theme-editor.js';
import Toast from './toast.js';
import Update from './update.js';
import RepairCatalog from './repaircatalog.js';

const Settings = {
    currentUser: null,

    open(currentUser) {
        this.currentUser = currentUser;
        const modal = document.getElementById('settings-modal');
        modal.classList.remove('hidden');
        this.render();
    },

    close() {
        document.getElementById('settings-modal').classList.add('hidden');
    },

    async render() {
        const content = document.getElementById('settings-content');
        content.innerHTML = '<div class="detail-loading"><div class="detail-spinner"></div>Loading…</div>';

        const [settingsRes, usersRes, appsRes, proRes, themes] = await Promise.all([
            API.get('/api/settings'),
            API.get('/api/auth/users'),
            API.get('/api/auth/apps'),
            API.get('/api/pro/status'),
            loadThemes(),
        ]);
        const themeNames = themes.map(t => t.name);

        const settings = settingsRes.ok ? settingsRes.data : {};
        const users = usersRes.ok ? usersRes.data : [];
        const apps = appsRes.ok ? appsRes.data : [];
        const proActive = proRes.ok && proRes.data.active;
        const savedTheme = localStorage.getItem('fh-theme') || 'default';

        content.innerHTML = `
            <div class="settings-section">
                <h3 class="settings-section-title">Server</h3>
                <div class="settings-row">
                    <label class="modal-label" for="settings-server-name">Server Name</label>
                    <div class="settings-inline">
                        <input type="text" class="modal-input" id="settings-server-name"
                               value="${this.esc(settings.serverName || '')}"
                               placeholder="e.g. My Archive Server">
                        <button class="btn btn-sm" id="settings-save-name">Save</button>
                    </div>
                </div>
            </div>
            <div class="settings-section">
                <h3 class="settings-section-title">Appearance</h3>
                <div class="settings-row">
                    <label class="modal-label" for="settings-theme">Theme</label>
                    <div class="settings-theme-controls">
                        <select class="search-select" id="settings-theme"></select>
                        <button class="btn btn-sm" id="settings-edit-theme">Edit</button>
                        <button class="btn btn-sm btn-danger" id="settings-delete-theme">Delete</button>
                    </div>
                    <button class="btn btn-sm" id="settings-new-theme">New Theme</button>
                </div>
            </div>
            <div class="settings-section">
                <h3 class="settings-section-title">Display</h3>
                <div class="settings-row">
                    <label class="modal-label">
                        <input type="checkbox" id="settings-show-hidden" ${settings.showHiddenFiles === '1' ? 'checked' : ''}>
                        Show hidden files and folders
                    </label>
                    <span class="settings-hint">Files and folders starting with a dot (e.g. .gitignore, .config)</span>
                </div>
            </div>
            <div class="settings-section">
                <h3 class="settings-section-title">Similarity Search</h3>
                <div class="settings-row">
                    <label class="modal-label">
                        <input type="checkbox" id="settings-similarity-enabled" ${settings.similaritySearchEnabled === '1' ? 'checked' : ''}>
                        Enable similarity search
                    </label>
                    <span class="settings-hint">Connect to an embedding service for image similarity search</span>
                </div>
                <div class="settings-row" id="settings-similarity-url-row" ${settings.similaritySearchEnabled === '1' ? '' : 'style="display:none"'}>
                    <label class="modal-label" for="settings-similarity-url">Embedding Service URL</label>
                    <div class="settings-inline">
                        <input type="text" class="modal-input" id="settings-similarity-url"
                               value="${this.esc(settings.similaritySearchUrl || '')}"
                               placeholder="e.g. http://hostname:8002">
                        <button class="btn btn-sm" id="settings-save-similarity">Save</button>
                    </div>
                </div>
            </div>
            <div class="settings-section">
                <h3 class="settings-section-title">Users</h3>
                <table class="settings-users-table">
                    <thead>
                        <tr><th>Username</th><th>Display Name</th><th></th></tr>
                    </thead>
                    <tbody id="settings-users-body"></tbody>
                </table>
                <button class="btn btn-sm" id="settings-add-user" style="margin-top:0.5rem">+ Add User</button>
                <button class="btn btn-sm" id="settings-add-app" style="margin-top:0.5rem">+ Add Application</button>
            </div>
            <div class="settings-section">
                <h3 class="settings-section-title">Applications</h3>
                <span class="settings-hint">API tokens for programmatic access. Use as a Bearer token in the Authorization header.</span>
                <table class="settings-users-table" id="settings-apps-table">
                    <thead>
                        <tr><th>Name</th><th>Token</th><th></th></tr>
                    </thead>
                    <tbody id="settings-apps-body"></tbody>
                </table>
            </div>
            <div class="settings-section">
                <h3 class="settings-section-title">Maintenance</h3>
                <div class="settings-row">
                    <button class="btn btn-sm" id="settings-repair-catalog">Repair Catalog</button>
                    <span class="settings-hint">Clears incorrect stale flags, recalculates file counts and duplicate detection</span>
                </div>
                <div class="settings-row">
                    <button class="btn btn-sm" id="settings-reset-queues">Reset Queues</button>
                    <span class="settings-hint">Removes temporary scan databases, clears all queued operations and pending hashes. Only available when no operations are running.</span>
                </div>
            </div>
            <div class="settings-section">
                ${proActive ? '<h3 class="settings-section-title">Pro Updates</h3>' : ''}
                <div class="settings-row">
                    <button class="btn btn-sm" id="settings-pro-updates">${proActive ? 'Manage Updates' : 'Upgrade to Pro'}</button>
                </div>
            </div>
            <div class="settings-section settings-footer">
                <button class="btn settings-btn-logout" id="settings-logout">Log Out</button>
            </div>
        `;

        // Theme selector
        const themeSelect = document.getElementById('settings-theme');
        const sorted = ['default', ...themeNames.filter(n => n !== 'default').sort()];
        for (const name of sorted) {
            const opt = document.createElement('option');
            opt.value = name;
            opt.textContent = name.replace(/-/g, ' ').replace(/\b\w/g, c => c.toUpperCase());
            themeSelect.appendChild(opt);
        }
        if (themeNames.includes(savedTheme)) themeSelect.value = savedTheme;

        const editBtn = document.getElementById('settings-edit-theme');
        const deleteBtn = document.getElementById('settings-delete-theme');
        const updateThemeButtons = () => {
            const builtIn = isBuiltIn(themeSelect.value);
            deleteBtn.disabled = builtIn || themeSelect.value === 'default';
        };
        updateThemeButtons();

        themeSelect.addEventListener('change', () => {
            applyTheme(themeSelect.value);
            updateThemeButtons();
        });

        const refreshThemeList = async () => {
            clearThemeCache();
            const fresh = await loadThemes();
            const names = fresh.map(t => t.name);
            themeSelect.innerHTML = '';
            const sorted = ['default', ...names.filter(n => n !== 'default').sort()];
            for (const name of sorted) {
                const opt = document.createElement('option');
                opt.value = name;
                opt.textContent = name.replace(/-/g, ' ').replace(/\b\w/g, c => c.toUpperCase());
                themeSelect.appendChild(opt);
            }
            const current = localStorage.getItem('fh-theme') || 'default';
            if (names.includes(current)) themeSelect.value = current;
            updateThemeButtons();
        };

        ThemeEditor.init(async (name) => {
            await refreshThemeList();
            themeSelect.value = name;
            updateThemeButtons();
        });

        editBtn.addEventListener('click', () => {
            ThemeEditor.open(themeSelect.value);
        });

        document.getElementById('settings-new-theme').addEventListener('click', () => {
            ThemeEditor.open(null);
        });

        deleteBtn.addEventListener('click', async () => {
            const current = themeSelect.value;
            if (isBuiltIn(current) || current === 'default') return;
            const ok = await ConfirmModal.open({
                title: 'Delete Theme',
                message: `Delete theme "${current}"?`,
                confirmLabel: 'Delete',
            });
            if (!ok) return;
            const res = await API.delete(`/api/themes/${current}`);
            if (!res.ok) {
                Toast.error(res.error || 'Failed to delete theme.');
                return;
            }
            applyTheme('default');
            await refreshThemeList();
        });

        // Users table
        this.renderUsers(users);

        // Applications table
        this.renderApps(apps);

        // Save server name
        document.getElementById('settings-save-name').addEventListener('click', async () => {
            const name = document.getElementById('settings-server-name').value.trim();
            await API.patch('/api/settings', { serverName: name });
        });

        // Show hidden files toggle
        document.getElementById('settings-show-hidden').addEventListener('change', async (e) => {
            await API.patch('/api/settings', { showHiddenFiles: e.target.checked });
        });

        // Similarity search
        document.getElementById('settings-similarity-enabled').addEventListener('change', async (e) => {
            const urlRow = document.getElementById('settings-similarity-url-row');
            urlRow.style.display = e.target.checked ? '' : 'none';
            await API.patch('/api/settings', { similaritySearchEnabled: e.target.checked });
        });
        document.getElementById('settings-save-similarity').addEventListener('click', async () => {
            const url = document.getElementById('settings-similarity-url').value.trim();
            await API.patch('/api/settings', { similaritySearchUrl: url });
        });

        // Add user / application
        document.getElementById('settings-add-user').addEventListener('click', () => this.showAddUser());
        document.getElementById('settings-add-app').addEventListener('click', () => this.showAddApp());

        // Repair catalog
        document.getElementById('settings-repair-catalog').addEventListener('click', () => {
            this.close();
            RepairCatalog.open();
        });

        // Reset queues
        document.getElementById('settings-reset-queues').addEventListener('click', async () => {
            const ok = await ConfirmModal.open({
                title: 'Reset Queues',
                message: 'Stop all operations and reset queues? This will cancel running scans, remove temporary databases, and clear all queued operations. This cannot be undone.',
                confirmLabel: 'Reset',
            });
            if (!ok) return;
            const res = await API.post('/api/maintenance/reset-queues');
            if (res.ok) {
                await ConfirmModal.open({
                    title: 'Reset Complete',
                    message: `${res.data.opsCancelled} operation(s) cancelled, ${res.data.tempFilesRemoved} temporary file(s) removed.`,
                    alert: true,
                });
            } else {
                await ConfirmModal.open({
                    title: 'Error',
                    message: res.error || 'Failed to reset queues.',
                    alert: true,
                });
            }
        });

        // Pro updates / upgrade
        document.getElementById('settings-pro-updates').addEventListener('click', () => {
            Update.open(settings.license_key || '', proActive);
        });

        // Logout
        document.getElementById('settings-logout').addEventListener('click', async () => {
            await API.post('/api/auth/logout');
            localStorage.removeItem('fh-token');
            location.reload();
        });
    },

    renderUsers(users) {
        const tbody = document.getElementById('settings-users-body');
        if (!tbody) return;
        tbody.innerHTML = '';
        for (const user of users) {
            const tr = document.createElement('tr');
            const isSelf = this.currentUser && this.currentUser.id === user.id;
            tr.innerHTML = `
                <td>${this.esc(user.username)}${isSelf ? ' <em>(you)</em>' : ''}</td>
                <td>${this.esc(user.displayName || '')}</td>
                <td class="settings-user-actions">
                    <button class="btn btn-sm settings-edit-user" data-id="${user.id}">Edit</button>
                    ${isSelf ? '' : `<button class="btn btn-sm btn-danger settings-delete-user" data-id="${user.id}">Delete</button>`}
                </td>
            `;
            tbody.appendChild(tr);
        }

        tbody.querySelectorAll('.settings-edit-user').forEach(btn => {
            btn.addEventListener('click', () => {
                const id = parseInt(btn.dataset.id, 10);
                const user = users.find(u => u.id === id);
                if (user) this.showEditUser(user);
            });
        });

        tbody.querySelectorAll('.settings-delete-user').forEach(btn => {
            btn.addEventListener('click', async () => {
                const id = parseInt(btn.dataset.id, 10);
                const user = users.find(u => u.id === id);
                if (user) {
                    const ok = await ConfirmModal.open({
                        title: 'Delete User',
                        message: `Delete user "${user.username}"?`,
                        confirmLabel: 'Delete',
                    });
                    if (ok) {
                        await API.delete(`/api/auth/users/${id}`);
                        this.render();
                    }
                }
            });
        });
    },

    showAddUser() {
        // Remove any existing form
        const existing = document.getElementById('settings-user-form');
        if (existing) existing.remove();

        const form = document.createElement('div');
        form.id = 'settings-user-form';
        form.className = 'settings-user-form';
        form.innerHTML = `
            <div class="settings-user-form-fields">
                <input type="text" class="modal-input" id="new-user-username" placeholder="Username" autocomplete="off">
                <input type="text" class="modal-input" id="new-user-display" placeholder="Display Name" autocomplete="off">
                <input type="password" class="modal-input modal-input-full" id="new-user-password" placeholder="Password" autocomplete="new-password">
            </div>
            <div class="settings-user-form-actions">
                <button class="btn btn-sm btn-primary" id="new-user-save">Save</button>
                <button class="btn btn-sm" id="new-user-cancel">Cancel</button>
            </div>
            <p class="modal-error hidden" id="new-user-error"></p>
        `;
        const table = document.querySelector('.settings-users-table');
        table.parentNode.insertBefore(form, table.nextSibling);

        document.getElementById('new-user-username').focus();
        document.getElementById('new-user-cancel').addEventListener('click', () => form.remove());
        document.getElementById('new-user-save').addEventListener('click', async () => {
            const username = document.getElementById('new-user-username').value.trim();
            const displayName = document.getElementById('new-user-display').value.trim();
            const password = document.getElementById('new-user-password').value;
            const errEl = document.getElementById('new-user-error');

            if (!username || !password) {
                errEl.textContent = 'Username and password required.';
                errEl.classList.remove('hidden');
                return;
            }

            const res = await API.post('/api/auth/users', { username, password, displayName });
            if (res.ok) {
                this.render();
            } else {
                errEl.textContent = res.error || 'Failed to create user.';
                errEl.classList.remove('hidden');
            }
        });
    },

    showEditUser(user) {
        // Remove any existing form
        const existingForm = document.getElementById('settings-user-form');
        if (existingForm) existingForm.remove();

        const tbody = document.getElementById('settings-users-body');
        // Find the row for this user and highlight it
        const rows = tbody.querySelectorAll('tr');
        for (const row of rows) {
            const editBtn = row.querySelector(`.settings-edit-user[data-id="${user.id}"]`);
            if (editBtn) {
                const form = document.createElement('div');
                form.id = 'settings-user-form';
                form.className = 'settings-user-form';
                form.innerHTML = `
                    <div class="settings-user-form-fields">
                        <input type="text" class="modal-input" id="edit-user-username" value="${this.esc(user.username)}" autocomplete="off">
                        <input type="text" class="modal-input" id="edit-user-display" value="${this.esc(user.displayName || '')}" placeholder="Display Name" autocomplete="off">
                        <input type="password" class="modal-input modal-input-full" id="edit-user-password" placeholder="New password (leave blank to keep)" autocomplete="new-password">
                    </div>
                    <div class="settings-user-form-actions">
                        <button class="btn btn-sm btn-primary" id="edit-user-save">Save</button>
                        <button class="btn btn-sm" id="edit-user-cancel">Cancel</button>
                    </div>
                    <p class="modal-error hidden" id="edit-user-error"></p>
                `;
                // Insert form after the table
                const table = document.querySelector('.settings-users-table');
                table.parentNode.insertBefore(form, table.nextSibling);

                document.getElementById('edit-user-username').focus();
                document.getElementById('edit-user-cancel').addEventListener('click', () => this.render());
                document.getElementById('edit-user-save').addEventListener('click', async () => {
                    const username = document.getElementById('edit-user-username').value.trim();
                    const displayName = document.getElementById('edit-user-display').value.trim();
                    const password = document.getElementById('edit-user-password').value;
                    const errEl = document.getElementById('edit-user-error');

                    if (!username) {
                        errEl.textContent = 'Username is required.';
                        errEl.classList.remove('hidden');
                        return;
                    }

                    const body = { username, displayName };
                    if (password) body.password = password;

                    const res = await API.patch(`/api/auth/users/${user.id}`, body);
                    if (res.ok) {
                        // If editing self, update cached user
                        if (this.currentUser && this.currentUser.id === user.id) {
                            this.currentUser.username = username;
                            this.currentUser.displayName = displayName;
                        }
                        this.render();
                    } else {
                        errEl.textContent = res.error || 'Failed to update user.';
                        errEl.classList.remove('hidden');
                    }
                });
                break;
            }
        }
    },

    renderApps(apps) {
        const tbody = document.getElementById('settings-apps-body');
        if (!tbody) return;
        const table = document.getElementById('settings-apps-table');
        if (!apps.length) {
            table.classList.add('hidden');
            return;
        }
        table.classList.remove('hidden');
        tbody.innerHTML = '';
        for (const app of apps) {
            const tr = document.createElement('tr');
            tr.innerHTML = `
                <td>${this.esc(app.name)}</td>
                <td><code class="settings-token">${this.esc(app.token)}</code></td>
                <td class="settings-user-actions">
                    <button class="btn btn-sm settings-copy-token" data-token="${this.esc(app.token)}">Copy</button>
                    <button class="btn btn-sm settings-regen-app" data-id="${app.id}" data-name="${this.esc(app.name)}">Regenerate</button>
                    <button class="btn btn-sm btn-danger settings-delete-app" data-id="${app.id}" data-name="${this.esc(app.name)}">Delete</button>
                </td>
            `;
            tbody.appendChild(tr);
        }

        tbody.querySelectorAll('.settings-copy-token').forEach(btn => {
            btn.addEventListener('click', () => {
                const text = btn.dataset.token;
                if (navigator.clipboard && navigator.clipboard.writeText) {
                    navigator.clipboard.writeText(text).then(() => {
                        btn.textContent = 'Copied';
                        setTimeout(() => { btn.textContent = 'Copy'; }, 2000);
                    }).catch(() => this.fallbackCopy(text, btn));
                } else {
                    this.fallbackCopy(text, btn);
                }
            });
        });

        tbody.querySelectorAll('.settings-regen-app').forEach(btn => {
            btn.addEventListener('click', async () => {
                const id = parseInt(btn.dataset.id, 10);
                const name = btn.dataset.name;
                const ok = await ConfirmModal.open({
                    title: 'Regenerate Token',
                    message: `Regenerate the API token for "${name}"? The current token will stop working immediately.`,
                    confirmLabel: 'Regenerate',
                });
                if (!ok) return;
                const res = await API.post(`/api/auth/apps/${id}/regenerate`);
                if (res.ok) {
                    this.render();
                } else {
                    Toast.error(res.error || 'Failed to regenerate token.');
                }
            });
        });

        tbody.querySelectorAll('.settings-delete-app').forEach(btn => {
            btn.addEventListener('click', async () => {
                const id = parseInt(btn.dataset.id, 10);
                const name = btn.dataset.name;
                const ok = await ConfirmModal.open({
                    title: 'Delete Application',
                    message: `Delete application "${name}"? Any integrations using this token will stop working.`,
                    confirmLabel: 'Delete',
                });
                if (ok) {
                    await API.delete(`/api/auth/apps/${id}`);
                    this.render();
                }
            });
        });
    },

    showAddApp() {
        const existing = document.getElementById('settings-app-form');
        if (existing) existing.remove();

        const form = document.createElement('div');
        form.id = 'settings-app-form';
        form.className = 'settings-user-form';
        form.innerHTML = `
            <div class="settings-user-form-fields">
                <input type="text" class="modal-input" id="new-app-name" placeholder="Application name" autocomplete="off">
            </div>
            <div class="settings-user-form-actions">
                <button class="btn btn-sm btn-primary" id="new-app-save">Create</button>
                <button class="btn btn-sm" id="new-app-cancel">Cancel</button>
            </div>
            <p class="modal-error hidden" id="new-app-error"></p>
        `;
        const table = document.getElementById('settings-apps-table');
        table.parentNode.insertBefore(form, table.nextSibling);

        document.getElementById('new-app-name').focus();
        document.getElementById('new-app-cancel').addEventListener('click', () => form.remove());
        document.getElementById('new-app-save').addEventListener('click', async () => {
            const name = document.getElementById('new-app-name').value.trim();
            const errEl = document.getElementById('new-app-error');

            if (!name) {
                errEl.textContent = 'Application name is required.';
                errEl.classList.remove('hidden');
                return;
            }

            const res = await API.post('/api/auth/apps', { name });
            if (res.ok) {
                this.render();
            } else {
                errEl.textContent = res.error || 'Failed to create application.';
                errEl.classList.remove('hidden');
            }
        });
    },

    fallbackCopy(text, btn) {
        const ta = document.createElement('textarea');
        ta.value = text;
        ta.style.position = 'fixed';
        ta.style.opacity = '0';
        document.body.appendChild(ta);
        ta.select();
        document.execCommand('copy');
        document.body.removeChild(ta);
        btn.textContent = 'Copied';
        setTimeout(() => { btn.textContent = 'Copy'; }, 2000);
    },

    esc(s) {
        const d = document.createElement('div');
        d.textContent = s;
        return d.innerHTML;
    },
};

export default Settings;
