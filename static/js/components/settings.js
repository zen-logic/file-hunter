import API from '../api.js';
import { themeNames, applyTheme } from '../themes.js';
import Update from './update.js';
import RepairCatalog from './repaircatalog.js';

const Settings = {
    _currentUser: null,

    open(currentUser) {
        this._currentUser = currentUser;
        const modal = document.getElementById('settings-modal');
        modal.classList.remove('hidden');
        this._render();
    },

    close() {
        document.getElementById('settings-modal').classList.add('hidden');
    },

    async _render() {
        const content = document.getElementById('settings-content');
        content.innerHTML = '<div class="detail-loading"><div class="detail-spinner"></div>Loading…</div>';

        const [settingsRes, usersRes, proRes] = await Promise.all([
            API.get('/api/settings'),
            API.get('/api/auth/users'),
            API.get('/api/pro/status'),
        ]);

        const settings = settingsRes.ok ? settingsRes.data : {};
        const users = usersRes.ok ? usersRes.data : [];
        const proActive = proRes.ok && proRes.data.active;
        const savedTheme = localStorage.getItem('fh-theme') || 'default';

        content.innerHTML = `
            <div class="settings-section">
                <h3 class="settings-section-title">Server</h3>
                <div class="settings-row">
                    <label class="modal-label" for="settings-server-name">Server Name</label>
                    <div class="settings-inline">
                        <input type="text" class="modal-input" id="settings-server-name"
                               value="${this._esc(settings.serverName || '')}"
                               placeholder="e.g. My Archive Server">
                        <button class="btn btn-sm" id="settings-save-name">Save</button>
                    </div>
                </div>
            </div>
            <div class="settings-section">
                <h3 class="settings-section-title">Appearance</h3>
                <div class="settings-row">
                    <label class="modal-label" for="settings-theme">Theme</label>
                    <select class="search-select" id="settings-theme"></select>
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
                <h3 class="settings-section-title">Users</h3>
                <table class="settings-users-table">
                    <thead>
                        <tr><th>Username</th><th>Display Name</th><th></th></tr>
                    </thead>
                    <tbody id="settings-users-body"></tbody>
                </table>
                <button class="btn btn-sm" id="settings-add-user" style="margin-top:0.5rem">+ Add User</button>
            </div>
            <div class="settings-section">
                <h3 class="settings-section-title">Maintenance</h3>
                <div class="settings-row">
                    <button class="btn btn-sm" id="settings-repair-catalog">Repair Catalog</button>
                    <span class="settings-hint">Clears incorrect stale flags, recalculates file counts and duplicate detection</span>
                </div>
                <div class="settings-row">
                    <button class="btn btn-sm btn-danger" id="settings-reset-queues">Reset Queues</button>
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
            opt.textContent = name.replace(/\b\w/g, c => c.toUpperCase());
            themeSelect.appendChild(opt);
        }
        if (themeNames.includes(savedTheme)) themeSelect.value = savedTheme;
        themeSelect.addEventListener('change', () => applyTheme(themeSelect.value));

        // Users table
        this._renderUsers(users);

        // Save server name
        document.getElementById('settings-save-name').addEventListener('click', async () => {
            const name = document.getElementById('settings-server-name').value.trim();
            await API.patch('/api/settings', { serverName: name });
        });

        // Show hidden files toggle
        document.getElementById('settings-show-hidden').addEventListener('change', async (e) => {
            await API.patch('/api/settings', { showHiddenFiles: e.target.checked });
        });

        // Add user
        document.getElementById('settings-add-user').addEventListener('click', () => this._showAddUser());

        // Repair catalog
        document.getElementById('settings-repair-catalog').addEventListener('click', () => {
            this.close();
            RepairCatalog.open();
        });

        // Reset queues
        document.getElementById('settings-reset-queues').addEventListener('click', async () => {
            if (!confirm('Stop all operations and reset queues? This will cancel running scans, remove temporary databases, and clear all queued operations. This cannot be undone.')) return;
            const res = await API.post('/api/maintenance/reset-queues');
            if (res.ok) {
                alert(`Reset complete. ${res.data.opsCancelled} operation(s) cancelled, ${res.data.tempFilesRemoved} temporary file(s) removed.`);
            } else {
                alert(res.error || 'Failed to reset queues.');
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

    _renderUsers(users) {
        const tbody = document.getElementById('settings-users-body');
        if (!tbody) return;
        tbody.innerHTML = '';
        for (const user of users) {
            const tr = document.createElement('tr');
            const isSelf = this._currentUser && this._currentUser.id === user.id;
            tr.innerHTML = `
                <td>${this._esc(user.username)}${isSelf ? ' <em>(you)</em>' : ''}</td>
                <td>${this._esc(user.displayName || '')}</td>
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
                if (user) this._showEditUser(user);
            });
        });

        tbody.querySelectorAll('.settings-delete-user').forEach(btn => {
            btn.addEventListener('click', async () => {
                const id = parseInt(btn.dataset.id, 10);
                const user = users.find(u => u.id === id);
                if (user && confirm(`Delete user "${user.username}"?`)) {
                    await API.delete(`/api/auth/users/${id}`);
                    this._render();
                }
            });
        });
    },

    _showAddUser() {
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
                this._render();
            } else {
                errEl.textContent = res.error || 'Failed to create user.';
                errEl.classList.remove('hidden');
            }
        });
    },

    _showEditUser(user) {
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
                        <input type="text" class="modal-input" id="edit-user-username" value="${this._esc(user.username)}" autocomplete="off">
                        <input type="text" class="modal-input" id="edit-user-display" value="${this._esc(user.displayName || '')}" placeholder="Display Name" autocomplete="off">
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
                document.getElementById('edit-user-cancel').addEventListener('click', () => this._render());
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
                        if (this._currentUser && this._currentUser.id === user.id) {
                            this._currentUser.username = username;
                            this._currentUser.displayName = displayName;
                        }
                        this._render();
                    } else {
                        errEl.textContent = res.error || 'Failed to update user.';
                        errEl.classList.remove('hidden');
                    }
                });
                break;
            }
        }
    },

    _esc(s) {
        const d = document.createElement('div');
        d.textContent = s;
        return d.innerHTML;
    },
};

export default Settings;
