import API from '../api.js';

function formatSize(bytes) {
    if (bytes === null || bytes === undefined) return '';
    if (bytes < 1024) return bytes + ' B';
    if (bytes < 1048576) return (bytes / 1024).toFixed(1) + ' KB';
    if (bytes < 1073741824) return (bytes / 1048576).toFixed(1) + ' MB';
    if (bytes < 1099511627776) return (bytes / 1073741824).toFixed(1) + ' GB';
    if (bytes < 1125899906842624) return (bytes / 1099511627776).toFixed(1) + ' TB';
    return (bytes / 1125899906842624).toFixed(1) + ' PB';
}

const StatusBar = {
    statsEl: null,
    activityEl: null,
    connectionEl: null,
    _loadEl: null,
    _loadBarEl: null,
    _loadDropdown: null,
    _dropdownOpen: false,
    _activities: [],
    _scanningLocationId: null,
    _pendingQueue: [],
    _stats: null,

    init() {
        this.statsEl = document.getElementById('status-stats');
        this.activityEl = document.getElementById('status-activity');
        this.connectionEl = document.getElementById('status-connection');
        this._loadEl = document.getElementById('status-load');
        this._loadBarEl = document.getElementById('status-load-bar');
        this._loadDropdown = document.getElementById('status-load-dropdown');

        if (this._loadEl) {
            this._loadEl.addEventListener('click', (e) => {
                e.stopPropagation();
                this._toggleDropdown();
            });
        }
        if (this._loadDropdown) {
            this._loadDropdown.addEventListener('click', (e) => e.stopPropagation());
        }
        document.addEventListener('click', () => this._closeDropdown());

        this.loadStats();
        this.renderActivity('idle');
        this.renderConnection(false);
    },

    async loadStats() {
        const res = await API.get('/api/stats');
        if (res.ok) {
            this._stats = res.data;
            this._renderStats();
        }
    },

    updateStatsFromProgress(msg) {
        if (!this._stats) return;
        if (msg.globalFileCount !== undefined) this._stats.totalFiles = msg.globalFileCount;
        if (msg.globalTotalSize !== undefined) {
            this._stats.totalSize = msg.globalTotalSize;
            this._stats.totalSizeFormatted = formatSize(msg.globalTotalSize);
        }
        if (msg.globalDuplicateCount !== undefined) this._stats.duplicateFiles = msg.globalDuplicateCount;
        this._renderStats();
    },

    _renderStats() {
        const s = this._stats;
        const pendingHtml = s.pendingOps > 0
            ? `<span class="status-item"><span>Pending:</span><span class="status-value" style="color:var(--color-pending-text)">${s.pendingOps}</span></span>`
            : '';
        this.statsEl.innerHTML = `
            <span class="status-item">
                <span>Files:</span>
                <span class="status-value">${s.totalFiles.toLocaleString()}</span>
            </span>
            <span class="status-item">
                <span>Locations:</span>
                <span class="status-value">${s.totalLocations}</span>
            </span>
            <span class="status-item">
                <span>Duplicates:</span>
                <span class="status-value">${s.duplicateFiles.toLocaleString()}</span>
            </span>
            <span class="status-item">
                <span>Catalog:</span>
                <span class="status-value">${s.totalSizeFormatted}</span>
            </span>
            ${pendingHtml}
        `;
    },

    _renderQueueBadge() {
        const count = this._pendingQueue.length;
        if (count === 0) return '';
        return `<span class="status-queue-info">+${count} queued</span>`;
    },

    renderActivity(state, detail, locationId) {
        if (state === 'scanning') {
            this._scanningLocationId = locationId || this._scanningLocationId;
            this.activityEl.innerHTML = `
                <span class="status-activity-text scanning">
                    Scanning: ${detail || '...'}
                    ${this._renderQueueBadge()}
                </span>
            `;
        } else if (state === 'consolidating') {
            this._scanningLocationId = null;
            this.activityEl.innerHTML = `
                <span class="status-activity-text scanning">
                    Consolidating: ${detail || '...'}
                </span>
            `;
        } else if (state === 'uploading') {
            this._scanningLocationId = null;
            this.activityEl.innerHTML = `
                <span class="status-activity-text scanning">
                    Uploading: ${detail || '...'}
                </span>
            `;
        } else if (state === 'merging') {
            this._scanningLocationId = null;
            this.activityEl.innerHTML = `
                <span class="status-activity-text scanning">
                    Merging: ${detail || '...'}
                </span>
            `;
        } else if (state === 'rehashing') {
            this._scanningLocationId = null;
            this.activityEl.innerHTML = `
                <span class="status-activity-text scanning">
                    ${detail || 'Re-hashing...'}
                </span>
            `;
        } else {
            this._scanningLocationId = null;
            this.activityEl.innerHTML = `
                <span class="status-activity-text">Idle</span>
            `;
        }
    },

    updateQueue(queueState) {
        this._pendingQueue = (queueState && queueState.pending) || [];
        // Re-render the queue badge if currently scanning
        if (this._scanningLocationId) {
            const badge = this.activityEl.querySelector('.status-queue-info');
            const count = this._pendingQueue.length;
            if (badge) {
                if (count === 0) {
                    badge.remove();
                } else {
                    badge.textContent = `+${count} queued`;
                }
            } else if (count > 0) {
                const span = document.createElement('span');
                span.className = 'status-queue-info';
                span.textContent = `+${count} queued`;
                const activityText = this.activityEl.querySelector('.status-activity-text');
                if (activityText) activityText.appendChild(span);
            }
        }
    },

    formatCopyProgress(filename, bytesSent, bytesTotal) {
        if (!bytesTotal) return `${filename} — Copying...`;
        const pct = Math.round((bytesSent / bytesTotal) * 100);
        return `${filename} — Copying ${formatSize(bytesSent)}/${formatSize(bytesTotal)} (${pct}%)`;
    },

    isScanning() {
        return this._scanningLocationId !== null;
    },

    getQueue() {
        return this._pendingQueue;
    },

    renderConnection(connected) {
        const dotClass = connected ? 'status-dot' : 'status-dot disconnected';
        const label = connected ? 'Connected' : 'Disconnected';
        this.connectionEl.innerHTML = `
            <span class="${dotClass}"></span>
            <span>${label}</span>
        `;
    },

    updateServerActivity(msg) {
        this._activities = msg.activities || [];
        const count = msg.count || 0;
        const maxOps = 5;
        const pct = Math.min(count / maxOps, 1) * 100;

        if (this._loadBarEl) {
            this._loadBarEl.style.width = pct + '%';
        }
        if (this._loadEl) {
            if (count === 0) {
                this._loadEl.title = 'Server idle';
            } else {
                const labels = this._activities.map(a => {
                    const p = a.progress ? ` (${a.progress})` : '';
                    return a.label + p;
                });
                this._loadEl.title = labels.join('\n');
            }
        }
        if (this._dropdownOpen) {
            this._renderDropdown();
        }
    },

    _toggleDropdown() {
        if (this._dropdownOpen) {
            this._closeDropdown();
        } else {
            this._dropdownOpen = true;
            this._renderDropdown();
            if (this._loadDropdown) this._loadDropdown.classList.remove('hidden');
        }
    },

    _closeDropdown() {
        this._dropdownOpen = false;
        if (this._loadDropdown) this._loadDropdown.classList.add('hidden');
    },

    _renderDropdown() {
        if (!this._loadDropdown) return;
        if (this._activities.length === 0) {
            this._loadDropdown.innerHTML = '<div class="load-dropdown-empty">Server idle</div>';
            return;
        }
        this._loadDropdown.innerHTML = this._activities.map(a => {
            const progress = a.progress ? `<span class="load-progress">${a.progress}</span>` : '';
            return `<div class="load-dropdown-item"><span class="load-label">${a.label}</span>${progress}</div>`;
        }).join('');
    },
};

export default StatusBar;
