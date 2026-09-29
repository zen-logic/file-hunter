import API from '../api.js';
import { formatSize, esc } from '../format.js';


const StatusBar = {
    statsEl: null,
    activityEl: null,
    connectionEl: null,
    loadEl: null,
    loadBarEl: null,
    loadDropdown: null,
    dropdownOpen: false,
    activities: [],
    scanningLocationId: null,
    pendingQueue: [],
    stats: null,

    init() {
        this.statsEl = document.getElementById('status-stats');
        this.activityEl = document.getElementById('status-activity');
        this.connectionEl = document.getElementById('status-connection');
        this.loadEl = document.getElementById('status-load');
        this.loadBarEl = document.getElementById('status-load-bar');
        this.loadDropdown = document.getElementById('status-load-dropdown');

        if (this.loadEl) {
            this.loadEl.addEventListener('click', (e) => {
                e.stopPropagation();
                this.toggleDropdown();
            });
        }
        if (this.loadDropdown) {
            this.loadDropdown.addEventListener('click', (e) => e.stopPropagation());
        }
        document.addEventListener('click', () => this.closeDropdown());

        this.loadStats();
        this.renderActivity('idle');
        this.renderConnection(false);
    },

    async loadStats() {
        const res = await API.get('/api/stats');
        if (res.ok) {
            this.stats = res.data;
            this.renderStats();
        }
    },

    updateStatsFromProgress(msg) {
        if (!this.stats) return;
        if (msg.globalFileCount !== undefined) this.stats.totalFiles = msg.globalFileCount;
        if (msg.globalTotalSize !== undefined) {
            this.stats.totalSize = msg.globalTotalSize;
            this.stats.totalSizeFormatted = formatSize(msg.globalTotalSize);
        }
        if (msg.globalDuplicateCount !== undefined) this.stats.duplicateFiles = msg.globalDuplicateCount;
        this.renderStats();
    },

    renderStats() {
        const s = this.stats;
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

    renderQueueBadge() {
        const count = this.pendingQueue.length;
        if (count === 0) return '';
        return `<span class="status-queue-info">+${count} queued</span>`;
    },

    renderActivity(state, detail, locationId) {
        if (state === 'active') {
            this.scanningLocationId = locationId || null;
            this.activityEl.innerHTML = `
                <span class="status-activity-text scanning">
                    ${esc(detail || '...')}
                    ${locationId ? this.renderQueueBadge() : ''}
                    ${locationId ? '<span class="status-cancel" title="Cancel">✕</span>' : ''}
                </span>
            `;
            const cancelEl = this.activityEl.querySelector('.status-cancel');
            if (cancelEl) {
                cancelEl.addEventListener('click', async (e) => {
                    e.stopPropagation();
                    await API.post('/api/scan/cancel', { location_id: locationId });
                });
            }
        } else {
            this.scanningLocationId = null;
            this.activityEl.innerHTML = `
                <span class="status-activity-text">Idle</span>
            `;
        }
    },

    updateQueue(queueState) {
        this.pendingQueue = (queueState && queueState.pending) || [];
        // Re-render the queue badge if currently scanning
        if (this.scanningLocationId) {
            const badge = this.activityEl.querySelector('.status-queue-info');
            const count = this.pendingQueue.length;
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
        return this.scanningLocationId !== null;
    },

    getQueue() {
        return this.pendingQueue;
    },

    renderConnection(connected) {
        const dotClass = connected ? 'status-dot' : 'status-dot disconnected';
        const label = connected ? 'Connected' : 'Disconnected';
        this.connectionEl.innerHTML = `
            <span class="${dotClass}"></span>
            <span>${esc(label)}</span>
        `;
    },

    updateServerActivity(msg) {
        this.activities = msg.activities || [];
        const count = msg.count || 0;
        const maxOps = 5;
        const pct = Math.min(count / maxOps, 1) * 100;

        if (this.loadBarEl) {
            this.loadBarEl.style.width = pct + '%';
        }
        if (this.loadEl) {
            if (count === 0) {
                this.loadEl.title = 'Server idle';
            } else {
                const labels = this.activities.map(a => {
                    const p = a.progress ? ` (${a.progress})` : '';
                    return a.label + p;
                });
                this.loadEl.title = labels.join('\n');
            }
        }
        if (this.dropdownOpen) {
            this.renderDropdown();
        }
    },

    toggleDropdown() {
        if (this.dropdownOpen) {
            this.closeDropdown();
        } else {
            this.dropdownOpen = true;
            this.renderDropdown();
            if (this.loadDropdown) this.loadDropdown.classList.remove('hidden');
        }
    },

    closeDropdown() {
        this.dropdownOpen = false;
        if (this.loadDropdown) this.loadDropdown.classList.add('hidden');
    },

    renderDropdown() {
        if (!this.loadDropdown) return;
        if (this.activities.length === 0) {
            this.loadDropdown.innerHTML = '<div class="load-dropdown-empty">Server idle</div>';
            return;
        }
        this.loadDropdown.innerHTML = this.activities.map(a => {
            const progress = a.progress ? `<span class="load-progress">${esc(a.progress)}</span>` : '';
            return `<div class="load-dropdown-item"><span class="load-label">${esc(a.label)}</span>${progress}</div>`;
        }).join('');
    },
};

export default StatusBar;
