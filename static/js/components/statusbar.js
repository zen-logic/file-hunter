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
    _scanningLocationId: null,
    _pendingQueue: [],
    _stats: null,
    _lastProgress: null,

    init() {
        this.statsEl = document.getElementById('status-stats');
        this.activityEl = document.getElementById('status-activity');
        this.connectionEl = document.getElementById('status-connection');
        this.loadStats();
        this.renderActivity('idle');
        this.renderConnection(false);
    },

    async loadStats() {
        const res = await API.get('/api/stats');
        if (res.ok) {
            this._stats = res.data;
            this._lastProgress = null;
            this._renderStats();
        }
    },

    updateStatsFromProgress(msg) {
        if (!this._stats) return;
        const prev = this._lastProgress || { filesFound: 0, duplicatesFound: 0 };
        const fileDelta = (msg.filesFound || 0) - (prev.filesFound || 0);
        const dupDelta = (msg.duplicatesFound || 0) - (prev.duplicatesFound || 0);
        if (fileDelta > 0) this._stats.totalFiles += fileDelta;
        if (dupDelta > 0) this._stats.duplicateFiles += dupDelta;
        if (fileDelta > 0 || dupDelta > 0) this._renderStats();
        this._lastProgress = { filesFound: msg.filesFound || 0, duplicatesFound: msg.duplicatesFound || 0 };
    },

    _renderStats() {
        const s = this._stats;
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
                    <button class="status-cancel-btn" id="status-cancel-scan">Cancel</button>
                    ${this._renderQueueBadge()}
                </span>
            `;
            document.getElementById('status-cancel-scan').addEventListener('click', () => this._cancelScan());
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
                    <button class="status-cancel-btn" id="status-cancel-merge">Cancel</button>
                </span>
            `;
            document.getElementById('status-cancel-merge')
                .addEventListener('click', async () => { await API.post('/api/merge/cancel'); });
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

    isScanning() {
        return this._scanningLocationId !== null;
    },

    getQueue() {
        return this._pendingQueue;
    },

    async _cancelScan() {
        if (!this._scanningLocationId) return;
        const res = await API.post('/api/scan/cancel', { location_id: this._scanningLocationId });
        if (res.ok) {
            this.activityEl.innerHTML = `
                <span class="status-activity-text scanning">Cancelling\u2026</span>
            `;
        }
    },

    renderConnection(connected) {
        const dotClass = connected ? 'status-dot' : 'status-dot disconnected';
        const label = connected ? 'Connected' : 'Disconnected';
        this.connectionEl.innerHTML = `
            <span class="${dotClass}"></span>
            <span>${label}</span>
        `;
    },
};

export default StatusBar;
