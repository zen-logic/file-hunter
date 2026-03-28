/**
 * Activity — single source of truth for all server activity state.
 *
 * Both real-time WS messages and polling (server_activity) feed into
 * this module. It owns updates to: status bar text, activity log,
 * and the load indicator/dropdown.
 *
 * WS handlers call: started(), progress(), completed(), error()
 * Polling calls: sync()
 */

import ActivityLog from './activitylog.js';
import StatusBar from './statusbar.js';

const Activity = {
    _ops: new Map(), // name -> { label, detail, locationId }

    /**
     * Register a new operation.
     * @param {string} name - unique key (e.g. 'scan-33', 'dup-recalc')
     * @param {object} opts
     * @param {string} opts.label - short label ('Scanning: Home')
     * @param {string} [opts.detail] - progress detail ('starting...')
     * @param {number} [opts.locationId] - associated location
     * @param {string|false} [opts.log] - activity log text, or false to suppress
     */
    started(name, { label, detail, locationId, log } = {}) {
        this._ops.set(name, { label, detail: detail || '', locationId });
        this._render();
        if (log !== false) ActivityLog.add(log || label);
    },

    /**
     * Update progress on an existing operation.
     * @param {string} name
     * @param {object} opts
     * @param {string} [opts.detail] - new progress text
     * @param {string} [opts.label] - update the label
     * @param {string} [opts.log] - activity log text (omit to skip)
     */
    progress(name, { detail, label, log } = {}) {
        let op = this._ops.get(name);
        if (!op) {
            // Auto-create if first message is a progress update
            op = { label: label || name, detail: detail || '', locationId: null };
            this._ops.set(name, op);
        }
        if (detail !== undefined) op.detail = detail;
        if (label !== undefined) op.label = label;
        this._render();
        if (log) ActivityLog.add(log);
    },

    /**
     * Mark an operation as complete and remove it.
     * @param {string} name
     * @param {object} [opts]
     * @param {string} [opts.log] - completion log text
     */
    completed(name, { log } = {}) {
        this._ops.delete(name);
        this._render();
        if (log) ActivityLog.add(log);
    },

    /**
     * Mark an operation as failed and remove it.
     * @param {string} name
     * @param {object} [opts]
     * @param {string} [opts.log] - error log text
     */
    error(name, { log } = {}) {
        this._ops.delete(name);
        this._render();
        if (log) ActivityLog.add(log);
    },

    /**
     * Sync with server_activity polling (authoritative state).
     * Reconciles local _ops with server state so all browsers
     * show consistent activity regardless of which real-time
     * messages they received.
     */
    sync(msg) {
        const serverOps = msg.activities || [];
        const serverNames = new Set(serverOps.map(a => a.name));

        // Add/update ops the server knows about
        for (const a of serverOps) {
            const existing = this._ops.get(a.name);
            if (!existing) {
                // Server has an op we don't — we missed the started message
                this._ops.set(a.name, {
                    label: a.label,
                    detail: a.progress || '',
                    locationId: null,
                });
            } else if (a.progress) {
                // Update progress from server (real-time may be more current,
                // but server is authoritative for existence)
                existing.detail = a.progress;
            }
        }

        // Remove ops the server no longer has — they completed and we
        // missed the completed message
        for (const name of this._ops.keys()) {
            if (!serverNames.has(name)) {
                this._ops.delete(name);
            }
        }

        this._render();
        StatusBar.updateServerActivity(msg);
    },

    /** True if any operation is active. */
    isActive() {
        return this._ops.size > 0;
    },

    /** Get the scanning location ID, if a scan is the primary op. */
    scanningLocationId() {
        for (const op of this._ops.values()) {
            if (op.locationId) return op.locationId;
        }
        return null;
    },

    _render() {
        if (this._ops.size === 0) {
            StatusBar.renderActivity('idle');
            return;
        }
        // Primary = last added operation
        let primary = null;
        for (const op of this._ops.values()) primary = op;
        const text = primary.detail
            ? `${primary.label} — ${primary.detail}`
            : primary.label;
        StatusBar.renderActivity('active', text, primary.locationId);
    },
};

export default Activity;
