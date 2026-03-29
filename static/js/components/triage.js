/**
 * Triage — session-scoped mark queues for batch file operations.
 *
 * Files can be marked for delete, consolidate, tag, or move.
 * Marks persist across folder/page navigation (session lifetime).
 * Renders a bar above the file table showing action buttons with counts.
 */

const OPERATIONS = ['delete', 'consolidate', 'tag', 'move'];
const OP_LABELS = { delete: 'Delete', consolidate: 'Consolidate', tag: 'Tag', move: 'Move' };
const OP_KEYS = { d: 'delete', c: 'consolidate', t: 'tag', m: 'move' };
const OP_CSS = {
    delete: 'triage-delete',
    consolidate: 'triage-consolidate',
    tag: 'triage-tag',
    move: 'triage-move',
};

const Triage = {
    // Each queue: Map<fileId, { id, name }>
    _queues: {
        delete: new Map(),
        consolidate: new Map(),
        tag: new Map(),
        move: new Map(),
    },

    _barEl: null,
    _onExecute: null,   // callback(op, items) — triggers the triage dialog
    _onRender: null,    // callback() — re-render file list badges

    init(onExecute, onRender) {
        this._onExecute = onExecute;
        this._onRender = onRender;
    },

    /** Mount the triage bar inside parentEl, before refEl. */
    mount(parentEl, refEl) {
        this._barEl = document.createElement('div');
        this._barEl.className = 'triage-bar hidden';
        parentEl.insertBefore(this._barEl, refEl);
    },

    /** Handle a key press — returns true if consumed. */
    handleKey(key, fileItems) {
        const op = OP_KEYS[key];
        if (!op) return false;
        if (!fileItems || fileItems.length === 0) return false;

        for (const item of fileItems) {
            if (item.type === 'folder') continue;
            this._toggle(op, item);
        }

        this._renderBar();
        if (this._onRender) this._onRender();
        return true;
    },

    /** Check if a file has any marks. Returns array of op names, e.g. ['delete', 'move']. */
    getMarks(fileId) {
        const marks = [];
        for (const op of OPERATIONS) {
            if (this._queues[op].has(fileId)) marks.push(op);
        }
        return marks;
    },

    /** Total marked items across all queues. */
    totalMarked() {
        let n = 0;
        for (const op of OPERATIONS) n += this._queues[op].size;
        return n;
    },

    /** Clear a single operation queue. */
    clearOp(op) {
        this._queues[op].clear();
        this._renderBar();
        if (this._onRender) this._onRender();
    },

    /** Clear all queues. */
    clearAll() {
        for (const op of OPERATIONS) this._queues[op].clear();
        this._renderBar();
        if (this._onRender) this._onRender();
    },

    // ── Internal ──

    _toggle(op, item) {
        const id = item.id;
        const q = this._queues[op];

        if (q.has(id)) {
            q.delete(id);
            return;
        }

        // Exclusion rules before marking
        if (op === 'delete') {
            this._queues.move.delete(id);
            this._queues.consolidate.delete(id);
            this._queues.tag.delete(id);
        } else if (op === 'move') {
            this._queues.delete.delete(id);
            this._queues.consolidate.delete(id);
        } else if (op === 'consolidate') {
            this._queues.delete.delete(id);
            this._queues.move.delete(id);
        } else if (op === 'tag') {
            this._queues.delete.delete(id);
        }

        q.set(id, { id, name: item.name });
    },

    _renderBar() {
        if (!this._barEl) return;
        const total = this.totalMarked();
        if (total === 0) {
            this._barEl.classList.add('hidden');
            this._barEl.innerHTML = '';
            return;
        }

        this._barEl.classList.remove('hidden');
        this._barEl.innerHTML = '';

        for (const op of OPERATIONS) {
            const count = this._queues[op].size;
            if (count === 0) continue;

            const btn = document.createElement('button');
            btn.className = `btn btn-sm triage-btn ${OP_CSS[op]}`;
            btn.textContent = `${OP_LABELS[op]} (${count})`;
            btn.addEventListener('click', (e) => {
                e.stopPropagation();
                this._execute(op);
            });
            this._barEl.appendChild(btn);
        }

        const clearBtn = document.createElement('button');
        clearBtn.className = 'btn btn-sm triage-btn triage-clear';
        clearBtn.textContent = 'Clear';
        clearBtn.addEventListener('click', (e) => {
            e.stopPropagation();
            this.clearAll();
        });
        this._barEl.appendChild(clearBtn);
    },

    _execute(op) {
        const items = Array.from(this._queues[op].values());
        if (items.length === 0) return;

        // Build args for SlideshowTriage.show(delete, consolidate, tag, move)
        const args = [[], [], [], []];
        const idx = OPERATIONS.indexOf(op);
        args[idx] = items;

        if (this._onExecute) this._onExecute(...args);

        // Clear this queue after triggering
        this._queues[op].clear();
        this._renderBar();
        if (this._onRender) this._onRender();
    },
};

export default Triage;
