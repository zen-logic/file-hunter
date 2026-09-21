/**
 * Triage — session-scoped mark queues for batch file operations.
 *
 * Files can be marked for delete, consolidate, tag, or move.
 * Marks persist across folder/page navigation (session lifetime).
 * Renders a bar above the file table showing action buttons with counts.
 */

// 'zip' is appended deliberately — execute indexes the first four to build
// the SlideshowTriage.show(delete, consolidate, tag, move) argument list.
const OPERATIONS = ['delete', 'consolidate', 'tag', 'move', 'zip'];
const OP_LABELS = { delete: 'Delete', consolidate: 'Consolidate', tag: 'Tag', move: 'Move / Copy', zip: 'Download ZIP' };
const OP_KEYS = { d: 'delete', c: 'consolidate', t: 'tag', m: 'move', z: 'zip' };
const OP_CSS = {
    delete: 'triage-delete',
    consolidate: 'triage-consolidate',
    tag: 'triage-tag',
    move: 'triage-move',
    zip: 'triage-zip',
};

const Triage = {
    // Each queue: Map<fileId, { id, name }>
    queues: {
        delete: new Map(),
        consolidate: new Map(),
        tag: new Map(),
        move: new Map(),
        zip: new Map(),
    },

    barEl: null,
    onExecute: null,   // callback(op, items) — triggers the triage dialog
    onRender: null,    // callback() — re-render file list badges
    onZip: null,       // callback(items) — starts a ZIP build, no dialog

    init(onExecute, onRender, onZip) {
        this.onExecute = onExecute;
        this.onRender = onRender;
        this.onZip = onZip;
    },

    /** Mount the triage bar inside parentEl, before refEl. */
    mount(parentEl, refEl) {
        this.barEl = document.createElement('div');
        this.barEl.className = 'triage-bar hidden';
        parentEl.insertBefore(this.barEl, refEl);
    },

    /** Handle a key press — returns true if consumed. */
    handleKey(key, fileItems) {
        const op = OP_KEYS[key];
        if (!op) return false;
        if (!fileItems || fileItems.length === 0) return false;

        for (const item of fileItems) {
            if (item.type === 'folder') continue;
            this.toggle(op, item);
        }

        this.renderBar();
        if (this.onRender) this.onRender();
        return true;
    },

    /** Check if a file has any marks. Returns array of op names, e.g. ['delete', 'move']. */
    getMarks(fileId) {
        const marks = [];
        for (const op of OPERATIONS) {
            if (this.queues[op].has(fileId)) marks.push(op);
        }
        return marks;
    },

    /** Total marked items across all queues. */
    totalMarked() {
        let n = 0;
        for (const op of OPERATIONS) n += this.queues[op].size;
        return n;
    },

    /** Clear a single operation queue. */
    clearOp(op) {
        this.queues[op].clear();
        this.renderBar();
        if (this.onRender) this.onRender();
    },

    /** Clear all queues. */
    clearAll() {
        for (const op of OPERATIONS) this.queues[op].clear();
        this.renderBar();
        if (this.onRender) this.onRender();
    },

    // ── Internal ──

    toggle(op, item) {
        const id = item.id;
        const q = this.queues[op];

        if (q.has(id)) {
            q.delete(id);
            return;
        }

        // Exclusion rules before marking
        if (op === 'delete') {
            this.queues.move.delete(id);
            this.queues.consolidate.delete(id);
            this.queues.tag.delete(id);
        } else if (op === 'move') {
            this.queues.delete.delete(id);
            this.queues.consolidate.delete(id);
        } else if (op === 'consolidate') {
            this.queues.delete.delete(id);
            this.queues.move.delete(id);
        } else if (op === 'tag') {
            this.queues.delete.delete(id);
        }

        q.set(id, { id, name: item.name });
    },

    renderBar() {
        if (!this.barEl) return;
        const total = this.totalMarked();
        if (total === 0) {
            this.barEl.classList.add('hidden');
            this.barEl.innerHTML = '';
            return;
        }

        this.barEl.classList.remove('hidden');
        this.barEl.innerHTML = '';

        for (const op of OPERATIONS) {
            const count = this.queues[op].size;
            if (count === 0) continue;

            const btn = document.createElement('button');
            btn.className = `btn btn-sm triage-btn ${OP_CSS[op]}`;
            btn.textContent = `${OP_LABELS[op]} (${count})`;
            btn.addEventListener('click', (e) => {
                e.stopPropagation();
                this.execute(op);
            });
            this.barEl.appendChild(btn);
        }

        const clearBtn = document.createElement('button');
        clearBtn.className = 'btn btn-sm triage-btn triage-clear';
        clearBtn.textContent = 'Clear';
        clearBtn.addEventListener('click', (e) => {
            e.stopPropagation();
            this.clearAll();
        });
        this.barEl.appendChild(clearBtn);
    },

    execute(op) {
        const items = Array.from(this.queues[op].values());
        if (items.length === 0) return;

        if (op === 'zip') {
            // No triage dialog — this starts a build immediately, the same as
            // the Download ZIP button. The zip_ready socket message delivers
            // the file, so nothing further is needed here.
            if (this.onZip) this.onZip(items);
        } else {
            // Build args for SlideshowTriage.show(delete, consolidate, tag, move)
            const args = [[], [], [], []];
            const idx = OPERATIONS.indexOf(op);
            args[idx] = items;

            if (this.onExecute) this.onExecute(...args);
        }

        // Clear this queue after triggering
        this.queues[op].clear();
        this.renderBar();
        if (this.onRender) this.onRender();
    },
};

export default Triage;
