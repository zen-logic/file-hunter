const Keyboard = {
    activePanel: null,
    handlers: {},
    searchToggle: null,
    selectAllHandler: null,
    newLocationHandler: null,
    scanHandler: null,
    deleteHandler: null,
    panels: {},

    init() {
        this.panels = {
            tree: document.getElementById('tree-content'),
            filelist: document.getElementById('file-content'),
            detail: document.getElementById('detail-content'),
        };

        // Make panels focusable
        for (const el of Object.values(this.panels)) {
            if (el) el.setAttribute('tabindex', '0');
        }

        // Track active panel via focusin
        for (const [name, el] of Object.entries(this.panels)) {
            if (!el) continue;
            el.addEventListener('focusin', () => this.setActivePanel(name));
        }

        // Also track via click on the panel containers (parent elements)
        const containerMap = {
            tree: document.getElementById('tree-panel'),
            filelist: document.getElementById('file-panel'),
            detail: document.getElementById('detail-panel'),
        };
        for (const [name, el] of Object.entries(containerMap)) {
            if (!el) continue;
            el.addEventListener('click', () => this.setActivePanel(name));
        }

        document.addEventListener('keydown', (e) => this.onKeyDown(e));
    },

    registerPanel(name, handler) {
        this.handlers[name] = handler;
    },

    setSearchToggle(fn) {
        this.searchToggle = fn;
    },

    setSelectAllHandler(fn) {
        this.selectAllHandler = fn;
    },

    setNewLocationHandler(fn) {
        this.newLocationHandler = fn;
    },

    setScanHandler(fn) {
        this.scanHandler = fn;
    },

    setDeleteHandler(fn) {
        this.deleteHandler = fn;
    },

    setActivePanel(name) {
        if (this.activePanel === name) return;
        this.activePanel = name;

        // Update visual indicator
        for (const [n, el] of Object.entries(this.panels)) {
            if (!el) continue;
            el.closest('#tree-panel, #file-panel, #detail-panel')
                ?.classList.toggle('panel-focused', n === name);
        }
    },

    isModalOpen() {
        const modals = document.querySelectorAll('.modal-overlay');
        for (const m of modals) {
            if (!m.classList.contains('hidden')) return true;
        }
        return false;
    },

    isInputFocused() {
        const el = document.activeElement;
        if (!el) return false;
        const tag = el.tagName;
        if (tag === 'INPUT' || tag === 'TEXTAREA' || tag === 'SELECT') return true;
        if (el.isContentEditable) return true;
        return false;
    },

    getActiveFilterInput() {
        if (this.activePanel === 'tree') {
            return document.getElementById('tree-filter');
        }
        if (this.activePanel === 'filelist') {
            return document.getElementById('file-filter');
        }
        return null;
    },

    onKeyDown(e) {
        // 1. Modal open? Let modal handle it
        if (this.isModalOpen()) return;

        // 2. Ctrl/Cmd+F — toggle search (works even from inputs)
        if ((e.ctrlKey || e.metaKey) && e.key === 'f') {
            e.preventDefault();
            if (this.searchToggle) this.searchToggle();
            return;
        }

        // 2b. Ctrl/Cmd+A — select all in file list panel (only when no input focused)
        if ((e.ctrlKey || e.metaKey) && e.key === 'a' && this.activePanel === 'filelist' && !this.isInputFocused()) {
            e.preventDefault();
            if (this.selectAllHandler) this.selectAllHandler();
            return;
        }

        // 3. Tab — cycle panels
        if (e.key === 'Tab' && !this.isInputFocused()) {
            e.preventDefault();
            const order = ['tree', 'filelist', 'detail'];
            const idx = order.indexOf(this.activePanel);
            const next = e.shiftKey
                ? order[(idx - 1 + order.length) % order.length]
                : order[(idx + 1) % order.length];
            const el = this.panels[next];
            if (el) {
                el.focus();
                this.setActivePanel(next);
            }
            return;
        }

        // 4. Input/textarea/select focused?
        if (this.isInputFocused()) {
            // Escape in a panel filter input: clear and blur
            if (e.key === 'Escape') {
                const el = document.activeElement;
                if (el.classList.contains('panel-filter')) {
                    el.value = '';
                    el.dispatchEvent(new Event('input'));
                    el.blur();
                    // Re-focus the panel content area
                    const panel = this.panels[this.activePanel];
                    if (panel) panel.focus();
                    e.preventDefault();
                    return;
                }
                // Escape in search panel fields: close search
                const searchPanel = document.getElementById('search-panel');
                if (searchPanel && searchPanel.contains(el)) {
                    if (this.searchToggle) this.searchToggle();
                    e.preventDefault();
                    return;
                }
            }
            return;
        }

        // 4. Slash — focus active panel's filter input
        if (e.key === '/') {
            const filter = this.getActiveFilterInput();
            if (filter) {
                e.preventDefault();
                filter.focus();
            }
            return;
        }

        // 5. N — new location
        if (e.key === 'n' || e.key === 'N') {
            e.preventDefault();
            if (this.newLocationHandler) this.newLocationHandler();
            return;
        }

        // 6. S — scan selected location
        if (e.key === 's' || e.key === 'S') {
            e.preventDefault();
            if (this.scanHandler) this.scanHandler();
            return;
        }

        // 7. Delete — delete selected file(s)
        if (e.key === 'Delete' || e.key === 'Backspace') {
            e.preventDefault();
            if (this.deleteHandler) this.deleteHandler();
            return;
        }

        // 8. Detail panel — forward file list keys (nav, triage, preview)
        if (this.activePanel === 'detail' && this.handlers['filelist']) {
            const k = e.key;
            if (k === 'ArrowDown' || k === 'ArrowUp' || k === ' ' ||
                k === 'Home' || k === 'End' || k === 'PageDown' || k === 'PageUp' ||
                'dctmz'.includes(k)) {
                this.handlers['filelist'](e);
                return;
            }
        }

        // 9. Route to active panel handler
        if (this.activePanel && this.handlers[this.activePanel]) {
            this.handlers[this.activePanel](e);
        }
    },
};

export default Keyboard;
