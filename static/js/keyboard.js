const Keyboard = {
    _activePanel: null,
    _handlers: {},
    _searchToggle: null,
    _selectAllHandler: null,
    _newLocationHandler: null,
    _scanHandler: null,
    _deleteHandler: null,
    _panels: {},

    init() {
        this._panels = {
            tree: document.getElementById('tree-content'),
            filelist: document.getElementById('file-content'),
            detail: document.getElementById('detail-content'),
        };

        // Make panels focusable
        for (const el of Object.values(this._panels)) {
            if (el) el.setAttribute('tabindex', '0');
        }

        // Track active panel via focusin
        for (const [name, el] of Object.entries(this._panels)) {
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

        document.addEventListener('keydown', (e) => this._onKeyDown(e));
    },

    registerPanel(name, handler) {
        this._handlers[name] = handler;
    },

    setSearchToggle(fn) {
        this._searchToggle = fn;
    },

    setSelectAllHandler(fn) {
        this._selectAllHandler = fn;
    },

    setNewLocationHandler(fn) {
        this._newLocationHandler = fn;
    },

    setScanHandler(fn) {
        this._scanHandler = fn;
    },

    setDeleteHandler(fn) {
        this._deleteHandler = fn;
    },

    setActivePanel(name) {
        if (this._activePanel === name) return;
        this._activePanel = name;

        // Update visual indicator
        for (const [n, el] of Object.entries(this._panels)) {
            if (!el) continue;
            el.closest('#tree-panel, #file-panel, #detail-panel')
                ?.classList.toggle('panel-focused', n === name);
        }
    },

    _isModalOpen() {
        const modals = document.querySelectorAll('.modal-overlay');
        for (const m of modals) {
            if (!m.classList.contains('hidden')) return true;
        }
        return false;
    },

    _isInputFocused() {
        const el = document.activeElement;
        if (!el) return false;
        const tag = el.tagName;
        if (tag === 'INPUT' || tag === 'TEXTAREA' || tag === 'SELECT') return true;
        if (el.isContentEditable) return true;
        return false;
    },

    _getActiveFilterInput() {
        if (this._activePanel === 'tree') {
            return document.getElementById('tree-filter');
        }
        if (this._activePanel === 'filelist') {
            return document.getElementById('file-filter');
        }
        return null;
    },

    _onKeyDown(e) {
        // 1. Modal open? Let modal handle it
        if (this._isModalOpen()) return;

        // 2. Ctrl/Cmd+F — toggle search (works even from inputs)
        if ((e.ctrlKey || e.metaKey) && e.key === 'f') {
            e.preventDefault();
            if (this._searchToggle) this._searchToggle();
            return;
        }

        // 2b. Ctrl/Cmd+A — select all in file list panel (only when no input focused)
        if ((e.ctrlKey || e.metaKey) && e.key === 'a' && this._activePanel === 'filelist' && !this._isInputFocused()) {
            e.preventDefault();
            if (this._selectAllHandler) this._selectAllHandler();
            return;
        }

        // 3. Tab — cycle panels
        if (e.key === 'Tab' && !this._isInputFocused()) {
            e.preventDefault();
            const order = ['tree', 'filelist', 'detail'];
            const idx = order.indexOf(this._activePanel);
            const next = e.shiftKey
                ? order[(idx - 1 + order.length) % order.length]
                : order[(idx + 1) % order.length];
            const el = this._panels[next];
            if (el) {
                el.focus();
                this.setActivePanel(next);
            }
            return;
        }

        // 4. Input/textarea/select focused?
        if (this._isInputFocused()) {
            // Escape in a panel filter input: clear and blur
            if (e.key === 'Escape') {
                const el = document.activeElement;
                if (el.classList.contains('panel-filter')) {
                    el.value = '';
                    el.dispatchEvent(new Event('input'));
                    el.blur();
                    // Re-focus the panel content area
                    const panel = this._panels[this._activePanel];
                    if (panel) panel.focus();
                    e.preventDefault();
                    return;
                }
                // Escape in search panel fields: close search
                const searchPanel = document.getElementById('search-panel');
                if (searchPanel && searchPanel.contains(el)) {
                    if (this._searchToggle) this._searchToggle();
                    e.preventDefault();
                    return;
                }
            }
            return;
        }

        // 4. Slash — focus active panel's filter input
        if (e.key === '/') {
            const filter = this._getActiveFilterInput();
            if (filter) {
                e.preventDefault();
                filter.focus();
            }
            return;
        }

        // 5. N — new location
        if (e.key === 'n' || e.key === 'N') {
            e.preventDefault();
            if (this._newLocationHandler) this._newLocationHandler();
            return;
        }

        // 6. S — scan selected location
        if (e.key === 's' || e.key === 'S') {
            e.preventDefault();
            if (this._scanHandler) this._scanHandler();
            return;
        }

        // 7. Delete — delete selected file(s)
        if (e.key === 'Delete' || e.key === 'Backspace') {
            e.preventDefault();
            if (this._deleteHandler) this._deleteHandler();
            return;
        }

        // 8. Route to active panel handler
        if (this._activePanel && this._handlers[this._activePanel]) {
            this._handlers[this._activePanel](e);
        }
    },
};

export default Keyboard;
