import API from '../api.js';
import PromptModal from './prompt.js';

function esc(s) { const d = document.createElement('div'); d.textContent = s; return d.innerHTML; }

const FIELD_OPTIONS = [
    { value: 'name', label: 'Name' },
    { value: 'type', label: 'Type' },
    { value: 'description', label: 'Description' },
    { value: 'tags', label: 'Tags' },
    { value: 'size', label: 'Size' },
    { value: 'date', label: 'Date' },
    { value: 'folder', label: 'Folder name' },
    { value: 'duplicates', label: 'Duplicates' },
];

const MATCH_OPTIONS = [
    { value: 'wildcard', label: 'Wildcard' },
    { value: 'anywhere', label: 'Anywhere' },
    { value: 'starts', label: 'Starts with' },
    { value: 'ends', label: 'Ends with' },
    { value: 'exact', label: 'Exact' },
];

const TYPE_OPTIONS = [
    { value: '', label: 'All types' },
    { value: 'image', label: 'Image' },
    { value: 'video', label: 'Video' },
    { value: 'audio', label: 'Audio' },
    { value: 'document', label: 'Document' },
    { value: 'text', label: 'Text' },
    { value: 'compressed', label: 'Compressed' },
    { value: 'font', label: 'Font' },
    { value: 'other', label: 'Other' },
];

const Search = {
    panelEl: null,
    toggleBtn: null,
    visible: false,
    onSearch: null,
    onClear: null,

    // Advanced mode state
    mode: 'basic',
    conditions: [],
    nextCondId: 0,
    scopeNode: null,

    init({ onSearch, onClear }) {
        this.panelEl = document.getElementById('search-panel');
        this.toggleBtn = document.getElementById('btn-search');
        this.searchBtn = document.getElementById('search-go');
        this.onSearch = onSearch;
        this.onClear = onClear;

        this.toggleBtn.addEventListener('click', () => this.toggle());

        this.searchBtn.addEventListener('click', () => this._doSearch());
        document.getElementById('search-clear').addEventListener('click', () => this._doClear());

        // Update button state on any input change (basic mode)
        document.getElementById('search-basic').querySelectorAll('input, select').forEach(el => {
            el.addEventListener('input', () => this._updateSearchBtn());
            el.addEventListener('change', () => this._updateSearchBtn());
            el.addEventListener('keydown', (e) => {
                if (e.key === 'Enter') this._doSearch();
            });
        });

        this._updateSearchBtn();
        this._initAdvanced();
        this._initSavedSearches();
    },

    toggle() {
        this.visible = !this.visible;
        this.panelEl.classList.toggle('hidden', !this.visible);
        this.toggleBtn.classList.toggle('btn-active', this.visible);
        if (this.visible) {
            if (this.mode === 'advanced') {
                const firstInput = document.querySelector('#search-conditions input');
                if (firstInput) firstInput.focus();
            } else {
                document.getElementById('search-name').focus();
            }
        }
    },

    close() {
        if (!this.visible && !this._hasFilters()) return;
        // Clear basic
        document.getElementById('search-basic').querySelectorAll('input[type="text"], input[type="date"], input[type="number"]').forEach(el => el.value = '');
        document.getElementById('search-basic').querySelectorAll('select').forEach(el => el.selectedIndex = 0);
        document.getElementById('search-files').checked = true;
        document.getElementById('search-folders').checked = false;
        document.getElementById('search-dupes').checked = false;
        // Clear advanced
        this._clearAdvanced();
        // Uncheck scope but keep bar visible
        document.getElementById('search-scope-check').checked = false;
        this._updateSearchBtn();
        if (this.visible) {
            this.visible = false;
            this.panelEl.classList.add('hidden');
            this.toggleBtn.classList.remove('btn-active');
        }
    },

    setScopeContext(node) {
        const scopeEl = document.getElementById('search-scope');
        const nameEl = document.getElementById('search-scope-name');
        const checkEl = document.getElementById('search-scope-check');
        if (node && (node.type === 'location' || node.type === 'folder')) {
            this.scopeNode = node;
            nameEl.textContent = node.label || node.name;
            scopeEl.classList.remove('hidden');
            checkEl.checked = false;
        } else {
            this.scopeNode = null;
            scopeEl.classList.add('hidden');
            checkEl.checked = false;
        }
    },

    _getValues() {
        if (this.mode === 'advanced') return this._getAdvancedValues();
        const values = {
            name: document.getElementById('search-name').value.trim(),
            nameMatch: document.getElementById('search-name-match').value,
            type: document.getElementById('search-type').value,
            description: document.getElementById('search-desc').value.trim(),
            tags: document.getElementById('search-tags').value.trim(),
            sizeMin: document.getElementById('search-size-min').value.trim(),
            sizeMax: document.getElementById('search-size-max').value.trim(),
            minDups: document.getElementById('search-min-dups').value.trim(),
            maxDups: document.getElementById('search-max-dups').value.trim(),
            dateFrom: document.getElementById('search-date-from').value,
            dateTo: document.getElementById('search-date-to').value,
            files: document.getElementById('search-files').checked,
            folders: document.getElementById('search-folders').checked,
            dupes: document.getElementById('search-dupes').checked,
        };
        if (document.getElementById('search-scope-check').checked && this.scopeNode) {
            values.scopeType = this.scopeNode.type;
            values.scopeId = this.scopeNode.id;
        }
        return values;
    },

    _hasFilters() {
        if (this.mode === 'advanced') return this._hasAdvancedFilters();
        const v = this._getValues();
        return v.name || v.type || v.description || v.tags ||
               v.sizeMin || v.sizeMax || v.minDups || v.maxDups || v.dateFrom || v.dateTo || v.dupes || v.folders;
    },

    _updateSearchBtn() {
        if (this.mode === 'advanced') {
            document.getElementById('search-adv-go').disabled = !this._hasAdvancedFilters();
        } else {
            this.searchBtn.disabled = !this._hasFilters();
        }
    },

    _doSearch() {
        if (!this._hasFilters()) return;
        const values = this._getValues();
        if (this.onSearch) this.onSearch(values);
    },

    _doClear() {
        if (this.mode === 'advanced') {
            this._clearAdvanced();
            document.getElementById('search-scope-check').checked = false;
            this._updateSearchBtn();
            if (this.onClear) this.onClear();
            return;
        }
        document.getElementById('search-basic').querySelectorAll('input[type="text"], input[type="date"], input[type="number"]').forEach(el => el.value = '');
        document.getElementById('search-basic').querySelectorAll('select').forEach(el => el.selectedIndex = 0);
        document.getElementById('search-files').checked = true;
        document.getElementById('search-folders').checked = false;
        document.getElementById('search-dupes').checked = false;
        document.getElementById('search-scope-check').checked = false;
        this._updateSearchBtn();
        if (this.onClear) this.onClear();
    },

    // ── Advanced mode ──

    _initAdvanced() {
        const modeLink = document.getElementById('search-mode-link');
        modeLink.addEventListener('click', () => this._toggleMode());

        document.getElementById('search-add-include').addEventListener('click', () => this._addCondition('include'));
        document.getElementById('search-add-exclude').addEventListener('click', () => this._addCondition('exclude'));
        document.getElementById('search-adv-go').addEventListener('click', () => this._doSearch());
        document.getElementById('search-adv-clear').addEventListener('click', () => this._doClear());

        // Restore mode from localStorage
        const saved = localStorage.getItem('fh-search-mode');
        if (saved === 'advanced') {
            this.mode = 'advanced';
            document.getElementById('search-basic').classList.add('hidden');
            document.getElementById('search-advanced').classList.remove('hidden');
            modeLink.textContent = 'Basic';
            this._addCondition('include');
        }
    },

    _toggleMode() {
        const modeLink = document.getElementById('search-mode-link');
        if (this.mode === 'basic') {
            this.mode = 'advanced';
            document.getElementById('search-basic').classList.add('hidden');
            document.getElementById('search-advanced').classList.remove('hidden');
            modeLink.textContent = 'Basic';
            if (this.conditions.length === 0) this._addCondition('include');
            const firstInput = document.querySelector('#search-conditions input');
            if (firstInput) firstInput.focus();
        } else {
            this.mode = 'basic';
            document.getElementById('search-basic').classList.remove('hidden');
            document.getElementById('search-advanced').classList.add('hidden');
            modeLink.textContent = 'Advanced';
            document.getElementById('search-name').focus();
        }
        localStorage.setItem('fh-search-mode', this.mode);
        this._updateSearchBtn();
    },

    _addCondition(op) {
        const id = this.nextCondId++;
        const container = document.getElementById('search-conditions');
        const row = document.createElement('div');
        row.className = 'search-condition-row';
        row.dataset.condId = id;

        // Op badge
        const opEl = document.createElement('span');
        opEl.className = `search-condition-op ${op}`;
        opEl.textContent = op === 'include' ? '+' : '\u2212';
        row.appendChild(opEl);

        // Field dropdown
        const fieldSel = document.createElement('select');
        fieldSel.className = 'search-select';
        for (const opt of FIELD_OPTIONS) {
            const o = document.createElement('option');
            o.value = opt.value;
            o.textContent = opt.label;
            fieldSel.appendChild(o);
        }
        row.appendChild(fieldSel);

        // Input area
        const inputsDiv = document.createElement('div');
        inputsDiv.className = 'search-condition-inputs';
        row.appendChild(inputsDiv);

        // Remove button
        const removeBtn = document.createElement('button');
        removeBtn.className = 'search-condition-remove';
        removeBtn.textContent = '\u00d7';
        removeBtn.title = 'Remove';
        removeBtn.addEventListener('click', () => this._removeCondition(id));
        row.appendChild(removeBtn);

        container.appendChild(row);

        const cond = { id, op, field: 'name', values: {} };
        this.conditions.push(cond);

        // Render initial inputs (name)
        this._renderConditionInputs(inputsDiv, 'name');

        // Field change handler
        fieldSel.addEventListener('change', () => {
            cond.field = fieldSel.value;
            this._renderConditionInputs(inputsDiv, fieldSel.value);
            this._updateSearchBtn();
        });

        this._updateRemoveButtons();
        this._updateSearchBtn();
        return cond;
    },

    _removeCondition(id) {
        if (this.conditions.length <= 1) return;
        this.conditions = this.conditions.filter(c => c.id !== id);
        const row = document.querySelector(`.search-condition-row[data-cond-id="${id}"]`);
        if (row) row.remove();
        this._updateRemoveButtons();
        this._updateSearchBtn();
    },

    _updateRemoveButtons() {
        const rows = document.querySelectorAll('.search-condition-row');
        rows.forEach(row => {
            const btn = row.querySelector('.search-condition-remove');
            if (btn) btn.style.visibility = this.conditions.length <= 1 ? 'hidden' : 'visible';
        });
    },

    _renderConditionInputs(container, field) {
        container.innerHTML = '';
        const bind = () => {
            container.querySelectorAll('input, select').forEach(el => {
                el.addEventListener('input', () => this._updateSearchBtn());
                el.addEventListener('change', () => this._updateSearchBtn());
                el.addEventListener('keydown', (e) => {
                    if (e.key === 'Enter') this._doSearch();
                });
            });
        };

        switch (field) {
            case 'name':
            case 'folder': {
                const inp = document.createElement('input');
                inp.type = 'text';
                inp.className = 'search-input';
                inp.placeholder = field === 'name' ? 'Filename...' : 'Folder name...';
                inp.dataset.role = 'value';
                container.appendChild(inp);

                const sel = document.createElement('select');
                sel.className = 'search-select';
                sel.dataset.role = 'match';
                for (const opt of MATCH_OPTIONS) {
                    const o = document.createElement('option');
                    o.value = opt.value;
                    o.textContent = opt.label;
                    sel.appendChild(o);
                }
                container.appendChild(sel);
                bind();
                inp.focus();
                break;
            }
            case 'type': {
                const sel = document.createElement('select');
                sel.className = 'search-select';
                sel.dataset.role = 'value';
                for (const opt of TYPE_OPTIONS) {
                    const o = document.createElement('option');
                    o.value = opt.value;
                    o.textContent = opt.label;
                    sel.appendChild(o);
                }
                container.appendChild(sel);
                bind();
                break;
            }
            case 'description': {
                const inp = document.createElement('input');
                inp.type = 'text';
                inp.className = 'search-input';
                inp.placeholder = 'Caption or description...';
                inp.dataset.role = 'value';
                container.appendChild(inp);
                bind();
                inp.focus();
                break;
            }
            case 'tags': {
                const inp = document.createElement('input');
                inp.type = 'text';
                inp.className = 'search-input';
                inp.placeholder = 'tag1, tag2...';
                inp.dataset.role = 'value';
                container.appendChild(inp);
                bind();
                inp.focus();
                break;
            }
            case 'size': {
                const min = document.createElement('input');
                min.type = 'text';
                min.className = 'search-input search-input-sm';
                min.placeholder = 'e.g. 5MB';
                min.dataset.role = 'min';
                container.appendChild(min);

                const sep = document.createElement('span');
                sep.className = 'search-separator';
                sep.innerHTML = '&ndash;';
                container.appendChild(sep);

                const max = document.createElement('input');
                max.type = 'text';
                max.className = 'search-input search-input-sm';
                max.placeholder = 'e.g. 1GB';
                max.dataset.role = 'max';
                container.appendChild(max);
                bind();
                min.focus();
                break;
            }
            case 'date': {
                const from = document.createElement('input');
                from.type = 'date';
                from.className = 'search-input search-input-sm';
                from.dataset.role = 'from';
                container.appendChild(from);

                const sep = document.createElement('span');
                sep.className = 'search-separator';
                sep.innerHTML = '&ndash;';
                container.appendChild(sep);

                const to = document.createElement('input');
                to.type = 'date';
                to.className = 'search-input search-input-sm';
                to.dataset.role = 'to';
                container.appendChild(to);
                bind();
                from.focus();
                break;
            }
            case 'duplicates': {
                const from = document.createElement('input');
                from.type = 'number';
                from.className = 'search-input search-input-sm';
                from.placeholder = 'Min';
                from.min = '1';
                from.dataset.role = 'from';
                container.appendChild(from);
                const sep = document.createElement('span');
                sep.className = 'search-separator';
                sep.innerHTML = '&ndash;';
                container.appendChild(sep);
                const to = document.createElement('input');
                to.type = 'number';
                to.className = 'search-input search-input-sm';
                to.placeholder = 'Max';
                to.min = '1';
                to.dataset.role = 'to';
                container.appendChild(to);
                bind();
                from.focus();
                break;
            }
        }
    },

    _readConditionValues() {
        const result = [];
        for (const cond of this.conditions) {
            const row = document.querySelector(`.search-condition-row[data-cond-id="${cond.id}"]`);
            if (!row) continue;
            const fieldSel = row.querySelector('select.search-select');
            const field = fieldSel ? fieldSel.value : cond.field;
            const inputs = row.querySelector('.search-condition-inputs');
            const entry = { op: cond.op, field };

            switch (field) {
                case 'name':
                case 'folder':
                case 'description':
                case 'tags': {
                    const valEl = inputs.querySelector('[data-role="value"]');
                    entry.value = valEl ? valEl.value.trim() : '';
                    const matchEl = inputs.querySelector('[data-role="match"]');
                    if (matchEl) entry.match = matchEl.value;
                    break;
                }
                case 'duplicates': {
                    entry.from = (inputs.querySelector('[data-role="from"]')?.value || '').trim();
                    entry.to = (inputs.querySelector('[data-role="to"]')?.value || '').trim();
                    break;
                }
                case 'type': {
                    const valEl = inputs.querySelector('[data-role="value"]');
                    entry.value = valEl ? valEl.value : '';
                    break;
                }
                case 'size': {
                    entry.min = (inputs.querySelector('[data-role="min"]')?.value || '').trim();
                    entry.max = (inputs.querySelector('[data-role="max"]')?.value || '').trim();
                    break;
                }
                case 'date': {
                    entry.from = (inputs.querySelector('[data-role="from"]')?.value || '').trim();
                    entry.to = (inputs.querySelector('[data-role="to"]')?.value || '').trim();
                    break;
                }
            }
            result.push(entry);
        }
        return result;
    },

    _getAdvancedValues() {
        const values = {
            mode: 'advanced',
            conditions: this._readConditionValues(),
            files: document.getElementById('search-adv-files').checked,
            folders: document.getElementById('search-adv-folders').checked,
        };
        if (document.getElementById('search-scope-check').checked && this.scopeNode) {
            values.scopeType = this.scopeNode.type;
            values.scopeId = this.scopeNode.id;
        }
        return values;
    },

    _hasAdvancedFilters() {
        const conds = this._readConditionValues();
        return conds.some(c => {
            if (c.field === 'size') return c.min || c.max;
            if (c.field === 'date') return c.from || c.to;
            return c.value;
        });
    },

    _clearAdvanced() {
        document.getElementById('search-conditions').innerHTML = '';
        this.conditions = [];
        this.nextCondId = 0;
        document.getElementById('search-adv-files').checked = true;
        document.getElementById('search-adv-folders').checked = false;
        this._addCondition('include');
    },

    // ── Saved searches ──

    _savedParams: {},

    _initSavedSearches() {
        PromptModal.init();
        document.getElementById('search-save').addEventListener('click', () => this._saveSearch());
        document.getElementById('search-adv-save').addEventListener('click', () => this._saveSearch());
        this.loadSavedSearches();
    },

    async _saveSearch() {
        if (!this._hasFilters()) return;
        const values = this._getValues();
        const name = await PromptModal.open({
            title: 'Save Search',
            message: 'Save current search as:',
            placeholder: 'Search name...',
        });
        if (!name) return;
        const res = await API.post('/api/searches', { name, params: values });
        if (res.ok) this.loadSavedSearches();
    },

    async loadSavedSearches() {
        const res = await API.get('/api/searches');
        if (!res.ok) return;
        this._renderSavedSearches(res.data);
    },

    _renderSavedSearches(searches) {
        const container = document.getElementById('saved-searches');
        if (!searches.length) { container.innerHTML = ''; return; }

        this._savedParams = {};
        for (const s of searches) {
            this._savedParams[s.id] = typeof s.params === 'string' ? JSON.parse(s.params) : s.params;
        }

        container.innerHTML = searches.map(s =>
            `<span class="saved-search-item" data-id="${s.id}">` +
            `<span class="saved-search-name">${esc(s.name)}</span>` +
            `<span class="saved-search-delete" data-id="${s.id}">&times;</span>` +
            `</span>`
        ).join('');

        container.querySelectorAll('.saved-search-name').forEach(el => {
            el.addEventListener('click', () => {
                const item = el.closest('.saved-search-item');
                const params = this._savedParams[item.dataset.id];
                if (params) this._applySavedSearch(params);
            });
        });

        container.querySelectorAll('.saved-search-delete').forEach(el => {
            el.addEventListener('click', async (e) => {
                e.stopPropagation();
                await API.delete(`/api/searches/${el.dataset.id}`);
                this.loadSavedSearches();
            });
        });
    },

    _applySavedSearch(params) {
        if (params.mode === 'advanced') {
            // Switch to advanced mode if not already
            if (this.mode !== 'advanced') this._toggleMode();
            this._clearAdvanced();
            // Rebuild conditions from saved params
            if (params.conditions && params.conditions.length) {
                // Remove the default condition added by _clearAdvanced
                document.getElementById('search-conditions').innerHTML = '';
                this.conditions = [];
                this.nextCondId = 0;
                for (const c of params.conditions) {
                    const cond = this._addCondition(c.op || 'include');
                    // Set field
                    const row = document.querySelector(`.search-condition-row[data-cond-id="${cond.id}"]`);
                    const fieldSel = row.querySelector('select.search-select');
                    fieldSel.value = c.field;
                    cond.field = c.field;
                    const inputsDiv = row.querySelector('.search-condition-inputs');
                    this._renderConditionInputs(inputsDiv, c.field);
                    // Set values
                    switch (c.field) {
                        case 'size': {
                            const minEl = inputsDiv.querySelector('[data-role="min"]');
                            const maxEl = inputsDiv.querySelector('[data-role="max"]');
                            if (minEl && c.min) minEl.value = c.min;
                            if (maxEl && c.max) maxEl.value = c.max;
                            break;
                        }
                        case 'date': {
                            const fromEl = inputsDiv.querySelector('[data-role="from"]');
                            const toEl = inputsDiv.querySelector('[data-role="to"]');
                            if (fromEl && c.from) fromEl.value = c.from;
                            if (toEl && c.to) toEl.value = c.to;
                            break;
                        }
                        default: {
                            const valEl = inputsDiv.querySelector('[data-role="value"]');
                            const matchEl = inputsDiv.querySelector('[data-role="match"]');
                            if (valEl && c.value) valEl.value = c.value;
                            if (matchEl && c.match) matchEl.value = c.match;
                            break;
                        }
                    }
                }
            }
            if (params.folders) document.getElementById('search-adv-folders').checked = true;
            if (params.files === false) document.getElementById('search-adv-files').checked = false;
        } else {
            // Switch to basic mode if not already
            if (this.mode !== 'basic') this._toggleMode();
            // Clear all fields
            document.getElementById('search-basic').querySelectorAll('input[type="text"], input[type="date"], input[type="number"]').forEach(el => el.value = '');
            document.getElementById('search-basic').querySelectorAll('select').forEach(el => el.selectedIndex = 0);
            document.getElementById('search-files').checked = true;
            document.getElementById('search-folders').checked = false;
            document.getElementById('search-dupes').checked = false;

            if (params.name) document.getElementById('search-name').value = params.name;
            if (params.nameMatch) document.getElementById('search-name-match').value = params.nameMatch;
            if (params.type) document.getElementById('search-type').value = params.type;
            if (params.description) document.getElementById('search-desc').value = params.description;
            if (params.tags) document.getElementById('search-tags').value = params.tags;
            if (params.sizeMin) document.getElementById('search-size-min').value = params.sizeMin;
            if (params.sizeMax) document.getElementById('search-size-max').value = params.sizeMax;
            if (params.minDups) document.getElementById('search-min-dups').value = params.minDups;
            if (params.maxDups) document.getElementById('search-max-dups').value = params.maxDups;
            if (params.dateFrom) document.getElementById('search-date-from').value = params.dateFrom;
            if (params.dateTo) document.getElementById('search-date-to').value = params.dateTo;
            if (params.files === false) document.getElementById('search-files').checked = false;
            if (params.folders) document.getElementById('search-folders').checked = true;
            if (params.dupes) document.getElementById('search-dupes').checked = true;
        }

        // Show search panel and run
        this.visible = true;
        this.panelEl.classList.remove('hidden');
        this.toggleBtn.classList.add('btn-active');
        this._updateSearchBtn();
        this._doSearch();
    },
};

export default Search;
