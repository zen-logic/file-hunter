import API from '../api.js';
import icons from '../icons.js';

const Consolidate = {
    overlay: null,
    filenameEl: null,
    locationsEl: null,
    modeGroup: null,
    destSection: null,
    treePicker: null,
    destDisplay: null,
    cancelBtn: null,
    submitBtn: null,
    onConsolidate: null,
    _file: null,
    _files: null,
    _dups: null,
    _isMulti: false,
    _isSearchMode: false,
    _selectedMode: 'keep_here',
    _selectedDest: null,
    _treeData: null,
    _expandedNodes: new Set(),

    init(onConsolidate) {
        this.onConsolidate = onConsolidate;
        this.overlay = document.getElementById('consolidate-modal');
        this.filenameEl = document.getElementById('consolidate-filename');
        this.locationsEl = document.getElementById('consolidate-locations');
        this.modeGroup = document.getElementById('consolidate-mode-group');
        this.destSection = document.getElementById('consolidate-dest-section');
        this.treePicker = document.getElementById('consolidate-tree-picker');
        this.destDisplay = document.getElementById('consolidate-dest-display');
        this.cancelBtn = document.getElementById('consolidate-cancel');
        this.submitBtn = document.getElementById('consolidate-submit');

        this.cancelBtn.addEventListener('click', () => this.close());
        this.overlay.addEventListener('click', (e) => {
            if (e.target === this.overlay) this.close();
        });
        document.addEventListener('keydown', (e) => {
            if (e.key === 'Escape' && !this.overlay.classList.contains('hidden')) {
                this.close();
            }
        });
        this.submitBtn.addEventListener('click', () => this._doSubmit());

        // Mode radio change
        this.modeGroup.addEventListener('change', (e) => {
            if (e.target.name === 'consolidate-mode') {
                this._selectedMode = e.target.value;
                this._updateModeUI();
            }
        });

        // Click on mode option labels for visual selection
        this.modeGroup.querySelectorAll('.consolidate-mode-option').forEach(label => {
            label.addEventListener('click', () => {
                this.modeGroup.querySelectorAll('.consolidate-mode-option').forEach(l => l.classList.remove('selected'));
                label.classList.add('selected');
            });
        });
    },

    async open(file, dups, { files = null, searchMode = false } = {}) {
        this._file = file;
        this._files = files;
        this._dups = dups;
        this._isMulti = files && files.length > 1;
        this._isSearchMode = searchMode;
        this._selectedDest = null;
        this._expandedNodes = new Set();

        // Locations list — only for single file
        const locsField = this.locationsEl.closest('.modal-field');
        if (this._isMulti) {
            this.filenameEl.textContent = `${files.length} files selected`;
            this.locationsEl.innerHTML = '';
            if (locsField) locsField.style.display = 'none';
        } else {
            this.filenameEl.textContent = file.name;
            this.locationsEl.innerHTML = dups
                .map(d => `<li>${d.location} &mdash; ${d.path}</li>`)
                .join('');
            if (locsField) locsField.style.display = '';
        }

        // Mode selector — hide entirely for search or multi with copy_to only
        const modeField = this.modeGroup.closest('.modal-field');
        const radios = this.modeGroup.querySelectorAll('input[name="consolidate-mode"]');
        const options = this.modeGroup.querySelectorAll('.consolidate-mode-option');

        if (this._isSearchMode) {
            // Search: copy_to only, hide mode selector entirely
            if (modeField) modeField.style.display = 'none';
            this._selectedMode = 'copy_to';
            radios.forEach(r => { r.checked = r.value === 'copy_to'; });
        } else {
            // Folder: both modes available
            if (modeField) modeField.style.display = '';
            options.forEach(o => o.style.display = '');
            this._selectedMode = 'keep_here';
            radios.forEach(r => { r.checked = r.value === 'keep_here'; });
        }
        options.forEach(l => l.classList.remove('selected'));
        options.forEach(o => {
            const input = o.querySelector('input');
            if (input && input.value === this._selectedMode) o.classList.add('selected');
        });

        // Fetch tree data before rendering mode UI
        const res = await API.get('/api/locations');
        if (res.ok) {
            this._treeData = res.data;
        } else {
            this._treeData = [];
        }

        this._updateModeUI();
        this.overlay.classList.remove('hidden');
    },

    close() {
        this.overlay.classList.add('hidden');
    },

    _updateModeUI() {
        if (this._selectedMode === 'copy_to') {
            this.destSection.classList.remove('hidden');
            this._renderTree();
        } else {
            this.destSection.classList.add('hidden');
        }
        this._updateDestDisplay();
    },

    _renderTree() {
        this.treePicker.innerHTML = '';
        if (!this._treeData) return;
        this._treeData.forEach(loc => {
            this._renderTreeNode(this.treePicker, loc, 0);
        });
    },

    _renderTreeNode(container, node, depth) {
        const div = document.createElement('div');
        div.className = 'ct-node';
        if (node.online === false) div.classList.add('ct-offline');
        if (this._selectedDest === node.id) div.classList.add('ct-selected');

        for (let i = 0; i < depth; i++) {
            const indent = document.createElement('span');
            indent.className = 'ct-indent';
            div.appendChild(indent);
        }

        const hasChildren = node.hasChildren || (node.children && node.children.length > 0);
        const toggle = document.createElement('span');
        toggle.className = 'ct-icon';
        if (hasChildren) {
            toggle.textContent = this._expandedNodes.has(node.id) ? '\u25BE' : '\u25B8';
        }
        div.appendChild(toggle);

        const icon = document.createElement('span');
        icon.className = 'ct-icon';
        icon.innerHTML = node.type === 'location' ? icons.location : icons.folder;
        div.appendChild(icon);

        const label = document.createElement('span');
        label.className = 'ct-label';
        label.textContent = node.label;
        div.appendChild(label);

        div.addEventListener('click', async (e) => {
            e.stopPropagation();
            if (node.online === false) return;

            if (hasChildren) {
                if (this._expandedNodes.has(node.id)) {
                    this._expandedNodes.delete(node.id);
                } else {
                    this._expandedNodes.add(node.id);
                    // Lazy-load children if not yet fetched
                    if (node.children === null) {
                        const numId = node.id.replace('fld-', '');
                        const res = await API.get(`/api/tree/children?ids=${numId}`);
                        if (res.ok && res.data[node.id]) {
                            node.children = res.data[node.id];
                        } else {
                            node.children = [];
                        }
                    }
                }
            }
            this._selectedDest = node.id;
            this._updateDestDisplay();
            this._renderTree();
        });

        container.appendChild(div);

        if (node.children && node.children.length > 0 && this._expandedNodes.has(node.id)) {
            node.children.forEach(child => this._renderTreeNode(container, child, depth + 1));
        }
    },

    _updateDestDisplay() {
        if (this._selectedMode !== 'copy_to' || !this._selectedDest) {
            this.destDisplay.textContent = 'No folder selected';
            return;
        }
        const node = this._findNode(this._treeData, this._selectedDest);
        if (node) {
            this.destDisplay.textContent = node.label;
        }
    },

    _findNode(nodes, id) {
        for (const n of nodes) {
            if (n.id === id) return n;
            if (n.children) {
                const found = this._findNode(n.children, id);
                if (found) return found;
            }
        }
        return null;
    },

    _doSubmit() {
        if (this._selectedMode === 'copy_to' && !this._selectedDest) return;

        if (this._isMulti) {
            this.onConsolidate({
                file_ids: this._files.map(f => f.id),
                mode: this._selectedMode,
                destination_folder_id: this._selectedMode === 'copy_to' ? this._selectedDest : undefined,
                batch: true,
            });
        } else {
            this.onConsolidate({
                file_id: this._file.id,
                mode: this._selectedMode,
                destination_folder_id: this._selectedMode === 'copy_to' ? this._selectedDest : undefined,
            });
        }
        this.close();
    },
};

export default Consolidate;
