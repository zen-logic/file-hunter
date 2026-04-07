import API from '../api.js';
import icons from '../icons.js';

const Consolidate = {
    // DOM — info step
    _overlay: null,
    _stepInfo: null,
    _subtitle: null,
    _fileList: null,
    _modeGroup: null,
    _filenameMatchCheck: null,

    // DOM — merge step
    _stepMerge: null,
    _mergeList: null,
    _mergeNextBtn: null,

    // DOM — destination step
    _stepDest: null,
    _treePicker: null,
    _destDisplay: null,

    // State
    _file: null,
    _files: null,
    _allDups: [],
    _dups: [],
    _mode: 'copy',
    _checkedDupIds: new Set(),
    _selectedDest: null,
    _treeData: null,
    _favourites: [],
    _expandedNodes: new Set(),
    _onConsolidate: null,
    _onDone: null,

    init(onConsolidate) {
        this._onConsolidate = onConsolidate;

        this._overlay = document.getElementById('consolidate-modal');
        this._stepInfo = document.getElementById('consolidate-step-info');
        this._stepMerge = document.getElementById('consolidate-step-merge');
        this._stepDest = document.getElementById('consolidate-step-dest');
        this._subtitle = document.getElementById('consolidate-subtitle');
        this._fileList = document.getElementById('consolidate-file-list');
        this._modeGroup = document.getElementById('consolidate-mode-group');
        this._filenameMatchCheck = document.getElementById('consolidate-filename-match');
        this._mergeList = document.getElementById('consolidate-merge-list');
        this._mergeNextBtn = document.getElementById('consolidate-merge-next');
        this._treePicker = document.getElementById('consolidate-tree-picker');
        this._destDisplay = document.getElementById('consolidate-dest-display');

        // Info step
        document.getElementById('consolidate-cancel').addEventListener('click', () => this.close());
        document.getElementById('consolidate-next').addEventListener('click', () => this._afterInfoStep());

        // Merge step
        document.getElementById('consolidate-merge-cancel').addEventListener('click', () => this.close());
        this._mergeNextBtn.addEventListener('click', () => this._showDestStep());

        // Destination step
        document.getElementById('consolidate-dest-cancel').addEventListener('click', () => this.close());
        document.getElementById('consolidate-submit').addEventListener('click', () => this._doSubmit());

        // Overlay + escape
        this._overlay.addEventListener('click', (e) => {
            if (e.target === this._overlay) this.close();
        });
        document.addEventListener('keydown', (e) => {
            if (e.key === 'Escape' && !this._overlay.classList.contains('hidden')) {
                this.close();
            }
        });

        // Mode radio
        this._modeGroup.addEventListener('change', (e) => {
            if (e.target.name === 'consolidate-mode') {
                this._mode = e.target.value;
            }
        });
        this._modeGroup.querySelectorAll('.consolidate-mode-option').forEach(label => {
            label.addEventListener('click', () => {
                this._modeGroup.querySelectorAll('.consolidate-mode-option').forEach(l => l.classList.remove('selected'));
                label.classList.add('selected');
            });
        });
    },

    /**
     * Open the consolidate dialog.
     *
     * @param {Object} opts
     * @param {Object} [opts.file]  Single file object (id, name, locationId)
     * @param {Array}  [opts.files] Array of file objects for batch/triaged
     * @param {Function} [opts.onDone] Called when dialog closes (submit or cancel)
     */
    async open({ file, files, onDone } = {}) {
        this._file = file || null;
        this._files = files || null;
        this._onDone = onDone || null;
        this._selectedDest = null;
        this._expandedNodes = new Set();
        this._checkedDupIds = new Set();
        this._mode = 'copy';
        this._filenameMatchCheck.checked = false;

        // Reset mode selection
        const radios = this._modeGroup.querySelectorAll('input[name="consolidate-mode"]');
        radios.forEach(r => { r.checked = r.value === 'copy'; });
        const options = this._modeGroup.querySelectorAll('.consolidate-mode-option');
        options.forEach(o => o.classList.remove('selected'));
        options[0].classList.add('selected');

        // Subtitle and file list
        if (this._files && this._files.length > 0) {
            const n = this._files.length;
            this._subtitle.textContent = `${n} file${n !== 1 ? 's' : ''}`;
            this._renderFileList(this._files);
            this._fileList.classList.remove('hidden');
        } else if (this._file) {
            this._subtitle.textContent = this._file.name;
            this._fileList.innerHTML = '';
            this._fileList.classList.add('hidden');
        }

        // Load all copies via preview endpoint (includes source files
        // since the merge step needs to show them for move operations)
        const previewIds = this._files
            ? this._files.map(f => f.id)
            : this._file ? [this._file.id] : [];
        if (previewIds.length > 0) {
            const preview = await API.post('/api/consolidate/preview', {
                file_ids: previewIds,
            });
            this._allDups = (preview.ok && preview.data.duplicates) ? preview.data.duplicates : [];
        } else {
            this._allDups = [];
        }

        // Show info step
        this._showStep(this._stepInfo);
        this._overlay.classList.remove('hidden');
    },

    close() {
        this._overlay.classList.add('hidden');
        const cb = this._onDone;
        this._onDone = null;
        if (cb) cb();
    },

    // ── Step management ──

    _showStep(step) {
        this._stepInfo.classList.add('hidden');
        this._stepMerge.classList.add('hidden');
        this._stepDest.classList.add('hidden');
        step.classList.remove('hidden');
    },

    // ── Info step ──

    _renderFileList(files) {
        this._fileList.innerHTML = '';
        const max = 5;
        const shown = files.slice(0, max);
        for (const f of shown) {
            const div = document.createElement('div');
            div.textContent = f.name;
            this._fileList.appendChild(div);
        }
        if (files.length > max) {
            const more = document.createElement('div');
            more.textContent = `...and ${files.length - max} more`;
            more.style.opacity = '0.5';
            this._fileList.appendChild(more);
        }
    },

    _getFilteredDups() {
        let dups = this._allDups;
        if (this._filenameMatchCheck.checked) {
            const sourceNames = new Set();
            if (this._file) sourceNames.add(this._file.name);
            if (this._files) this._files.forEach(f => sourceNames.add(f.name));
            dups = dups.filter(d => sourceNames.has(d.name));
        }
        return dups;
    },

    _afterInfoStep() {
        this._dups = this._getFilteredDups();

        if (this._mode === 'move' && this._dups.length > 0) {
            this._showMergeStep();
        } else {
            this._showDestStep();
        }
    },

    // ── Merge step (move only) ──

    _showMergeStep() {
        this._checkedDupIds = new Set();
        this._mergeNextBtn.disabled = true;
        this._renderMergeList();
        this._showStep(this._stepMerge);
    },

    _renderMergeList() {
        this._mergeList.innerHTML = '';

        for (const d of this._dups) {
            const label = document.createElement('label');
            label.className = 'consolidate-merge-item';

            const checkbox = document.createElement('input');
            checkbox.type = 'checkbox';
            checkbox.checked = this._checkedDupIds.has(d.fileId);
            checkbox.addEventListener('change', () => {
                if (checkbox.checked) {
                    this._checkedDupIds.add(d.fileId);
                } else {
                    this._checkedDupIds.delete(d.fileId);
                }
                this._mergeNextBtn.disabled = this._checkedDupIds.size === 0;
            });
            label.appendChild(checkbox);

            const text = document.createElement('span');
            const agent = d.agent ? ` [${d.agent}]` : '';
            text.textContent = `${d.location}${agent} ${d.path}`;
            label.appendChild(text);

            this._mergeList.appendChild(label);
        }
    },

    // ── Destination step ──

    async _showDestStep() {
        const [res, favRes] = await Promise.all([
            API.get('/api/locations'),
            API.get('/api/favourites'),
        ]);
        this._treeData = res.ok ? res.data : [];
        this._favourites = favRes.ok ? favRes.data : [];

        this._selectedDest = null;
        this._destDisplay.textContent = 'No folder selected';
        this._renderTree();
        this._showStep(this._stepDest);
    },

    _renderTree() {
        this._treePicker.innerHTML = '';
        if (!this._treeData) return;

        // "Consolidate in place" option — move mode only
        if (this._mode === 'move') {
            const keepDiv = document.createElement('div');
            keepDiv.className = 'ct-node';
            if (this._selectedDest === 'keep_here') keepDiv.classList.add('ct-selected');

            const icon = document.createElement('span');
            icon.className = 'ct-icon';
            icon.innerHTML = icons.location;
            keepDiv.appendChild(icon);

            const label = document.createElement('span');
            label.className = 'ct-label';
            label.style.fontWeight = 'var(--font-weight-semibold)';
            label.textContent = 'Consolidate in place';
            keepDiv.appendChild(label);

            keepDiv.addEventListener('click', (e) => {
                e.stopPropagation();
                this._selectedDest = 'keep_here';
                this._destDisplay.textContent = 'Consolidate in place';
                this._renderTree();
            });

            this._treePicker.appendChild(keepDiv);

            const divider = document.createElement('div');
            divider.className = 'ct-divider';
            this._treePicker.appendChild(divider);
        }

        this._renderFavourites(this._treePicker);
        this._treeData.forEach(loc => {
            this._renderTreeNode(this._treePicker, loc, 0);
        });
    },

    _renderFavourites(container) {
        if (!this._favourites || this._favourites.length === 0) return;

        const header = document.createElement('div');
        header.className = 'ct-section-header';
        header.textContent = 'Favourites';
        container.appendChild(header);

        for (const fav of this._favourites) {
            const div = document.createElement('div');
            div.className = 'ct-node';
            if (this._selectedDest === fav.id) div.classList.add('ct-selected');

            const heartIcon = document.createElement('span');
            heartIcon.className = 'ct-icon';
            heartIcon.innerHTML = icons.heart;
            div.appendChild(heartIcon);

            const label = document.createElement('span');
            label.className = 'ct-label';
            label.textContent = fav.path;
            div.appendChild(label);

            div.addEventListener('click', (e) => {
                e.stopPropagation();
                this._selectedDest = fav.id;
                this._destDisplay.textContent = fav.path;
                this._renderTree();
            });

            container.appendChild(div);
        }

        const divider = document.createElement('div');
        divider.className = 'ct-divider';
        container.appendChild(divider);
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
            this._destDisplay.textContent = node.label;
            this._renderTree();
        });

        container.appendChild(div);

        if (node.children && node.children.length > 0 && this._expandedNodes.has(node.id)) {
            node.children.forEach(child => this._renderTreeNode(container, child, depth + 1));
        }
    },

    // ── Submit ──

    _doSubmit() {
        if (!this._selectedDest) return;

        const fnMatch = this._filenameMatchCheck.checked;
        const isKeepHere = this._selectedDest === 'keep_here';

        const params = {
            consolidateMode: this._mode,
        };

        if (this._files && this._files.length > 0) {
            params.file_ids = this._files.map(f => f.id);
            params.batch = true;
        } else if (this._file) {
            params.file_id = this._file.id;
        }

        // Map to backend modes
        if (this._mode === 'move' && isKeepHere) {
            params.mode = 'keep_here';
        } else {
            params.mode = 'move_to';
            params.destination_folder_id = this._selectedDest;
        }

        if (fnMatch) params.filename_match_only = true;

        // For move mode, pass the selected file IDs to stub
        if (this._mode === 'move' && this._checkedDupIds.size > 0) {
            params.stub_file_ids = Array.from(this._checkedDupIds);
        }

        if (this._onConsolidate) this._onConsolidate(params);
        this.close();
    },
};

export default Consolidate;
