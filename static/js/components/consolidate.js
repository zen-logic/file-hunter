import API from '../api.js';
import icons from '../icons.js';

const Consolidate = {
    // DOM — step 1
    _overlay: null,
    _step1: null,
    _subtitle: null,
    _fileList: null,
    _dupsEl: null,
    _modeGroup: null,
    _filenameMatchCheck: null,

    // DOM — step 2
    _step2: null,
    _treePicker: null,
    _destDisplay: null,

    // State
    _file: null,
    _files: null,
    _allDups: [],
    _dups: [],
    _mode: 'copy',
    _selectedDest: null,
    _treeData: null,
    _favourites: [],
    _expandedNodes: new Set(),
    _onConsolidate: null,
    _onDone: null,

    init(onConsolidate) {
        this._onConsolidate = onConsolidate;

        this._overlay = document.getElementById('consolidate-modal');
        this._step1 = document.getElementById('consolidate-step1');
        this._step2 = document.getElementById('consolidate-step2');
        this._subtitle = document.getElementById('consolidate-subtitle');
        this._fileList = document.getElementById('consolidate-file-list');
        this._dupsEl = document.getElementById('consolidate-dups');
        this._modeGroup = document.getElementById('consolidate-mode-group');
        this._filenameMatchCheck = document.getElementById('consolidate-filename-match');
        this._treePicker = document.getElementById('consolidate-tree-picker');
        this._destDisplay = document.getElementById('consolidate-dest-display');

        const cancelBtn = document.getElementById('consolidate-cancel');
        const nextBtn = document.getElementById('consolidate-next');
        const cancelBtn2 = document.getElementById('consolidate-cancel2');
        const submitBtn = document.getElementById('consolidate-submit');

        cancelBtn.addEventListener('click', () => this.close());
        nextBtn.addEventListener('click', () => this._showStep2());
        cancelBtn2.addEventListener('click', () => this.close());
        submitBtn.addEventListener('click', () => this._doSubmit());

        this._overlay.addEventListener('click', (e) => {
            if (e.target === this._overlay) this.close();
        });

        document.addEventListener('keydown', (e) => {
            if (e.key === 'Escape' && !this._overlay.classList.contains('hidden')) {
                this.close();
            }
        });

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

        this._filenameMatchCheck.addEventListener('change', () => this._renderDups());
    },

    /**
     * Open the consolidate dialog.
     *
     * @param {Object} opts
     * @param {Object} [opts.file]  Single file object (id, name, locationId)
     * @param {Array}  [opts.files] Array of file objects for batch/triaged
     * @param {Array}  [opts.dups]  Pre-loaded dup data (single file from detail panel)
     * @param {Function} [opts.onDone] Called when dialog closes (submit or cancel)
     */
    async open({ file, files, dups, onDone } = {}) {
        this._file = file || null;
        this._files = files || null;
        this._onDone = onDone || null;
        this._selectedDest = null;
        this._expandedNodes = new Set();
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

        // Load duplicates
        if (dups && dups.length > 0) {
            this._allDups = dups;
        } else if (this._files && this._files.length > 0) {
            const preview = await API.post('/api/consolidate/preview', {
                file_ids: this._files.map(f => f.id),
            });
            this._allDups = (preview.ok && preview.data.duplicates) ? preview.data.duplicates : [];
        } else if (this._file) {
            // Single file with no pre-loaded dups — fetch via preview
            const preview = await API.post('/api/consolidate/preview', {
                file_ids: [this._file.id],
            });
            this._allDups = (preview.ok && preview.data.duplicates) ? preview.data.duplicates : [];
        } else {
            this._allDups = [];
        }

        this._renderDups();

        // Show step 1
        this._step1.classList.remove('hidden');
        this._step2.classList.add('hidden');
        this._overlay.classList.remove('hidden');
    },

    close() {
        this._overlay.classList.add('hidden');
        const cb = this._onDone;
        this._onDone = null;
        if (cb) cb();
    },

    // ── Step 1 rendering ──

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

    _renderDups() {
        const fnMatch = this._filenameMatchCheck.checked;
        let dups = this._allDups;

        if (fnMatch) {
            const sourceNames = new Set();
            if (this._file) sourceNames.add(this._file.name);
            if (this._files) this._files.forEach(f => sourceNames.add(f.name));
            dups = dups.filter(d => sourceNames.has(d.name));
        }

        this._dups = dups;
        this._dupsEl.innerHTML = '';

        if (dups.length === 0) {
            const div = document.createElement('div');
            div.textContent = 'No duplicates found';
            div.style.color = 'var(--color-text-placeholder)';
            this._dupsEl.appendChild(div);
            return;
        }

        for (const d of dups) {
            const div = document.createElement('div');
            const agent = d.agent ? ` [${d.agent}]` : '';
            div.textContent = `${d.location}${agent} ${d.path}`;
            this._dupsEl.appendChild(div);
        }
    },

    // ── Step 2 ──

    async _showStep2() {
        const [res, favRes] = await Promise.all([
            API.get('/api/locations'),
            API.get('/api/favourites'),
        ]);
        this._treeData = res.ok ? res.data : [];
        this._favourites = favRes.ok ? favRes.data : [];

        this._selectedDest = null;
        this._destDisplay.textContent = 'No folder selected';
        this._renderTree();

        this._step1.classList.add('hidden');
        this._step2.classList.remove('hidden');
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

        // Map to existing backend modes for now
        if (this._mode === 'move' && isKeepHere) {
            params.mode = 'keep_here';
        } else {
            params.mode = 'move_to';
            params.destination_folder_id = this._selectedDest;
        }

        if (fnMatch) params.filename_match_only = true;

        if (this._onConsolidate) this._onConsolidate(params);
        this.close();
    },
};

export default Consolidate;
