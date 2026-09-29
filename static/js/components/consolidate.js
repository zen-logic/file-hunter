import API from '../api.js';
import icons from '../icons.js';
import { createFolderPicker } from './folderpicker.js';
import { wireModal } from './modal.js';

const Consolidate = {
    // DOM — info step
    overlay: null,
    stepInfo: null,
    subtitle: null,
    fileList: null,
    modeGroup: null,
    filenameMatchCheck: null,

    // DOM — merge step
    stepMerge: null,
    mergeList: null,
    mergeNextBtn: null,
    mergeSelectAll: null,

    // DOM — destination step
    stepDest: null,
    treePicker: null,
    destDisplay: null,

    // State
    file: null,
    files: null,
    allDups: [],
    dups: [],
    mode: 'copy',
    checkedDupIds: new Set(),
    picker: null,
    onConsolidate: null,
    onDone: null,

    init(onConsolidate) {
        this.onConsolidate = onConsolidate;

        this.overlay = document.getElementById('consolidate-modal');
        this.stepInfo = document.getElementById('consolidate-step-info');
        this.stepMerge = document.getElementById('consolidate-step-merge');
        this.stepDest = document.getElementById('consolidate-step-dest');
        this.subtitle = document.getElementById('consolidate-subtitle');
        this.fileList = document.getElementById('consolidate-file-list');
        this.modeGroup = document.getElementById('consolidate-mode-group');
        this.filenameMatchCheck = document.getElementById('consolidate-filename-match');
        this.mergeList = document.getElementById('consolidate-merge-list');
        this.mergeNextBtn = document.getElementById('consolidate-merge-next');
        this.mergeSelectAll = document.getElementById('consolidate-merge-select-all');
        this.treePicker = document.getElementById('consolidate-tree-picker');
        this.destDisplay = document.getElementById('consolidate-dest-display');
        this.picker = createFolderPicker(this.treePicker, {
            isDisabled: (node) => node.online === false,
            onPick: (id, label) => {
                this.destDisplay.textContent = label;
            },
            renderTop: (picker) => this.renderKeepHere(picker),
        });

        // Info step
        document.getElementById('consolidate-cancel').addEventListener('click', () => this.close());
        document.getElementById('consolidate-next').addEventListener('click', () => this.afterInfoStep());

        // Merge step
        document.getElementById('consolidate-merge-cancel').addEventListener('click', () => this.close());
        this.mergeNextBtn.addEventListener('click', () => this.showDestStep());
        this.mergeSelectAll.addEventListener('change', () => {
            if (this.mergeSelectAll.checked) {
                this.dups.forEach(d => this.checkedDupIds.add(d.fileId));
            } else {
                this.checkedDupIds.clear();
            }
            this.mergeSelectAll.indeterminate = false;
            this.mergeNextBtn.disabled = this.checkedDupIds.size === 0;
            this.renderMergeList();
        });

        // Destination step
        document.getElementById('consolidate-dest-cancel').addEventListener('click', () => this.close());
        document.getElementById('consolidate-submit').addEventListener('click', () => this.doSubmit());

        // Overlay + escape
        wireModal(this.overlay, {
            close: () => this.close(),
            submit: () => this.doSubmit(),
            enterFrom: 'dialog',
        });

        // Mode radio
        this.modeGroup.addEventListener('change', (e) => {
            if (e.target.name === 'consolidate-mode') {
                this.mode = e.target.value;
            }
        });
        this.modeGroup.querySelectorAll('.consolidate-mode-option').forEach(label => {
            label.addEventListener('click', () => {
                this.modeGroup.querySelectorAll('.consolidate-mode-option').forEach(l => l.classList.remove('selected'));
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
        this.file = file || null;
        this.files = files || null;
        this.onDone = onDone || null;
        this.picker.selected = null;
        this.checkedDupIds = new Set();
        this.mode = 'copy';
        this.filenameMatchCheck.checked = false;

        // Reset mode selection
        const radios = this.modeGroup.querySelectorAll('input[name="consolidate-mode"]');
        radios.forEach(r => { r.checked = r.value === 'copy'; });
        const options = this.modeGroup.querySelectorAll('.consolidate-mode-option');
        options.forEach(o => o.classList.remove('selected'));
        options[0].classList.add('selected');

        // Subtitle and file list
        if (this.files && this.files.length > 0) {
            const n = this.files.length;
            this.subtitle.textContent = `${n} file${n !== 1 ? 's' : ''}`;
            this.renderFileList(this.files);
            this.fileList.classList.remove('hidden');
        } else if (this.file) {
            this.subtitle.textContent = this.file.name;
            this.fileList.innerHTML = '';
            this.fileList.classList.add('hidden');
        }

        // Load all copies via preview endpoint (includes source files
        // since the merge step needs to show them for move operations)
        const previewIds = this.files
            ? this.files.map(f => f.id)
            : this.file ? [this.file.id] : [];
        if (previewIds.length > 0) {
            const preview = await API.post('/api/consolidate/preview', {
                file_ids: previewIds,
            });
            this.allDups = (preview.ok && preview.data.duplicates) ? preview.data.duplicates : [];
        } else {
            this.allDups = [];
        }

        this.showStep(this.stepInfo);
        this.overlay.classList.remove('hidden');
    },

    close() {
        this.overlay.classList.add('hidden');
        const cb = this.onDone;
        this.onDone = null;
        if (cb) cb();
    },

    // ── Step management ──

    showStep(step) {
        this.stepInfo.classList.add('hidden');
        this.stepMerge.classList.add('hidden');
        this.stepDest.classList.add('hidden');
        step.classList.remove('hidden');
    },

    // ── Info step ──

    renderFileList(files) {
        this.fileList.innerHTML = '';
        const max = 5;
        const shown = files.slice(0, max);
        for (const f of shown) {
            const div = document.createElement('div');
            div.textContent = f.name;
            this.fileList.appendChild(div);
        }
        if (files.length > max) {
            const more = document.createElement('div');
            more.textContent = `...and ${files.length - max} more`;
            more.style.opacity = '0.5';
            this.fileList.appendChild(more);
        }
    },

    getFilteredDups() {
        let dups = this.allDups;
        if (this.filenameMatchCheck.checked) {
            const sourceNames = new Set();
            if (this.file) sourceNames.add(this.file.name);
            if (this.files) this.files.forEach(f => sourceNames.add(f.name));
            dups = dups.filter(d => sourceNames.has(d.name));
        }
        return dups;
    },

    afterInfoStep() {
        this.dups = this.getFilteredDups();

        if (this.mode === 'move' && this.dups.length > 0) {
            this.showMergeStep();
        } else {
            this.showDestStep();
        }
    },

    // ── Merge step (move only) ──

    showMergeStep() {
        this.checkedDupIds = new Set();
        this.mergeNextBtn.disabled = true;
        this.mergeSelectAll.checked = false;
        this.mergeSelectAll.indeterminate = false;
        this.renderMergeList();
        this.showStep(this.stepMerge);
    },

    renderMergeList() {
        this.mergeList.innerHTML = '';

        for (const d of this.dups) {
            const label = document.createElement('label');
            label.className = 'consolidate-merge-item';

            const checkbox = document.createElement('input');
            checkbox.type = 'checkbox';
            checkbox.checked = this.checkedDupIds.has(d.fileId);
            checkbox.addEventListener('change', () => {
                if (checkbox.checked) {
                    this.checkedDupIds.add(d.fileId);
                } else {
                    this.checkedDupIds.delete(d.fileId);
                }
                this.mergeNextBtn.disabled = this.checkedDupIds.size === 0;
                this.updateMergeSelectAllState();
            });
            label.appendChild(checkbox);

            const text = document.createElement('span');
            const agent = d.agent ? ` [${d.agent}]` : '';
            text.textContent = `${d.location}${agent} ${d.path}`;
            label.appendChild(text);

            this.mergeList.appendChild(label);
        }
    },

    updateMergeSelectAllState() {
        const total = this.dups.length;
        const n = this.checkedDupIds.size;
        this.mergeSelectAll.checked = total > 0 && n >= total;
        this.mergeSelectAll.indeterminate = n > 0 && n < total;
    },

    // ── Destination step ──

    async showDestStep() {
        await this.picker.load();
        this.destDisplay.textContent = 'No folder selected';
        this.showStep(this.stepDest);
    },

    /** "Consolidate in place", above the favourites — move mode only. */
    renderKeepHere(picker) {
        if (this.mode !== 'move') return;
        const keepDiv = picker.row(0);
        if (picker.selected === 'keep_here') keepDiv.classList.add('ct-selected');
        picker.addIcon(keepDiv, icons.location);
        const label = picker.addLabel(keepDiv, 'Consolidate in place');
        label.style.fontWeight = 'var(--font-weight-semibold)';
        keepDiv.addEventListener('click', (e) => {
            e.stopPropagation();
            picker.select('keep_here', 'Consolidate in place');
        });
        picker.container.appendChild(keepDiv);
        picker.addDivider();
    },

    // ── Submit ──

    doSubmit() {
        if (!this.picker.selected) return;

        const fnMatch = this.filenameMatchCheck.checked;
        const isKeepHere = this.picker.selected === 'keep_here';

        const params = {
            consolidateMode: this.mode,
        };

        if (this.files && this.files.length > 0) {
            params.file_ids = this.files.map(f => f.id);
            params.batch = true;
        } else if (this.file) {
            params.file_id = this.file.id;
        }

        // Map to backend modes
        if (this.mode === 'move' && isKeepHere) {
            params.mode = 'keep_here';
        } else {
            params.mode = 'move_to';
            params.destination_folder_id = this.picker.selected;
        }

        if (fnMatch) params.filename_match_only = true;

        // For move mode, pass the selected file IDs to stub
        if (this.mode === 'move' && this.checkedDupIds.size > 0) {
            params.stub_file_ids = Array.from(this.checkedDupIds);
        }

        if (this.onConsolidate) this.onConsolidate(params);
        this.close();
    },
};

export default Consolidate;
