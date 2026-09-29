import API from '../api.js';
import { createFolderPicker } from './folderpicker.js';
import { formatSize } from '../format.js';
import { wireModal } from './modal.js';


const Merge = {
    overlay: null,
    sourceNameEl: null,
    sourceStatsEl: null,
    treePicker: null,
    destDisplay: null,
    cancelBtn: null,
    submitBtn: null,
    onMerge: null,
    sourceNode: null,
    picker: null,

    init(onMerge) {
        this.onMerge = onMerge;
        this.overlay = document.getElementById('merge-modal');
        this.sourceNameEl = document.getElementById('merge-source-name');
        this.sourceStatsEl = document.getElementById('merge-source-stats');
        this.treePicker = document.getElementById('merge-tree-picker');
        this.destDisplay = document.getElementById('merge-dest-display');
        this.cancelBtn = document.getElementById('merge-cancel');
        this.submitBtn = document.getElementById('merge-submit');
        this.picker = createFolderPicker(this.treePicker, {
            isDisabled: (node) => node.online === false
                || this.picker.inSubtree(this.sourceNode && this.sourceNode.id, node.id),
            onPick: (id, label) => {
                this.destDisplay.textContent = label;
                this.submitBtn.disabled = false;
            },
        });

        wireModal(this.overlay, {
            close: () => this.close(),
            submit: () => this.doSubmit(),
            cancelBtn: this.cancelBtn,
            submitBtn: this.submitBtn,
        });
    },

    async open(sourceNode) {
        this.sourceNode = sourceNode;

        this.sourceNameEl.textContent = sourceNode.label || sourceNode.name;
        this.sourceStatsEl.textContent = 'Loading stats...';
        this.submitBtn.disabled = true;
        this.destDisplay.textContent = 'No folder selected';
        document.getElementById('merge-copy-only').checked = false;

        // Fetch stats for source
        const isLocation = String(sourceNode.id).startsWith('loc-');
        const numId = String(sourceNode.id).replace(/^(loc-|fld-)/, '');
        const statsUrl = isLocation
            ? `/api/locations/${numId}/stats`
            : `/api/folders/${numId}/stats`;
        const statsRes = await API.get(statsUrl);
        if (statsRes.ok) {
            const s = statsRes.data;
            this.sourceStatsEl.textContent = `${(s.fileCount || 0).toLocaleString()} files, ${s.totalSizeFormatted || formatSize(s.totalSize || 0)}`;
        } else {
            this.sourceStatsEl.textContent = '';
        }

        await this.picker.load();
        this.overlay.classList.remove('hidden');
    },

    close() {
        this.overlay.classList.add('hidden');
    },

    doSubmit() {
        if (!this.picker.selected) return;

        const copyOnly = document.getElementById('merge-copy-only').checked;
        this.onMerge({
            source_id: this.sourceNode.id,
            destination_id: this.picker.selected,
            mode: copyOnly ? 'copy' : 'move',
        });
        this.close();
    },
};

export default Merge;
