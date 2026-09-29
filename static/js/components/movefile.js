import { createFolderPicker } from './folderpicker.js';
import { wireModal, setDialogError } from './modal.js';

const MoveFileModal = {
    overlay: null,
    fileNameEl: null,
    treePicker: null,
    destDisplay: null,
    errorEl: null,
    cancelBtn: null,
    submitBtn: null,
    onMove: null,
    file: null,
    excludeId: null,
    picker: null,

    init(onMove) {
        this.onMove = onMove;
        this.overlay = document.getElementById('move-file-modal');
        this.fileNameEl = document.getElementById('move-file-name');
        this.treePicker = document.getElementById('move-file-tree-picker');
        this.destDisplay = document.getElementById('move-file-dest-display');
        this.errorEl = document.getElementById('move-file-error');
        this.cancelBtn = document.getElementById('move-file-cancel');
        this.submitBtn = document.getElementById('move-file-submit');
        this.copyCheckbox = document.getElementById('move-file-copy');
        this.picker = createFolderPicker(this.treePicker, {
            isDisabled: (node) => this.picker.inSubtree(this.excludeId, node.id),
            offlineSelectable: true,
            onPick: (id, label, node) => {
                this.destDisplay.textContent = node.online === false
                    ? `${label} (offline \u2014 will be queued)`
                    : label;
                this.submitBtn.disabled = false;
            },
        });

        wireModal(this.overlay, {
            close: () => this.close(),
            submit: () => this.doSubmit(),
            enterFrom: 'dialog',
            cancelBtn: this.cancelBtn,
            submitBtn: this.submitBtn,
        });
        this.copyCheckbox.addEventListener('change', () => {
            this.submitBtn.textContent = this.copyCheckbox.checked ? 'Copy' : 'Move';
        });
    },

    async open(file, excludeId) {
        this.file = file;
        this.excludeId = excludeId || null;

        this.fileNameEl.textContent = file.name;
        this.submitBtn.disabled = true;
        this.submitBtn.textContent = 'Move';
        this.copyCheckbox.checked = false;
        this.destDisplay.textContent = 'No folder selected';
        setDialogError(this.errorEl, '');

        await this.picker.load();
        this.overlay.classList.remove('hidden');
    },

    close() {
        this.overlay.classList.add('hidden');
    },

    setBusy(busy) {
        this.submitBtn.disabled = busy;
        this.cancelBtn.disabled = busy;
        this.copyCheckbox.disabled = busy;
        this.treePicker.style.pointerEvents = busy ? 'none' : '';
        this.treePicker.style.opacity = busy ? '0.5' : '';
        if (busy) {
            const verb = this.copyCheckbox.checked ? 'Copying' : 'Moving';
            this.submitBtn.innerHTML = `<span class="detail-spinner" style="width:1rem;height:1rem;display:inline-block;vertical-align:middle;margin-right:0.4rem"></span>${verb}\u2026`;
        } else {
            this.submitBtn.textContent = this.copyCheckbox.checked ? 'Copy' : 'Move';
        }
    },

    async doSubmit() {
        if (!this.picker.selected || !this.file) return;

        setDialogError(this.errorEl, '');

        this.setBusy(true);
        const copy = this.copyCheckbox.checked;
        const result = await this.onMove(this.file, this.picker.selected, copy);
        if (result && result.error) {
            this.setBusy(false);
            setDialogError(this.errorEl, result.error);
            return;
        }
        this.setBusy(false);
        this.close();
    },
};

export default MoveFileModal;
