import { wireModal, setDialogError } from './modal.js';
const RenameFileModal = {
    overlayEl: null,
    nameInput: null,
    errorEl: null,
    onConfirm: null,
    file: null,

    init(onConfirm) {
        this.overlayEl = document.getElementById('rename-file-modal');
        this.nameInput = document.getElementById('rename-file-name');
        this.errorEl = document.getElementById('rename-file-error');
        this.onConfirm = onConfirm;

        wireModal(this.overlayEl, {
            close: () => this.close(),
            submit: () => this.confirm(),
            enterFrom: [this.nameInput],
            cancelBtn: document.getElementById('rename-file-cancel'),
            submitBtn: document.getElementById('rename-file-submit'),
        });
    },

    open(file) {
        this.file = file;
        const name = file.name || '';
        this.nameInput.value = name;
        setDialogError(this.errorEl, '');
        this.overlayEl.classList.remove('hidden');
        this.nameInput.focus();
        // Select filename without extension
        const dotIdx = name.lastIndexOf('.');
        if (dotIdx > 0) {
            this.nameInput.setSelectionRange(0, dotIdx);
        } else {
            this.nameInput.select();
        }
    },

    close() {
        this.overlayEl.classList.add('hidden');
        this.file = null;
    },

    async confirm() {
        const newName = this.nameInput.value.trim();
        if (!newName) return;
        if (!this.file || !this.onConfirm) return;

        setDialogError(this.errorEl, '');

        const result = await this.onConfirm(this.file, newName);
        if (result && result.error) {
            setDialogError(this.errorEl, result.error);
            return;
        }
        this.close();
    },
};

export default RenameFileModal;
