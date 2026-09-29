import { wireModal, setDialogError } from './modal.js';
const RenameFolderModal = {
    overlayEl: null,
    nameInput: null,
    errorEl: null,
    onConfirm: null,
    folder: null,

    init(onConfirm) {
        this.overlayEl = document.getElementById('rename-folder-modal');
        this.nameInput = document.getElementById('rename-folder-name');
        this.errorEl = document.getElementById('rename-folder-error');
        this.onConfirm = onConfirm;

        wireModal(this.overlayEl, {
            close: () => this.close(),
            submit: () => this.confirm(),
            enterFrom: [this.nameInput],
            cancelBtn: document.getElementById('rename-folder-cancel'),
            submitBtn: document.getElementById('rename-folder-submit'),
        });
    },

    open(folder) {
        this.folder = folder;
        this.nameInput.value = folder.label || folder.name || '';
        setDialogError(this.errorEl, '');
        this.overlayEl.classList.remove('hidden');
        this.nameInput.focus();
        this.nameInput.select();
    },

    close() {
        this.overlayEl.classList.add('hidden');
        this.folder = null;
    },

    async confirm() {
        const newName = this.nameInput.value.trim();
        if (!newName) return;
        if (!this.folder || !this.onConfirm) return;

        setDialogError(this.errorEl, '');

        const folder = this.folder;
        this.close();
        const result = await this.onConfirm(folder, newName);
        if (result && result.error) {
            // Re-open with error if the rename failed
            this.open(folder);
            this.nameInput.value = newName;
            setDialogError(this.errorEl, result.error);
        }
    },
};

export default RenameFolderModal;
