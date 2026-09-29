import { wireModal, setDialogError } from './modal.js';
const NewFolderModal = {
    overlayEl: null,
    nameInput: null,
    errorEl: null,
    onConfirm: null,
    parentNode: null,

    init(onConfirm) {
        this.overlayEl = document.getElementById('new-folder-modal');
        this.nameInput = document.getElementById('new-folder-name');
        this.errorEl = document.getElementById('new-folder-error');
        this.onConfirm = onConfirm;

        wireModal(this.overlayEl, {
            close: () => this.close(),
            submit: () => this.confirm(),
            enterFrom: [this.nameInput],
            cancelBtn: document.getElementById('new-folder-cancel'),
            submitBtn: document.getElementById('new-folder-submit'),
        });
    },

    open(parentNode) {
        this.parentNode = parentNode;
        this.nameInput.value = '';
        setDialogError(this.errorEl, '');
        this.overlayEl.classList.remove('hidden');
        this.nameInput.focus();
    },

    close() {
        this.overlayEl.classList.add('hidden');
        this.parentNode = null;
    },

    async confirm() {
        const name = this.nameInput.value.trim();
        if (!name) return;
        if (!this.parentNode || !this.onConfirm) return;

        const parentNode = this.parentNode;
        this.close();

        const result = await this.onConfirm(parentNode, name);
        if (result && result.error) {
            this.open(parentNode);
            this.nameInput.value = name;
            setDialogError(this.errorEl, result.error);
        }
    },
};

export default NewFolderModal;
