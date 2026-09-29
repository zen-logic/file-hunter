import { wireModal, setDialogError } from './modal.js';
const RenameLocationModal = {
    overlayEl: null,
    nameInput: null,
    errorEl: null,
    onConfirm: null,
    node: null,

    init(onConfirm) {
        this.overlayEl = document.getElementById('rename-location-modal');
        this.nameInput = document.getElementById('rename-loc-name');
        this.errorEl = document.getElementById('rename-loc-error');
        this.onConfirm = onConfirm;

        wireModal(this.overlayEl, {
            close: () => this.close(),
            submit: () => this.confirm(),
            enterFrom: [this.nameInput],
            cancelBtn: document.getElementById('rename-loc-cancel'),
            submitBtn: document.getElementById('rename-loc-submit'),
        });
    },

    open(node) {
        this.node = node;
        this.nameInput.value = node.label;
        setDialogError(this.errorEl, '');
        this.overlayEl.classList.remove('hidden');
        this.nameInput.focus();
        this.nameInput.select();
    },

    close() {
        this.overlayEl.classList.add('hidden');
        this.node = null;
    },

    async confirm() {
        const newName = this.nameInput.value.trim();
        if (!newName) return;
        if (!this.node || !this.onConfirm) return;

        setDialogError(this.errorEl, '');

        const result = await this.onConfirm(this.node, newName);
        if (result && result.error) {
            setDialogError(this.errorEl, result.error);
            return;
        }
        this.close();
    },
};

export default RenameLocationModal;
