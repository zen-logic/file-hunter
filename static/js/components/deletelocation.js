import { wireModal } from './modal.js';
const DeleteLocationModal = {
    overlayEl: null,
    textEl: null,
    onConfirm: null,
    node: null,

    init(onConfirm) {
        this.overlayEl = document.getElementById('delete-location-modal');
        this.textEl = document.getElementById('delete-location-text');
        this.onConfirm = onConfirm;

        wireModal(this.overlayEl, {
            close: () => this.close(),
            submit: () => this.confirm(),
            enterFrom: 'dialog',
            cancelBtn: document.getElementById('delete-location-cancel'),
            submitBtn: document.getElementById('delete-location-submit'),
        });
    },

    open(node) {
        this.node = node;
        this.textEl.textContent = `Remove "${node.label}" from the catalog? This deletes all catalog entries for this location but does not remove any files from disk.`;
        this.overlayEl.classList.remove('hidden');
    },

    close() {
        this.overlayEl.classList.add('hidden');
        this.node = null;
    },

    confirm() {
        if (this.node && this.onConfirm) this.onConfirm(this.node);
        this.close();
    },
};

export default DeleteLocationModal;
