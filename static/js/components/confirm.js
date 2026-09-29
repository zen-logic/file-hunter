import { wireModal } from './modal.js';
const ConfirmModal = {
    overlayEl: null,
    titleEl: null,
    textEl: null,
    submitEl: null,
    cancelEl: null,
    pendingResolve: null,

    init() {
        this.overlayEl = document.getElementById('confirm-modal');
        this.titleEl = document.getElementById('confirm-modal-title');
        this.textEl = document.getElementById('confirm-modal-text');
        this.submitEl = document.getElementById('confirm-modal-submit');
        this.cancelEl = document.getElementById('confirm-modal-cancel');

        wireModal(this.overlayEl, {
            close: () => this.complete(false),
            submit: () => this.complete(true),
            enterFrom: 'dialog',
            cancelBtn: this.cancelEl,
            submitBtn: this.submitEl,
        });
    },

    /** Show the modal and return a promise that resolves true (confirm) or false (cancel). */
    open({ title = 'Confirm', message, confirmLabel = 'OK', alert = false } = {}) {
        this.titleEl.textContent = title;
        this.textEl.textContent = message;
        this.submitEl.textContent = confirmLabel;
        this.cancelEl.classList.toggle('hidden', alert);
        this.overlayEl.classList.remove('hidden');
        return new Promise((resolve) => { this.pendingResolve = resolve; });
    },

    complete(result) {
        this.overlayEl.classList.add('hidden');
        this.cancelEl.classList.remove('hidden');
        if (this.pendingResolve) {
            this.pendingResolve(result);
            this.pendingResolve = null;
        }
    },
};

export default ConfirmModal;
