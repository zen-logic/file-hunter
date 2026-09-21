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

        this.cancelEl.addEventListener('click', () => this.complete(false));
        this.submitEl.addEventListener('click', () => this.complete(true));

        this.overlayEl.addEventListener('click', (e) => {
            if (e.target === this.overlayEl) this.complete(false);
        });

        document.addEventListener('keydown', (e) => {
            if (this.overlayEl.classList.contains('hidden')) return;
            if (e.key === 'Escape') {
                this.complete(false);
            } else if (e.key === 'Enter') {
                e.preventDefault();
                this.complete(true);
            }
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
