const DeleteLocationModal = {
    overlayEl: null,
    textEl: null,
    onConfirm: null,
    node: null,

    init(onConfirm) {
        this.overlayEl = document.getElementById('delete-location-modal');
        this.textEl = document.getElementById('delete-location-text');
        this.onConfirm = onConfirm;

        document.getElementById('delete-location-cancel').addEventListener('click', () => this.close());
        document.getElementById('delete-location-submit').addEventListener('click', () => this.confirm());

        this.overlayEl.addEventListener('click', (e) => {
            if (e.target === this.overlayEl) this.close();
        });

        document.addEventListener('keydown', (e) => {
            if (this.overlayEl.classList.contains('hidden')) return;
            if (e.key === 'Escape') {
                this.close();
            } else if (e.key === 'Enter') {
                e.preventDefault();
                this.confirm();
            }
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
