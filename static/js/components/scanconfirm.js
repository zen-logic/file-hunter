const ScanConfirm = {
    overlayEl: null,
    textEl: null,
    onConfirm: null,
    locationNode: null,
    folderNode: null,

    init(onConfirm) {
        this.overlayEl = document.getElementById('scan-confirm-modal');
        this.textEl = document.getElementById('scan-confirm-text');
        this.onConfirm = onConfirm;

        document.getElementById('scan-confirm-cancel').addEventListener('click', () => this.close());
        document.getElementById('scan-confirm-submit').addEventListener('click', () => this._doConfirm());

        // Hide fast scan elements if they exist in HTML
        const fastBtn = document.getElementById('scan-confirm-fast');
        if (fastBtn) fastBtn.style.display = 'none';
        const warningEl = document.getElementById('scan-confirm-warning');
        if (warningEl) warningEl.style.display = 'none';

        this.overlayEl.addEventListener('click', (e) => {
            if (e.target === this.overlayEl) this.close();
        });

        document.addEventListener('keydown', (e) => {
            if (e.key === 'Escape' && !this.overlayEl.classList.contains('hidden')) {
                this.close();
            }
        });
    },

    open(locationNode, folderNode) {
        this.locationNode = locationNode;
        this.folderNode = folderNode || null;

        if (this.folderNode) {
            const folderLabel = this.folderNode.label || this.folderNode.name;
            this.textEl.textContent = `Scan "${locationNode.label} / ${folderLabel}"?`;
        } else {
            this.textEl.textContent = `Scan "${locationNode.label}"?`;
        }

        this.overlayEl.classList.remove('hidden');
    },

    close() {
        this.overlayEl.classList.add('hidden');
        this.locationNode = null;
        this.folderNode = null;
    },

    _doConfirm() {
        if (this.locationNode && this.onConfirm) {
            this.onConfirm(this.locationNode, this.folderNode);
        }
        this.close();
    },
};

export default ScanConfirm;
