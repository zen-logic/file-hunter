const ScanConfirm = {
    overlayEl: null,
    textEl: null,
    optionsEl: null,
    onConfirm: null,
    locationNode: null,
    folderNode: null,

    init(onConfirm) {
        this.overlayEl = document.getElementById('scan-confirm-modal');
        this.textEl = document.getElementById('scan-confirm-text');
        this.optionsEl = document.getElementById('scan-confirm-options');
        this.onConfirm = onConfirm;

        document.getElementById('scan-confirm-cancel').addEventListener('click', () => this.close());
        document.getElementById('scan-confirm-submit').addEventListener('click', () => this._doConfirm());

        this.overlayEl.addEventListener('click', (e) => {
            if (e.target === this.overlayEl) this.close();
        });

        document.addEventListener('keydown', (e) => {
            if (e.key === 'Escape' && !this.overlayEl.classList.contains('hidden')) {
                this.close();
            }
        });
    },

    open(locationNode, folderNode, hasQuickScan) {
        this.locationNode = locationNode;
        this.folderNode = folderNode || null;

        if (this.folderNode) {
            const folderLabel = this.folderNode.label || this.folderNode.name;
            this.textEl.textContent = `${locationNode.label} / ${folderLabel}`;
        } else {
            this.textEl.textContent = locationNode.label;
        }

        this.optionsEl.style.display = hasQuickScan ? '' : 'none';

        // Reset to full scan
        const fullRadio = this.overlayEl.querySelector('input[value="full"]');
        if (fullRadio) fullRadio.checked = true;

        this.overlayEl.classList.remove('hidden');
    },

    close() {
        this.overlayEl.classList.add('hidden');
        this.locationNode = null;
        this.folderNode = null;
    },

    _doConfirm() {
        if (!this.locationNode || !this.onConfirm) return;
        const selected = this.overlayEl.querySelector('input[name="scan-type"]:checked');
        const type = selected ? selected.value : 'full';
        this.onConfirm(this.locationNode, this.folderNode, type);
        this.close();
    },
};

export default ScanConfirm;
