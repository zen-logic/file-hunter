const ScanConfirm = {
    overlayEl: null,
    textEl: null,
    warningEl: null,
    fastBtn: null,
    onConfirm: null,
    onFastScan: null,
    locationNode: null,
    folderNode: null,

    init(onConfirm, onFastScan) {
        this.overlayEl = document.getElementById('scan-confirm-modal');
        this.textEl = document.getElementById('scan-confirm-text');
        this.warningEl = document.getElementById('scan-confirm-warning');
        this.fastBtn = document.getElementById('scan-confirm-fast');
        this.onConfirm = onConfirm;
        this.onFastScan = onFastScan;

        document.getElementById('scan-confirm-cancel').addEventListener('click', () => this.close());
        document.getElementById('scan-confirm-submit').addEventListener('click', () => this._doConfirm());
        this.fastBtn.addEventListener('click', () => this._doFastScan());

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

        // Show fast scan button only for local locations (not folder scans)
        const isLocal = locationNode.agent === 'local';
        const isLocationScan = !this.folderNode;
        if (isLocal && isLocationScan) {
            this.fastBtn.style.display = '';
            // Show warning if previously scanned
            if (locationNode.lastScanned) {
                this.warningEl.textContent = 'Fast Scan will replace all existing catalog data for this location.';
                this.warningEl.style.display = '';
            } else {
                this.warningEl.style.display = 'none';
            }
        } else {
            this.fastBtn.style.display = 'none';
            this.warningEl.style.display = 'none';
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

    _doFastScan() {
        if (this.locationNode && this.onFastScan) {
            this.onFastScan(this.locationNode);
        }
        this.close();
    },
};

export default ScanConfirm;
