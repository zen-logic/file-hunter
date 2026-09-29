import { wireModal } from './modal.js';
const ScanConfirm = {
    overlayEl: null,
    textEl: null,
    optionsEl: null,
    similarityEl: null,
    onConfirm: null,
    locationNode: null,
    folderNode: null,

    init(onConfirm) {
        this.overlayEl = document.getElementById('scan-confirm-modal');
        this.textEl = document.getElementById('scan-confirm-text');
        this.optionsEl = document.getElementById('scan-confirm-options');
        this.similarityEl = document.getElementById('scan-confirm-similarity');
        this.onConfirm = onConfirm;

        wireModal(this.overlayEl, {
            close: () => this.close(),
            submit: () => this.confirm(),
            cancelBtn: document.getElementById('scan-confirm-cancel'),
            submitBtn: document.getElementById('scan-confirm-submit'),
        });
    },

    open(locationNode, folderNode, hasQuickScan, hasSimilarity) {
        this.locationNode = locationNode;
        this.folderNode = folderNode || null;

        if (this.folderNode) {
            const folderLabel = this.folderNode.label || this.folderNode.name;
            this.textEl.textContent = `${locationNode.label} / ${folderLabel}`;
        } else {
            this.textEl.textContent = locationNode.label;
        }

        this.optionsEl.style.display = hasQuickScan ? '' : 'none';
        this.similarityEl.style.display = hasSimilarity ? '' : 'none';

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

    confirm() {
        if (!this.locationNode || !this.onConfirm) return;
        const selected = this.overlayEl.querySelector('input[name="scan-type"]:checked');
        const type = selected ? selected.value : 'full';
        this.onConfirm(this.locationNode, this.folderNode, type);
        this.close();
    },
};

export default ScanConfirm;
