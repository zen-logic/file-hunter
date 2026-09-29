import API from '../api.js';
import { formatSize } from '../format.js';
import { wireModal } from './modal.js';


const IgnoreFileModal = {
    overlayEl: null,
    nameEl: null,
    sizeEl: null,
    countEl: null,
    locationLabelEl: null,
    onConfirm: null,
    file: null,

    init(onConfirm) {
        this.overlayEl = document.getElementById('ignore-file-modal');
        this.nameEl = document.getElementById('ignore-file-name');
        this.sizeEl = document.getElementById('ignore-file-size');
        this.countEl = document.getElementById('ignore-file-count');
        this.locationLabelEl = document.getElementById('ignore-scope-location-label');
        this.onConfirm = onConfirm;

        wireModal(this.overlayEl, {
            close: () => this.close(),
            submit: () => this.confirm(),
            enterFrom: 'dialog',
            cancelBtn: document.getElementById('ignore-file-cancel'),
            submitBtn: document.getElementById('ignore-file-submit'),
        });
    },

    async open(file) {
        this.file = file;
        this.nameEl.textContent = file.filename;
        this.sizeEl.textContent = formatSize(file.file_size);
        this.countEl.textContent = '';

        // Set location label
        if (file.locationName) {
            this.locationLabelEl.textContent = `${file.locationName} only`;
        } else {
            this.locationLabelEl.textContent = 'This location only';
        }

        // Reset to global scope
        const globalRadio = this.overlayEl.querySelector('input[value="global"]');
        if (globalRadio) globalRadio.checked = true;

        this.overlayEl.classList.remove('hidden');

        // Fetch match count
        const params = new URLSearchParams({
            filename: file.filename,
            file_size: String(file.file_size),
        });
        const res = await API.get(`/api/ignore/count?${params}`);
        if (res.ok) {
            const n = res.data.count;
            this.countEl.textContent = `${n} file${n !== 1 ? 's' : ''} in the catalog match this filename and size.`;
        }
    },

    close() {
        this.overlayEl.classList.add('hidden');
        this.file = null;
    },

    confirm() {
        if (!this.file || !this.onConfirm) return;
        const scope = this.overlayEl.querySelector('input[name="ignore-scope"]:checked');
        const locationId = (scope && scope.value === 'location') ? this.file.locationId : null;
        this.onConfirm({
            filename: this.file.filename,
            file_size: this.file.file_size,
            location_id: locationId,
        });
        this.close();
    },
};

export default IgnoreFileModal;
