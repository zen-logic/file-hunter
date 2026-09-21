import API from '../api.js';

function formatSize(bytes) {
    if (bytes === null || bytes === undefined) return '';
    if (bytes < 1024) return bytes + ' B';
    if (bytes < 1048576) return (bytes / 1024).toFixed(1) + ' KB';
    if (bytes < 1073741824) return (bytes / 1048576).toFixed(1) + ' MB';
    if (bytes < 1099511627776) return (bytes / 1073741824).toFixed(1) + ' GB';
    return (bytes / 1099511627776).toFixed(1) + ' TB';
}

const IgnoreFileModal = {
    overlayEl: null,
    nameEl: null,
    sizeEl: null,
    countEl: null,
    locationLabelEl: null,
    onConfirm: null,
    _file: null,

    init(onConfirm) {
        this.overlayEl = document.getElementById('ignore-file-modal');
        this.nameEl = document.getElementById('ignore-file-name');
        this.sizeEl = document.getElementById('ignore-file-size');
        this.countEl = document.getElementById('ignore-file-count');
        this.locationLabelEl = document.getElementById('ignore-scope-location-label');
        this.onConfirm = onConfirm;

        document.getElementById('ignore-file-cancel').addEventListener('click', () => this.close());
        document.getElementById('ignore-file-submit').addEventListener('click', () => this._doConfirm());

        this.overlayEl.addEventListener('click', (e) => {
            if (e.target === this.overlayEl) this.close();
        });

        document.addEventListener('keydown', (e) => {
            if (this.overlayEl.classList.contains('hidden')) return;
            if (e.key === 'Escape') {
                this.close();
            } else if (e.key === 'Enter') {
                e.preventDefault();
                this._doConfirm();
            }
        });
    },

    async open(file) {
        this._file = file;
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
        this._file = null;
    },

    _doConfirm() {
        if (!this._file || !this.onConfirm) return;
        const scope = this.overlayEl.querySelector('input[name="ignore-scope"]:checked');
        const locationId = (scope && scope.value === 'location') ? this._file.locationId : null;
        this.onConfirm({
            filename: this._file.filename,
            file_size: this._file.file_size,
            location_id: locationId,
        });
        this.close();
    },
};

export default IgnoreFileModal;
