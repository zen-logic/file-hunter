import API from '../api.js';

const DeleteFileModal = {
    overlayEl: null,
    textEl: null,
    dupsRowEl: null,
    dupsCheckEl: null,
    dupsLabelEl: null,
    onConfirm: null,
    item: null,

    init(onConfirm) {
        this.overlayEl = document.getElementById('delete-file-modal');
        this.textEl = document.getElementById('delete-file-text');
        this.dupsRowEl = document.getElementById('delete-file-dups-row');
        this.dupsCheckEl = document.getElementById('delete-file-dups-check');
        this.dupsLabelEl = document.getElementById('delete-file-dups-label');
        this.onConfirm = onConfirm;

        document.getElementById('delete-file-cancel').addEventListener('click', () => this.close());
        document.getElementById('delete-file-submit').addEventListener('click', () => this._doConfirm());

        this.overlayEl.addEventListener('click', (e) => {
            if (e.target === this.overlayEl) this.close();
        });

        document.addEventListener('keydown', (e) => {
            if (e.key === 'Escape' && !this.overlayEl.classList.contains('hidden')) {
                this.close();
            }
        });
    },

    async open(item) {
        this.item = item;

        if (item.type === 'batch') {
            const fileCount = (item.batchFileIds || []).length;
            const folderCount = (item.batchFolderIds || []).length;
            const parts = [];
            if (fileCount > 0) parts.push(`${fileCount} file${fileCount !== 1 ? 's' : ''}`);
            if (folderCount > 0) parts.push(`${folderCount} folder${folderCount !== 1 ? 's' : ''}`);
            this.textEl.textContent = `Permanently delete ${parts.join(' and ')}? This will remove them from disk and the catalog.`;
            this.dupsRowEl.classList.add('hidden');
            this.dupsCheckEl.checked = false;
            this.overlayEl.classList.remove('hidden');
            return;
        }

        if (item.type === 'folder') {
            // Fetch folder stats for confirmation text
            const folderId = String(item.id).replace('fld-', '');
            const res = await API.get(`/api/folders/${folderId}/stats`);
            let sizeInfo = '';
            let offline = item.offline || false;
            if (res.ok) {
                sizeInfo = ` and all its contents (${res.data.fileCount.toLocaleString()} files, ${res.data.totalSizeFormatted})`;
                if (res.data.locationOnline === false) offline = true;
            }
            let text = `Permanently delete folder "${item.name || item.label}"${sizeInfo}? This will remove all files from disk and the catalog.`;
            if (offline) {
                text += ' (Location is offline \u2014 only the catalog entries will be removed. Files on disk will remain until the next scan.)';
            }
            this.textEl.textContent = text;
            // No duplicates checkbox for folders
            this.dupsRowEl.classList.add('hidden');
        } else {
            let text = `Permanently delete "${item.name}"? This will remove the file from disk and the catalog.`;
            if (item.offline) {
                text += ' (Location is offline \u2014 the file will be deleted when the location comes online.)';
            }
            this.textEl.textContent = text;

            // Show duplicates checkbox if file has duplicates
            if (item.dupCount > 0) {
                this.dupsLabelEl.textContent = `Also delete ${item.dupCount} duplicate${item.dupCount === 1 ? '' : 's'} across all locations`;
                this.dupsRowEl.classList.remove('hidden');
            } else {
                this.dupsRowEl.classList.add('hidden');
            }
        }

        // Always reset checkbox
        this.dupsCheckEl.checked = false;

        this.overlayEl.classList.remove('hidden');
    },

    close() {
        this.overlayEl.classList.add('hidden');
        this.item = null;
    },

    _doConfirm() {
        if (this.item && this.onConfirm) {
            this.item.deleteAllDuplicates = this.dupsCheckEl.checked;
            this.onConfirm(this.item);
        }
        this.close();
    },
};

export default DeleteFileModal;
