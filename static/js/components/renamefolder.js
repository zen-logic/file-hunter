const RenameFolderModal = {
    overlayEl: null,
    nameInput: null,
    errorEl: null,
    onConfirm: null,
    folder: null,

    init(onConfirm) {
        this.overlayEl = document.getElementById('rename-folder-modal');
        this.nameInput = document.getElementById('rename-folder-name');
        this.errorEl = document.getElementById('rename-folder-error');
        this.onConfirm = onConfirm;

        document.getElementById('rename-folder-cancel').addEventListener('click', () => this.close());
        document.getElementById('rename-folder-submit').addEventListener('click', () => this._doConfirm());

        this.overlayEl.addEventListener('click', (e) => {
            if (e.target === this.overlayEl) this.close();
        });

        document.addEventListener('keydown', (e) => {
            if (e.key === 'Escape' && !this.overlayEl.classList.contains('hidden')) {
                this.close();
            }
        });

        this.nameInput.addEventListener('keydown', (e) => {
            if (e.key === 'Enter') this._doConfirm();
        });
    },

    open(folder) {
        this.folder = folder;
        this.nameInput.value = folder.label || folder.name || '';
        if (this.errorEl) {
            this.errorEl.textContent = '';
            this.errorEl.classList.add('hidden');
        }
        this.overlayEl.classList.remove('hidden');
        this.nameInput.focus();
        this.nameInput.select();
    },

    close() {
        this.overlayEl.classList.add('hidden');
        this.folder = null;
    },

    async _doConfirm() {
        const newName = this.nameInput.value.trim();
        if (!newName) return;
        if (!this.folder || !this.onConfirm) return;

        if (this.errorEl) {
            this.errorEl.textContent = '';
            this.errorEl.classList.add('hidden');
        }

        const result = await this.onConfirm(this.folder, newName);
        if (result && result.error) {
            if (this.errorEl) {
                this.errorEl.textContent = result.error;
                this.errorEl.classList.remove('hidden');
            }
            return;
        }
        this.close();
    },
};

export default RenameFolderModal;
