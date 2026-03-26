const NewFolderModal = {
    overlayEl: null,
    nameInput: null,
    errorEl: null,
    onConfirm: null,
    parentNode: null,

    init(onConfirm) {
        this.overlayEl = document.getElementById('new-folder-modal');
        this.nameInput = document.getElementById('new-folder-name');
        this.errorEl = document.getElementById('new-folder-error');
        this.onConfirm = onConfirm;

        document.getElementById('new-folder-cancel').addEventListener('click', () => this.close());
        document.getElementById('new-folder-submit').addEventListener('click', () => this._doConfirm());

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

    open(parentNode) {
        this.parentNode = parentNode;
        this.nameInput.value = '';
        if (this.errorEl) {
            this.errorEl.textContent = '';
            this.errorEl.classList.add('hidden');
        }
        this.overlayEl.classList.remove('hidden');
        this.nameInput.focus();
    },

    close() {
        this.overlayEl.classList.add('hidden');
        this.parentNode = null;
    },

    async _doConfirm() {
        const name = this.nameInput.value.trim();
        if (!name) return;
        if (!this.parentNode || !this.onConfirm) return;

        const parentNode = this.parentNode;
        this.close();

        const result = await this.onConfirm(parentNode, name);
        if (result && result.error) {
            this.open(parentNode);
            this.nameInput.value = name;
            if (this.errorEl) {
                this.errorEl.textContent = result.error;
                this.errorEl.classList.remove('hidden');
            }
        }
    },
};

export default NewFolderModal;
