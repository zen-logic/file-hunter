const RenameFileModal = {
    overlayEl: null,
    nameInput: null,
    errorEl: null,
    onConfirm: null,
    file: null,

    init(onConfirm) {
        this.overlayEl = document.getElementById('rename-file-modal');
        this.nameInput = document.getElementById('rename-file-name');
        this.errorEl = document.getElementById('rename-file-error');
        this.onConfirm = onConfirm;

        document.getElementById('rename-file-cancel').addEventListener('click', () => this.close());
        document.getElementById('rename-file-submit').addEventListener('click', () => this.confirm());

        this.overlayEl.addEventListener('click', (e) => {
            if (e.target === this.overlayEl) this.close();
        });

        document.addEventListener('keydown', (e) => {
            if (e.key === 'Escape' && !this.overlayEl.classList.contains('hidden')) {
                this.close();
            }
        });

        this.nameInput.addEventListener('keydown', (e) => {
            if (e.key === 'Enter') this.confirm();
        });
    },

    open(file) {
        this.file = file;
        const name = file.name || '';
        this.nameInput.value = name;
        if (this.errorEl) {
            this.errorEl.textContent = '';
            this.errorEl.classList.add('hidden');
        }
        this.overlayEl.classList.remove('hidden');
        this.nameInput.focus();
        // Select filename without extension
        const dotIdx = name.lastIndexOf('.');
        if (dotIdx > 0) {
            this.nameInput.setSelectionRange(0, dotIdx);
        } else {
            this.nameInput.select();
        }
    },

    close() {
        this.overlayEl.classList.add('hidden');
        this.file = null;
    },

    async confirm() {
        const newName = this.nameInput.value.trim();
        if (!newName) return;
        if (!this.file || !this.onConfirm) return;

        if (this.errorEl) {
            this.errorEl.textContent = '';
            this.errorEl.classList.add('hidden');
        }

        const result = await this.onConfirm(this.file, newName);
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

export default RenameFileModal;
