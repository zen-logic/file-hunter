const RenameLocationModal = {
    overlayEl: null,
    nameInput: null,
    errorEl: null,
    onConfirm: null,
    node: null,

    init(onConfirm) {
        this.overlayEl = document.getElementById('rename-location-modal');
        this.nameInput = document.getElementById('rename-loc-name');
        this.errorEl = document.getElementById('rename-loc-error');
        this.onConfirm = onConfirm;

        document.getElementById('rename-loc-cancel').addEventListener('click', () => this.close());
        document.getElementById('rename-loc-submit').addEventListener('click', () => this.confirm());

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

    open(node) {
        this.node = node;
        this.nameInput.value = node.label;
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
        this.node = null;
    },

    async confirm() {
        const newName = this.nameInput.value.trim();
        if (!newName) return;
        if (!this.node || !this.onConfirm) return;

        if (this.errorEl) {
            this.errorEl.textContent = '';
            this.errorEl.classList.add('hidden');
        }

        const result = await this.onConfirm(this.node, newName);
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

export default RenameLocationModal;
