import FSBrowser from './fsbrowser.js';

const AddLocationModal = {
    overlayEl: null,
    nameInput: null,
    pathInput: null,
    errorEl: null,
    onAdd: null,

    init(onAdd) {
        this.overlayEl = document.getElementById('add-location-modal');
        this.nameInput = document.getElementById('add-loc-name');
        this.pathInput = document.getElementById('add-loc-path');
        this.errorEl = document.getElementById('add-loc-error');
        this.onAdd = onAdd;

        document.getElementById('btn-add-location').addEventListener('click', () => this.open());
        document.getElementById('add-loc-cancel').addEventListener('click', () => this.close());
        document.getElementById('add-loc-submit').addEventListener('click', () => this.doSubmit());
        FSBrowser.init();
        document.getElementById('add-loc-browse').addEventListener('click', () => {
            FSBrowser.open(this.pathInput.value.trim() || null, (path) => {
                this.pathInput.value = path;
            });
        });

        this.overlayEl.addEventListener('click', (e) => {
            if (e.target === this.overlayEl) this.close();
        });

        document.addEventListener('keydown', (e) => {
            if (e.key === 'Escape' && !this.overlayEl.classList.contains('hidden')) {
                this.close();
            }
        });

        this.overlayEl.querySelectorAll('input').forEach(el => {
            el.addEventListener('keydown', (e) => {
                if (e.key === 'Enter') this.doSubmit();
            });
        });
    },

    open() {
        this.nameInput.value = '';
        this.pathInput.value = '';
        if (this.errorEl) {
            this.errorEl.textContent = '';
            this.errorEl.classList.add('hidden');
        }
        this.overlayEl.classList.remove('hidden');
        this.nameInput.focus();
    },

    close() {
        this.overlayEl.classList.add('hidden');
    },

    async doSubmit() {
        const name = this.nameInput.value.trim();
        const path = this.pathInput.value.trim();
        if (!name || !path) return;

        if (this.errorEl) {
            this.errorEl.textContent = '';
            this.errorEl.classList.add('hidden');
        }

        if (this.onAdd) {
            const result = await this.onAdd({ name, path });
            if (result && result.error) {
                if (this.errorEl) {
                    this.errorEl.textContent = result.error;
                    this.errorEl.classList.remove('hidden');
                }
                return;
            }
        }
        this.close();
    },
};

export default AddLocationModal;
