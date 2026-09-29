import FSBrowser from './fsbrowser.js';
import { wireModal, setDialogError } from './modal.js';

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
        FSBrowser.init();
        document.getElementById('add-loc-browse').addEventListener('click', () => {
            FSBrowser.open(this.pathInput.value.trim() || null, (path) => {
                this.pathInput.value = path;
            });
        });

        wireModal(this.overlayEl, {
            close: () => this.close(),
            submit: () => this.doSubmit(),
            enterFrom: this.overlayEl.querySelectorAll('input'),
            cancelBtn: document.getElementById('add-loc-cancel'),
            submitBtn: document.getElementById('add-loc-submit'),
        });
    },

    open() {
        this.nameInput.value = '';
        this.pathInput.value = '';
        setDialogError(this.errorEl, '');
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

        setDialogError(this.errorEl, '');

        if (this.onAdd) {
            const result = await this.onAdd({ name, path });
            if (result && result.error) {
                setDialogError(this.errorEl, result.error);
                return;
            }
        }
        this.close();
    },
};

export default AddLocationModal;
