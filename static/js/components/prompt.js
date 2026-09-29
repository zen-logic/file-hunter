import { wireModal } from './modal.js';
const PromptModal = {
    overlayEl: null,
    titleEl: null,
    textEl: null,
    inputEl: null,
    submitEl: null,
    pendingResolve: null,

    init() {
        this.overlayEl = document.getElementById('prompt-modal');
        this.titleEl = document.getElementById('prompt-modal-title');
        this.textEl = document.getElementById('prompt-modal-text');
        this.inputEl = document.getElementById('prompt-modal-input');
        this.submitEl = document.getElementById('prompt-modal-submit');

        wireModal(this.overlayEl, {
            close: () => this.complete(null),
            submit: () => this.complete(this.inputEl.value.trim()),
            enterFrom: [this.inputEl],
            cancelBtn: document.getElementById('prompt-modal-cancel'),
            submitBtn: this.submitEl,
        });
    },

    open({ title = 'Input', message = '', placeholder = '' } = {}) {
        this.titleEl.textContent = title;
        this.textEl.textContent = message;
        this.inputEl.value = '';
        this.inputEl.placeholder = placeholder;
        this.overlayEl.classList.remove('hidden');
        this.inputEl.focus();
        return new Promise((resolve) => { this.pendingResolve = resolve; });
    },

    complete(result) {
        this.overlayEl.classList.add('hidden');
        if (this.pendingResolve) {
            this.pendingResolve(result);
            this.pendingResolve = null;
        }
    },
};

export default PromptModal;
