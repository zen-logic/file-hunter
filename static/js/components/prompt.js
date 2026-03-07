const PromptModal = {
    overlayEl: null,
    titleEl: null,
    textEl: null,
    inputEl: null,
    submitEl: null,
    _resolve: null,

    init() {
        this.overlayEl = document.getElementById('prompt-modal');
        this.titleEl = document.getElementById('prompt-modal-title');
        this.textEl = document.getElementById('prompt-modal-text');
        this.inputEl = document.getElementById('prompt-modal-input');
        this.submitEl = document.getElementById('prompt-modal-submit');

        document.getElementById('prompt-modal-cancel').addEventListener('click', () => this._finish(null));
        this.submitEl.addEventListener('click', () => this._finish(this.inputEl.value.trim()));

        this.inputEl.addEventListener('keydown', (e) => {
            if (e.key === 'Enter') this._finish(this.inputEl.value.trim());
        });

        this.overlayEl.addEventListener('click', (e) => {
            if (e.target === this.overlayEl) this._finish(null);
        });

        document.addEventListener('keydown', (e) => {
            if (e.key === 'Escape' && !this.overlayEl.classList.contains('hidden')) {
                this._finish(null);
            }
        });
    },

    open({ title = 'Input', message = '', placeholder = '' } = {}) {
        this.titleEl.textContent = title;
        this.textEl.textContent = message;
        this.inputEl.value = '';
        this.inputEl.placeholder = placeholder;
        this.overlayEl.classList.remove('hidden');
        this.inputEl.focus();
        return new Promise((resolve) => { this._resolve = resolve; });
    },

    _finish(result) {
        this.overlayEl.classList.add('hidden');
        if (this._resolve) {
            this._resolve(result);
            this._resolve = null;
        }
    },
};

export default PromptModal;
