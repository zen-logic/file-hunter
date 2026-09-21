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

        document.getElementById('prompt-modal-cancel').addEventListener('click', () => this.complete(null));
        this.submitEl.addEventListener('click', () => this.complete(this.inputEl.value.trim()));

        this.inputEl.addEventListener('keydown', (e) => {
            if (e.key === 'Enter') this.complete(this.inputEl.value.trim());
        });

        this.overlayEl.addEventListener('click', (e) => {
            if (e.target === this.overlayEl) this.complete(null);
        });

        document.addEventListener('keydown', (e) => {
            if (e.key === 'Escape' && !this.overlayEl.classList.contains('hidden')) {
                this.complete(null);
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
