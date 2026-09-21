const Toast = {
    container: null,

    ensureContainer() {
        if (this.container) return;
        this.container = document.createElement('div');
        this.container.id = 'toast-container';
        document.body.appendChild(this.container);
    },

    success(msg) { this.show(msg, 'success'); },
    error(msg)   { this.show(msg, 'error'); },
    info(msg)    { this.show(msg, 'info'); },

    show(message, level) {
        this.ensureContainer();

        const toast = document.createElement('div');
        toast.className = `toast toast-${level}`;
        toast.innerHTML = `
            <span class="toast-msg">${message}</span>
            <button class="toast-close">&times;</button>
        `;

        toast.querySelector('.toast-close').addEventListener('click', () => this.dismiss(toast));

        this.container.appendChild(toast);

        // Trigger slide-in on next frame
        requestAnimationFrame(() => toast.classList.add('toast-visible'));

        // Auto-dismiss after 4 seconds
        setTimeout(() => this.dismiss(toast), 4000);
    },

    dismiss(toast) {
        if (toast.classList.contains('toast-dismissing')) return;
        toast.classList.add('toast-dismissing');
        toast.addEventListener('transitionend', () => toast.remove(), { once: true });
        // Fallback removal if transition doesn't fire
        setTimeout(() => toast.remove(), 400);
    },
};

export default Toast;
