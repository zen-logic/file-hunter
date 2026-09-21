import { loadThemeNames, applyTheme } from '../themes.js';

const ThemeSelector = {
    async init() {
        this.el = document.getElementById('theme-select');
        if (!this.el) return;

        const themeNames = await loadThemeNames();

        // Populate options — default first, rest alphabetical
        this.el.innerHTML = '';
        const sorted = ['default', ...themeNames.filter(n => n !== 'default').sort()];
        for (const name of sorted) {
            const opt = document.createElement('option');
            opt.value = name;
            opt.textContent = name.replace(/-/g, ' ').replace(/\b\w/g, c => c.toUpperCase());
            this.el.appendChild(opt);
        }

        // Set initial value from localStorage
        const saved = localStorage.getItem('fh-theme') || 'default';
        if (themeNames.includes(saved)) {
            this.el.value = saved;
        }

        this.el.addEventListener('change', () => this.onChange());
    },

    onChange() {
        applyTheme(this.el.value);
    },
};

export default ThemeSelector;
