import { loadThemeNames, applyTheme } from '../themes.js';

const ThemeSelector = {
    async init() {
        this._el = document.getElementById('theme-select');
        if (!this._el) return;

        const themeNames = await loadThemeNames();

        // Populate options — default first, rest alphabetical
        this._el.innerHTML = '';
        const sorted = ['default', ...themeNames.filter(n => n !== 'default').sort()];
        for (const name of sorted) {
            const opt = document.createElement('option');
            opt.value = name;
            opt.textContent = name.replace(/-/g, ' ').replace(/\b\w/g, c => c.toUpperCase());
            this._el.appendChild(opt);
        }

        // Set initial value from localStorage
        const saved = localStorage.getItem('fh-theme') || 'default';
        if (themeNames.includes(saved)) {
            this._el.value = saved;
        }

        this._el.addEventListener('change', () => this._onChange());
    },

    _onChange() {
        applyTheme(this._el.value);
    },
};

export default ThemeSelector;
