let _themeNames = null;

export async function loadThemeNames() {
    if (_themeNames) return _themeNames;
    const res = await fetch('/api/themes', { credentials: 'same-origin' });
    if (res.ok) {
        const data = await res.json();
        _themeNames = data.data || [];
    } else {
        _themeNames = ['default'];
    }
    return _themeNames;
}

export function getThemeNames() {
    return _themeNames || ['default'];
}

export function applyTheme(name) {
    const link = document.getElementById('theme-link');
    if (!link) return;
    if (name === 'default') {
        link.removeAttribute('href');
    } else {
        link.setAttribute('href', `/css/themes/${name}.css`);
    }
    localStorage.setItem('fh-theme', name);
}

// Apply saved theme on module load (fallback for first load)
const saved = localStorage.getItem('fh-theme');
if (saved && saved !== 'default') {
    applyTheme(saved);
}
