let _themes = null;

export async function loadThemes() {
    if (_themes) return _themes;
    const res = await fetch('/api/themes', { credentials: 'same-origin' });
    if (res.ok) {
        const data = await res.json();
        _themes = data.data || [];
    } else {
        _themes = [{ name: 'default', builtIn: true }];
    }
    return _themes;
}

export async function loadThemeNames() {
    const themes = await loadThemes();
    return themes.map(t => t.name);
}

export function isBuiltIn(name) {
    if (!_themes) return true;
    const t = _themes.find(t => t.name === name);
    return t ? t.builtIn : true;
}

export function clearThemeCache() {
    _themes = null;
}

export function applyTheme(name, bustCache) {
    const link = document.getElementById('theme-link');
    if (!link) return;
    if (name === 'default') {
        link.removeAttribute('href');
    } else {
        let href = `/css/themes/${name}.css`;
        if (bustCache) href += `?v=${Date.now()}`;
        link.setAttribute('href', href);
    }
    localStorage.setItem('fh-theme', name);
}

// Apply saved theme on module load
const saved = localStorage.getItem('fh-theme');
if (saved && saved !== 'default') {
    applyTheme(saved);
}
