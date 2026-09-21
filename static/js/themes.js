let themes = null;

export async function loadThemes() {
    if (themes) return themes;
    const res = await fetch('/api/themes', { credentials: 'same-origin' });
    if (res.ok) {
        const data = await res.json();
        themes = data.data || [];
    } else {
        themes = [{ name: 'default', builtIn: true }];
    }
    return themes;
}

export async function loadThemeNames() {
    const themes = await loadThemes();
    return themes.map(t => t.name);
}

export function isBuiltIn(name) {
    if (!themes) return true;
    const t = themes.find(t => t.name === name);
    return t ? t.builtIn : true;
}

export function clearThemeCache() {
    themes = null;
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
