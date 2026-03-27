import API from '../api.js';
import { applyTheme, isBuiltIn } from '../themes.js';
import ConfirmModal from './confirm.js';
import PromptModal from './prompt.js';
import Toast from './toast.js';

let manifest = null;
let currentValues = {};
let originalTheme = null;
let editingName = null;
let canSave = false;

const ThemeEditor = {
    overlayEl: null,
    titleEl: null,
    bodyEl: null,
    actionsEl: null,
    onSave: null,
    _initialized: false,

    init(onSave) {
        this.onSave = onSave;
        if (this._initialized) return;
        this._initialized = true;
        this.overlayEl = document.getElementById('theme-editor-modal');
        this.titleEl = document.getElementById('theme-editor-title');
        this.bodyEl = document.getElementById('theme-editor-body');
        this.actionsEl = document.getElementById('theme-editor-actions');

        this.overlayEl.addEventListener('click', (e) => {
            if (e.target === this.overlayEl) this._cancel();
        });

        document.addEventListener('keydown', (e) => {
            if (e.key !== 'Escape') return;
            if (this.overlayEl.classList.contains('hidden')) return;
            // Don't close if a sub-modal (prompt/confirm) is open
            const prompt = document.getElementById('prompt-modal');
            const confirm = document.getElementById('confirm-modal');
            if (!prompt.classList.contains('hidden')) return;
            if (!confirm.classList.contains('hidden')) return;
            this._cancel();
        });
    },

    async open(themeName) {
        originalTheme = localStorage.getItem('fh-theme') || 'default';
        editingName = themeName;
        canSave = !!themeName && themeName !== 'default' && !isBuiltIn(themeName);

        if (!manifest) {
            const res = await fetch('/js/theme-manifest.json');
            manifest = await res.json();
        }

        currentValues = {};
        if (themeName && themeName !== 'default') {
            await this._loadThemeValues(themeName);
        } else {
            this._loadComputedValues();
        }

        this._render(themeName || '');
        this.overlayEl.classList.remove('hidden');
    },

    async _loadThemeValues(themeName) {
        const res = await fetch(`/css/themes/${themeName}.css`);
        if (!res.ok) {
            Toast.error('Could not load theme.');
            this._loadComputedValues();
            return;
        }
        const text = await res.text();
        for (const line of text.split('\n')) {
            const m = line.match(/^\s*(--[\w-]+)\s*:\s*(.+?)\s*;?\s*$/);
            if (m) currentValues[m[1]] = m[2].replace(/;$/, '').trim();
        }
    },

    _loadComputedValues() {
        const style = getComputedStyle(document.documentElement);
        for (const group of manifest.groups) {
            for (const v of group.vars) {
                const val = style.getPropertyValue(v.var).trim();
                if (val) currentValues[v.var] = val;
            }
        }
    },

    _render(themeName) {
        this.bodyEl.innerHTML = '';
        const displayName = themeName
            ? themeName.replace(/-/g, ' ').replace(/\b\w/g, c => c.toUpperCase())
            : 'New Theme';
        this.titleEl.textContent = `Theme Editor \u2014 ${displayName}`;

        for (const group of manifest.groups) {
            const section = document.createElement('fieldset');
            section.className = 'theme-editor-group';
            const legend = document.createElement('legend');
            legend.textContent = group.label;
            legend.addEventListener('click', () => section.classList.toggle('collapsed'));
            section.appendChild(legend);

            for (const v of group.vars) {
                const row = document.createElement('div');
                row.className = 'theme-editor-row';

                const label = document.createElement('label');
                label.className = 'theme-editor-label';
                label.textContent = v.label;
                row.appendChild(label);

                const inputs = document.createElement('div');
                inputs.className = 'theme-editor-row-inputs';
                const val = currentValues[v.var] || '';

                if (v.type === 'color') {
                    const hex = this._toHex(val);
                    const picker = document.createElement('input');
                    picker.type = 'color';
                    picker.className = 'theme-editor-color';
                    picker.value = hex || '#000000';
                    picker.dataset.var = v.var;

                    const text = document.createElement('input');
                    text.type = 'text';
                    text.className = 'theme-editor-text';
                    text.value = val;
                    text.dataset.var = v.var;

                    picker.addEventListener('input', () => {
                        text.value = picker.value;
                        this._preview(v.var, picker.value);
                    });
                    text.addEventListener('change', () => {
                        const h = this._toHex(text.value);
                        if (h) picker.value = h;
                        this._preview(v.var, text.value);
                    });

                    inputs.appendChild(picker);
                    inputs.appendChild(text);
                } else if (v.type === 'slider') {
                    const slider = document.createElement('input');
                    slider.type = 'range';
                    slider.className = 'theme-editor-slider';
                    slider.min = v.min ?? 0;
                    slider.max = v.max ?? 1;
                    slider.step = v.step ?? 0.05;
                    slider.value = parseFloat(val) || 0;
                    slider.dataset.var = v.var;

                    const display = document.createElement('span');
                    display.className = 'theme-editor-slider-val';
                    display.textContent = slider.value;

                    slider.addEventListener('input', () => {
                        display.textContent = slider.value;
                        this._preview(v.var, slider.value);
                    });

                    inputs.appendChild(slider);
                    inputs.appendChild(display);
                } else if (v.type === 'select') {
                    const select = document.createElement('select');
                    select.className = 'theme-editor-select';
                    select.dataset.var = v.var;
                    for (const opt of v.options) {
                        const o = document.createElement('option');
                        o.value = opt;
                        o.textContent = opt;
                        if (opt === val) o.selected = true;
                        select.appendChild(o);
                    }
                    select.addEventListener('change', () => {
                        this._preview(v.var, select.value);
                    });
                    inputs.appendChild(select);
                } else {
                    // text input for fonts, glows, complex values
                    const text = document.createElement('input');
                    text.type = 'text';
                    text.className = 'theme-editor-text';
                    text.value = val;
                    text.dataset.var = v.var;
                    text.addEventListener('change', () => {
                        this._preview(v.var, text.value);
                    });
                    inputs.appendChild(text);
                }

                row.appendChild(inputs);
                section.appendChild(row);
            }

            this.bodyEl.appendChild(section);
        }

        const isNew = !editingName;
        this.actionsEl.innerHTML = `
            <button class="btn" id="theme-editor-cancel">Cancel</button>
            <button class="btn btn-primary" id="theme-editor-save" ${canSave || isNew ? '' : 'disabled'}>Save</button>
            <button class="btn btn-primary" id="theme-editor-save-as">Save As</button>
        `;

        document.getElementById('theme-editor-cancel').addEventListener('click', () => this._cancel());
        document.getElementById('theme-editor-save').addEventListener('click', () => {
            if (canSave) this._saveExisting();
            else this._saveAs();
        });
        document.getElementById('theme-editor-save-as').addEventListener('click', () => this._saveAs());
    },

    _preview(varName, value) {
        document.documentElement.style.setProperty(varName, value);
    },

    _toHex(val) {
        if (!val) return null;
        val = val.trim();
        if (/^#[0-9a-fA-F]{6}$/.test(val)) return val;
        if (/^#[0-9a-fA-F]{3}$/.test(val)) {
            return '#' + val[1] + val[1] + val[2] + val[2] + val[3] + val[3];
        }
        const m = val.match(/^rgb\(\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)\s*\)$/);
        if (m) {
            return '#' + [m[1], m[2], m[3]].map(n => parseInt(n).toString(16).padStart(2, '0')).join('');
        }
        return null;
    },

    _collectValues() {
        const values = {};
        for (const el of this.bodyEl.querySelectorAll('[data-var]')) {
            if (el.type === 'color') continue;
            values[el.dataset.var] = el.value;
        }
        return values;
    },

    _buildCss() {
        const values = this._collectValues();
        let css = ':root {\n';
        for (const group of manifest.groups) {
            css += `    /* ${group.label} */\n`;
            for (const v of group.vars) {
                const val = values[v.var];
                if (val !== undefined && val !== '') {
                    css += `    ${v.var}: ${val};\n`;
                }
            }
            css += '\n';
        }
        css += '}\n';
        return css;
    },

    async _saveExisting() {
        if (!canSave || !editingName) return;
        const css = this._buildCss();
        const res = await API.post('/api/themes', { name: editingName, css, overwrite: true });
        if (!res.ok) {
            Toast.error(res.error || 'Failed to save theme.');
            return;
        }
        this._close();
        if (this.onSave) await this.onSave(editingName);
        applyTheme(editingName, true);
    },

    async _saveAs() {
        const input = await PromptModal.open({
            title: 'Save Theme As',
            message: 'Enter a name for the new theme.',
            placeholder: 'theme-name',
        });
        if (!input) return;
        const name = input.toLowerCase().replace(/\s+/g, '-').replace(/[^a-z0-9-]/g, '');
        if (!name) return;
        const css = this._buildCss();
        const res = await API.post('/api/themes', { name, css });
        if (res.ok) {
            this._close();
            if (this.onSave) await this.onSave(name);
            applyTheme(name, true);
        } else if (res.error && res.error.includes('already exists')) {
            const overwrite = await ConfirmModal.open({
                title: 'Overwrite Theme',
                message: `Theme "${name}" already exists. Overwrite?`,
                confirmLabel: 'Overwrite',
            });
            if (overwrite) {
                const res2 = await API.post('/api/themes', { name, css, overwrite: true });
                if (res2.ok) {
                    this._close();
                    if (this.onSave) await this.onSave(name);
                    applyTheme(name, true);
                } else {
                    Toast.error(res2.error || 'Failed to save theme.');
                }
            }
        } else {
            Toast.error(res.error || 'Failed to save theme.');
        }
    },

    _cancel() {
        this._clearInlineOverrides();
        applyTheme(originalTheme);
        this._close();
    },

    _clearInlineOverrides() {
        if (!manifest) return;
        for (const group of manifest.groups) {
            for (const v of group.vars) {
                document.documentElement.style.removeProperty(v.var);
            }
        }
    },

    _close() {
        this._clearInlineOverrides();
        this.overlayEl.classList.add('hidden');
    },
};

export default ThemeEditor;
