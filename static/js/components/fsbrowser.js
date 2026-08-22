import API from '../api.js';
import icons from '../icons.js';

const FSBrowser = {
    overlay: null,
    treeContainer: null,
    pathDisplay: null,
    cancelBtn: null,
    selectBtn: null,
    _selectedPath: null,
    _loadedChildren: new Map(),
    _expandedPaths: new Set(),
    _onSelect: null,
    _browseUrl: '/api/browse',

    init() {
        this.overlay = document.getElementById('fs-browser-modal');
        this.treeContainer = document.getElementById('fs-browser-tree');
        this.pathDisplay = document.getElementById('fs-browser-path');
        this.cancelBtn = document.getElementById('fs-browser-cancel');
        this.selectBtn = document.getElementById('fs-browser-select');

        this.cancelBtn.addEventListener('click', () => this.close());
        this.overlay.addEventListener('click', (e) => {
            if (e.target === this.overlay) this.close();
        });
        document.addEventListener('keydown', (e) => {
            if (e.key === 'Escape' && !this.overlay.classList.contains('hidden')) {
                this.close();
            }
        });
        this.selectBtn.addEventListener('click', () => this._doSelect());
    },

    async open(initialPath, onSelect, browseUrl) {
        this._onSelect = onSelect;
        this._browseUrl = browseUrl || '/api/browse';
        this._selectedPath = null;
        this._loadedChildren = new Map();
        this._expandedPaths = new Set();
        this._updatePathDisplay();

        this.overlay.classList.remove('hidden');

        // Show loading state while fetching from agent
        this.treeContainer.innerHTML = '<div class="fs-loading">Loading...</div>';

        // Load root entries
        await this._loadEntries(null);
        this._renderTree();

        // If initialPath provided, expand to it
        if (initialPath) {
            await this._expandToPath(initialPath);
        }
    },

    close() {
        this.overlay.classList.add('hidden');
    },

    _doSelect() {
        if (this._selectedPath && this._onSelect) {
            this._onSelect(this._selectedPath);
        }
        this.close();
    },

    async _loadEntries(path) {
        const key = path || '__root__';
        if (this._loadedChildren.has(key)) return;

        const url = path ? `${this._browseUrl}?path=${encodeURIComponent(path)}` : this._browseUrl;
        const res = await API.get(url);
        if (res.ok) {
            this._loadedChildren.set(key, res.data.entries);
        } else {
            this._loadedChildren.set(key, []);
        }
    },

    _renderTree() {
        this.treeContainer.innerHTML = '';
        const rootEntries = this._loadedChildren.get('__root__') || [];
        rootEntries.forEach(entry => this._renderNode(entry, 0));
    },

    _renderNode(entry, depth) {
        const div = document.createElement('div');
        div.className = 'ct-node';
        if (this._selectedPath === entry.path) div.classList.add('ct-selected');

        for (let i = 0; i < depth; i++) {
            const indent = document.createElement('span');
            indent.className = 'ct-indent';
            div.appendChild(indent);
        }

        // Expand/collapse toggle
        const toggle = document.createElement('span');
        toggle.className = 'ct-icon';
        if (entry.hasChildren) {
            toggle.textContent = this._expandedPaths.has(entry.path) ? '\u25BE' : '\u25B8';
        }
        div.appendChild(toggle);

        // Folder/location icon
        const icon = document.createElement('span');
        icon.className = 'ct-icon';
        icon.innerHTML = depth === 0 ? icons.location : icons.folder;
        div.appendChild(icon);

        // Label
        const label = document.createElement('span');
        label.className = 'ct-label';
        label.textContent = entry.name;
        div.appendChild(label);

        // Click to select + toggle expand
        div.addEventListener('click', async (e) => {
            e.stopPropagation();
            this._selectedPath = entry.path;
            this._updatePathDisplay();

            if (entry.hasChildren) {
                if (this._expandedPaths.has(entry.path)) {
                    this._expandedPaths.delete(entry.path);
                    this._renderTree();
                } else {
                    this._expandedPaths.add(entry.path);
                    this._renderTree(); // show spinner immediately
                    await this._loadEntries(entry.path);
                    this._renderTree(); // replace spinner with children
                    const sel = this.treeContainer.querySelector('.ct-selected');
                    if (sel) {
                        const selDepth = sel.querySelectorAll('.ct-indent').length;
                        let last = sel;
                        let sib = sel.nextElementSibling;
                        while (sib && sib.querySelectorAll('.ct-indent').length > selDepth) {
                            last = sib;
                            sib = sib.nextElementSibling;
                        }
                        if (last !== sel) last.scrollIntoView({ block: 'nearest', behavior: 'instant' });
                    }
                    return;
                }
            } else {
                this._renderTree();
            }
        });

        // Double-click to confirm selection
        div.addEventListener('dblclick', (e) => {
            e.stopPropagation();
            this._selectedPath = entry.path;
            this._doSelect();
        });

        this.treeContainer.appendChild(div);

        // Render children if expanded
        if (this._expandedPaths.has(entry.path)) {
            if (!this._loadedChildren.has(entry.path)) {
                // Still loading — show inline spinner
                const loading = document.createElement('div');
                loading.className = 'fs-loading';
                loading.style.paddingLeft = `${(depth + 1) * 1.25}rem`;
                loading.textContent = 'Loading...';
                this.treeContainer.appendChild(loading);
            } else {
                const children = this._loadedChildren.get(entry.path) || [];
                children.forEach(child => this._renderNode(child, depth + 1));
            }
        }
    },

    _updatePathDisplay() {
        this.pathDisplay.textContent = this._selectedPath || 'No folder selected';
        this.selectBtn.disabled = !this._selectedPath;
    },

    async _expandToPath(targetPath) {
        // Find which root entry is an ancestor of targetPath
        const rootEntries = this._loadedChildren.get('__root__') || [];
        let ancestor = rootEntries.find(e => targetPath === e.path || targetPath.startsWith(e.path + '/'));
        if (!ancestor) return;

        // Walk down the path, expanding each segment
        let currentPath = ancestor.path;
        this._selectedPath = currentPath;
        this._expandedPaths.add(currentPath);
        await this._loadEntries(currentPath);

        if (targetPath !== currentPath) {
            const remaining = targetPath.slice(currentPath.length + 1).split('/');
            for (const segment of remaining) {
                const children = this._loadedChildren.get(currentPath) || [];
                const match = children.find(c => c.name === segment);
                if (!match) break;
                currentPath = match.path;
                this._selectedPath = currentPath;
                this._expandedPaths.add(currentPath);
                await this._loadEntries(currentPath);
            }
        }

        this._selectedPath = targetPath;
        this._updatePathDisplay();
        this._renderTree();
    },
};

export default FSBrowser;
