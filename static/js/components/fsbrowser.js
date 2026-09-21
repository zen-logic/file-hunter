import API from '../api.js';
import icons from '../icons.js';

const FSBrowser = {
    overlay: null,
    treeContainer: null,
    pathDisplay: null,
    cancelBtn: null,
    selectBtn: null,
    selectedPath: null,
    loadedChildren: new Map(),
    expandedPaths: new Set(),
    onSelect: null,
    browseUrl: '/api/browse',

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
        this.selectBtn.addEventListener('click', () => this.doSelect());
    },

    async open(initialPath, onSelect, browseUrl) {
        this.onSelect = onSelect;
        this.browseUrl = browseUrl || '/api/browse';
        this.selectedPath = null;
        this.loadedChildren = new Map();
        this.expandedPaths = new Set();
        this.updatePathDisplay();

        this.overlay.classList.remove('hidden');

        // Show loading state while fetching from agent
        this.treeContainer.innerHTML = '<div class="fs-loading">Loading...</div>';

        // Load root entries
        await this.loadEntries(null);
        this.renderTree();

        // If initialPath provided, expand to it
        if (initialPath) {
            await this.expandToPath(initialPath);
        }
    },

    close() {
        this.overlay.classList.add('hidden');
    },

    doSelect() {
        if (this.selectedPath && this.onSelect) {
            this.onSelect(this.selectedPath);
        }
        this.close();
    },

    async loadEntries(path) {
        const key = path || '__root__';
        if (this.loadedChildren.has(key)) return;

        const url = path ? `${this.browseUrl}?path=${encodeURIComponent(path)}` : this.browseUrl;
        const res = await API.get(url);
        if (res.ok) {
            this.loadedChildren.set(key, res.data.entries);
        } else {
            this.loadedChildren.set(key, []);
        }
    },

    renderTree() {
        this.treeContainer.innerHTML = '';
        const rootEntries = this.loadedChildren.get('__root__') || [];
        rootEntries.forEach(entry => this.renderNode(entry, 0));
    },

    renderNode(entry, depth) {
        const div = document.createElement('div');
        div.className = 'ct-node';
        if (this.selectedPath === entry.path) div.classList.add('ct-selected');

        for (let i = 0; i < depth; i++) {
            const indent = document.createElement('span');
            indent.className = 'ct-indent';
            div.appendChild(indent);
        }

        // Expand/collapse toggle
        const toggle = document.createElement('span');
        toggle.className = 'ct-icon';
        if (entry.hasChildren) {
            toggle.textContent = this.expandedPaths.has(entry.path) ? '\u25BE' : '\u25B8';
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
            this.selectedPath = entry.path;
            this.updatePathDisplay();

            if (entry.hasChildren) {
                if (this.expandedPaths.has(entry.path)) {
                    this.expandedPaths.delete(entry.path);
                    this.renderTree();
                } else {
                    this.expandedPaths.add(entry.path);
                    this.renderTree(); // show spinner immediately
                    await this.loadEntries(entry.path);
                    this.renderTree(); // replace spinner with children
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
                this.renderTree();
            }
        });

        // Double-click to confirm selection
        div.addEventListener('dblclick', (e) => {
            e.stopPropagation();
            this.selectedPath = entry.path;
            this.doSelect();
        });

        this.treeContainer.appendChild(div);

        // Render children if expanded
        if (this.expandedPaths.has(entry.path)) {
            if (!this.loadedChildren.has(entry.path)) {
                // Still loading — show inline spinner
                const loading = document.createElement('div');
                loading.className = 'fs-loading';
                loading.style.paddingLeft = `${(depth + 1) * 1.25}rem`;
                loading.textContent = 'Loading...';
                this.treeContainer.appendChild(loading);
            } else {
                const children = this.loadedChildren.get(entry.path) || [];
                children.forEach(child => this.renderNode(child, depth + 1));
            }
        }
    },

    updatePathDisplay() {
        this.pathDisplay.textContent = this.selectedPath || 'No folder selected';
        this.selectBtn.disabled = !this.selectedPath;
    },

    async expandToPath(targetPath) {
        // Find which root entry is an ancestor of targetPath
        const rootEntries = this.loadedChildren.get('__root__') || [];
        let ancestor = rootEntries.find(e => targetPath === e.path || targetPath.startsWith(e.path + '/'));
        if (!ancestor) return;

        // Walk down the path, expanding each segment
        let currentPath = ancestor.path;
        this.selectedPath = currentPath;
        this.expandedPaths.add(currentPath);
        await this.loadEntries(currentPath);

        if (targetPath !== currentPath) {
            const remaining = targetPath.slice(currentPath.length + 1).split('/');
            for (const segment of remaining) {
                const children = this.loadedChildren.get(currentPath) || [];
                const match = children.find(c => c.name === segment);
                if (!match) break;
                currentPath = match.path;
                this.selectedPath = currentPath;
                this.expandedPaths.add(currentPath);
                await this.loadEntries(currentPath);
            }
        }

        this.selectedPath = targetPath;
        this.updatePathDisplay();
        this.renderTree();
    },
};

export default FSBrowser;
