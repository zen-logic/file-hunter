import API from '../api.js';
import icons from '../icons.js';

function formatSize(bytes) {
    if (bytes === null || bytes === undefined) return '';
    if (bytes < 1024) return bytes + ' B';
    if (bytes < 1048576) return (bytes / 1024).toFixed(1) + ' KB';
    if (bytes < 1073741824) return (bytes / 1048576).toFixed(1) + ' MB';
    if (bytes < 1099511627776) return (bytes / 1073741824).toFixed(1) + ' GB';
    if (bytes < 1125899906842624) return (bytes / 1099511627776).toFixed(1) + ' TB';
    return (bytes / 1125899906842624).toFixed(1) + ' PB';
}

const Merge = {
    overlay: null,
    sourceNameEl: null,
    sourceStatsEl: null,
    treePicker: null,
    destDisplay: null,
    cancelBtn: null,
    submitBtn: null,
    onMerge: null,
    sourceNode: null,
    selectedDest: null,
    treeData: null,
    favourites: [],
    expandedNodes: new Set(),

    init(onMerge) {
        this.onMerge = onMerge;
        this.overlay = document.getElementById('merge-modal');
        this.sourceNameEl = document.getElementById('merge-source-name');
        this.sourceStatsEl = document.getElementById('merge-source-stats');
        this.treePicker = document.getElementById('merge-tree-picker');
        this.destDisplay = document.getElementById('merge-dest-display');
        this.cancelBtn = document.getElementById('merge-cancel');
        this.submitBtn = document.getElementById('merge-submit');

        this.cancelBtn.addEventListener('click', () => this.close());
        this.overlay.addEventListener('click', (e) => {
            if (e.target === this.overlay) this.close();
        });
        document.addEventListener('keydown', (e) => {
            if (e.key === 'Escape' && !this.overlay.classList.contains('hidden')) {
                this.close();
            }
        });
        this.submitBtn.addEventListener('click', () => this.doSubmit());
    },

    async open(sourceNode) {
        this.sourceNode = sourceNode;
        this.selectedDest = null;
        this.expandedNodes = new Set();

        this.sourceNameEl.textContent = sourceNode.label || sourceNode.name;
        this.sourceStatsEl.textContent = 'Loading stats...';
        this.submitBtn.disabled = true;
        this.destDisplay.textContent = 'No folder selected';
        document.getElementById('merge-copy-only').checked = false;

        // Fetch stats for source
        const isLocation = String(sourceNode.id).startsWith('loc-');
        const numId = String(sourceNode.id).replace(/^(loc-|fld-)/, '');
        const statsUrl = isLocation
            ? `/api/locations/${numId}/stats`
            : `/api/folders/${numId}/stats`;
        const statsRes = await API.get(statsUrl);
        if (statsRes.ok) {
            const s = statsRes.data;
            this.sourceStatsEl.textContent = `${(s.fileCount || 0).toLocaleString()} files, ${s.totalSizeFormatted || formatSize(s.totalSize || 0)}`;
        } else {
            this.sourceStatsEl.textContent = '';
        }

        // Fetch tree data and favourites
        const [res, favRes] = await Promise.all([
            API.get('/api/locations'),
            API.get('/api/favourites'),
        ]);
        this.treeData = res.ok ? res.data : [];
        this.favourites = favRes.ok ? favRes.data : [];

        this.renderTree();
        this.overlay.classList.remove('hidden');
    },

    close() {
        this.overlay.classList.add('hidden');
    },

    renderTree() {
        this.treePicker.innerHTML = '';
        if (!this.treeData) return;
        this.renderFavourites(this.treePicker);
        this.treeData.forEach(loc => {
            this.renderTreeNode(this.treePicker, loc, 0);
        });
    },

    renderFavourites(container) {
        if (!this.favourites || this.favourites.length === 0) return;

        const header = document.createElement('div');
        header.className = 'ct-section-header';
        header.textContent = 'Favourites';
        container.appendChild(header);

        for (const fav of this.favourites) {
            const isDisabled = this.isSourceOrChild(fav.id);
            const div = document.createElement('div');
            div.className = 'ct-node';
            if (isDisabled) div.classList.add('ct-offline');
            if (this.selectedDest === fav.id) div.classList.add('ct-selected');

            const heartIcon = document.createElement('span');
            heartIcon.className = 'ct-icon';
            heartIcon.innerHTML = icons.heart;
            div.appendChild(heartIcon);

            const label = document.createElement('span');
            label.className = 'ct-label';
            label.textContent = fav.path;
            div.appendChild(label);

            div.addEventListener('click', (e) => {
                e.stopPropagation();
                if (isDisabled) return;
                this.selectedDest = fav.id;
                this.updateDestDisplay();
                this.renderTree();
            });

            container.appendChild(div);
        }

        const divider = document.createElement('div');
        divider.className = 'ct-divider';
        container.appendChild(divider);
    },

    renderTreeNode(container, node, depth) {
        const isDisabled = node.online === false || this.isSourceOrChild(node.id);
        const div = document.createElement('div');
        div.className = 'ct-node';
        if (isDisabled) div.classList.add('ct-offline');
        if (this.selectedDest === node.id) div.classList.add('ct-selected');

        for (let i = 0; i < depth; i++) {
            const indent = document.createElement('span');
            indent.className = 'ct-indent';
            div.appendChild(indent);
        }

        const hasChildren = node.hasChildren || (node.children && node.children.length > 0);
        const toggle = document.createElement('span');
        toggle.className = 'ct-icon';
        if (hasChildren) {
            toggle.textContent = this.expandedNodes.has(node.id) ? '\u25BE' : '\u25B8';
        }
        div.appendChild(toggle);

        const icon = document.createElement('span');
        icon.className = 'ct-icon';
        icon.innerHTML = node.type === 'location' ? icons.location : icons.folder;
        div.appendChild(icon);

        const label = document.createElement('span');
        label.className = 'ct-label';
        label.textContent = node.label;
        div.appendChild(label);

        div.addEventListener('click', async (e) => {
            e.stopPropagation();
            if (isDisabled) return;

            let expanded = false;
            if (hasChildren) {
                if (this.expandedNodes.has(node.id)) {
                    this.expandedNodes.delete(node.id);
                } else {
                    expanded = true;
                    this.expandedNodes.add(node.id);
                    // Lazy-load children if not yet fetched
                    if (node.children === null) {
                        const numId = node.id.replace('fld-', '');
                        const res = await API.get(`/api/tree/children?ids=${numId}`);
                        if (res.ok && res.data[node.id]) {
                            node.children = res.data[node.id];
                        } else {
                            node.children = [];
                        }
                    }
                }
            }
            this.selectedDest = node.id;
            this.updateDestDisplay();
            this.renderTree();
            if (expanded) {
                const sel = this.treePicker.querySelector('.ct-selected');
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
            }
        });

        container.appendChild(div);

        if (node.children && node.children.length > 0 && this.expandedNodes.has(node.id)) {
            node.children.forEach(child => this.renderTreeNode(container, child, depth + 1));
        }
    },

    isSourceOrChild(nodeId) {
        if (!this.sourceNode) return false;
        const sourceId = String(this.sourceNode.id);
        const checkId = String(nodeId);

        // Exact match
        if (checkId === sourceId) return true;

        // Check if nodeId is a descendant of source by walking tree
        return this.isDescendantOf(this.treeData, sourceId, checkId);
    },

    isDescendantOf(nodes, ancestorId, targetId) {
        for (const n of nodes) {
            if (String(n.id) === ancestorId) {
                // Found the ancestor — check if targetId is in its subtree
                return this.containsNode(n.children || [], targetId);
            }
            if (n.children) {
                const found = this.isDescendantOf(n.children, ancestorId, targetId);
                if (found) return true;
            }
        }
        return false;
    },

    containsNode(children, targetId) {
        if (!children) return false;
        for (const c of children) {
            if (String(c.id) === targetId) return true;
            if (c.children && this.containsNode(c.children, targetId)) return true;
        }
        return false;
    },

    updateDestDisplay() {
        if (!this.selectedDest) {
            this.destDisplay.textContent = 'No folder selected';
            this.submitBtn.disabled = true;
            return;
        }
        const node = this.findNode(this.treeData, this.selectedDest);
        if (node) {
            this.destDisplay.textContent = node.label;
            this.submitBtn.disabled = false;
        }
    },

    findNode(nodes, id) {
        for (const n of nodes) {
            if (n.id === id) return n;
            if (n.children) {
                const found = this.findNode(n.children, id);
                if (found) return found;
            }
        }
        if (nodes === this.treeData && this.favourites) {
            const fav = this.favourites.find(f => f.id === id);
            if (fav) return { id: fav.id, label: fav.path, type: fav.type };
        }
        return null;
    },

    doSubmit() {
        if (!this.selectedDest) return;

        const copyOnly = document.getElementById('merge-copy-only').checked;
        this.onMerge({
            source_id: this.sourceNode.id,
            destination_id: this.selectedDest,
            mode: copyOnly ? 'copy' : 'move',
        });
        this.close();
    },
};

export default Merge;
