import API from '../api.js';
import icons from '../icons.js';

const MoveFileModal = {
    overlay: null,
    fileNameEl: null,
    treePicker: null,
    destDisplay: null,
    errorEl: null,
    cancelBtn: null,
    submitBtn: null,
    onMove: null,
    _file: null,
    _excludeId: null,
    _selectedDest: null,
    _treeData: null,
    _expandedNodes: new Set(),

    init(onMove) {
        this.onMove = onMove;
        this.overlay = document.getElementById('move-file-modal');
        this.fileNameEl = document.getElementById('move-file-name');
        this.treePicker = document.getElementById('move-file-tree-picker');
        this.destDisplay = document.getElementById('move-file-dest-display');
        this.errorEl = document.getElementById('move-file-error');
        this.cancelBtn = document.getElementById('move-file-cancel');
        this.submitBtn = document.getElementById('move-file-submit');

        this.cancelBtn.addEventListener('click', () => this.close());
        this.overlay.addEventListener('click', (e) => {
            if (e.target === this.overlay) this.close();
        });
        document.addEventListener('keydown', (e) => {
            if (e.key === 'Escape' && !this.overlay.classList.contains('hidden')) {
                this.close();
            }
        });
        this.submitBtn.addEventListener('click', () => this._doSubmit());
    },

    async open(file, excludeId) {
        this._file = file;
        this._excludeId = excludeId || null;
        this._selectedDest = null;
        this._expandedNodes = new Set();

        this.fileNameEl.textContent = file.name;
        this.submitBtn.disabled = true;
        this.destDisplay.textContent = 'No folder selected';
        if (this.errorEl) {
            this.errorEl.textContent = '';
            this.errorEl.classList.add('hidden');
        }

        const res = await API.get('/api/locations');
        if (res.ok) {
            this._treeData = res.data;
        } else {
            this._treeData = [];
        }

        this._renderTree();
        this.overlay.classList.remove('hidden');
    },

    close() {
        this.overlay.classList.add('hidden');
    },

    _renderTree() {
        this.treePicker.innerHTML = '';
        if (!this._treeData) return;
        this._treeData.forEach(loc => {
            this._renderTreeNode(this.treePicker, loc, 0);
        });
    },

    _renderTreeNode(container, node, depth) {
        const isDisabled = this._isExcluded(node.id);
        const isOffline = node.online === false;
        const div = document.createElement('div');
        div.className = 'ct-node';
        if (isDisabled) div.classList.add('ct-offline');
        if (isOffline) div.classList.add('ct-offline-hint');
        if (this._selectedDest === node.id) div.classList.add('ct-selected');

        for (let i = 0; i < depth; i++) {
            const indent = document.createElement('span');
            indent.className = 'ct-indent';
            div.appendChild(indent);
        }

        const hasChildren = node.hasChildren || (node.children && node.children.length > 0);
        const toggle = document.createElement('span');
        toggle.className = 'ct-icon';
        if (hasChildren) {
            toggle.textContent = this._expandedNodes.has(node.id) ? '\u25BE' : '\u25B8';
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

            if (hasChildren) {
                if (this._expandedNodes.has(node.id)) {
                    this._expandedNodes.delete(node.id);
                } else {
                    this._expandedNodes.add(node.id);
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
            this._selectedDest = node.id;
            this._updateDestDisplay();
            this._renderTree();
        });

        container.appendChild(div);

        if (node.children && node.children.length > 0 && this._expandedNodes.has(node.id)) {
            node.children.forEach(child => this._renderTreeNode(container, child, depth + 1));
        }
    },

    _updateDestDisplay() {
        if (!this._selectedDest) {
            this.destDisplay.textContent = 'No folder selected';
            this.submitBtn.disabled = true;
            return;
        }
        const node = this._findNode(this._treeData, this._selectedDest);
        if (node) {
            this.destDisplay.textContent = node.online === false
                ? `${node.label} (offline \u2014 will be queued)`
                : node.label;
            this.submitBtn.disabled = false;
        }
    },

    _findNode(nodes, id) {
        for (const n of nodes) {
            if (n.id === id) return n;
            if (n.children) {
                const found = this._findNode(n.children, id);
                if (found) return found;
            }
        }
        return null;
    },

    _isExcluded(nodeId) {
        if (!this._excludeId) return false;
        const excludeStr = String(this._excludeId);
        const checkStr = String(nodeId);
        if (checkStr === excludeStr) return true;
        return this._isDescendantOf(this._treeData, excludeStr, checkStr);
    },

    _isDescendantOf(nodes, ancestorId, targetId) {
        for (const n of nodes) {
            if (String(n.id) === ancestorId) {
                return this._containsNode(n.children || [], targetId);
            }
            if (n.children) {
                const found = this._isDescendantOf(n.children, ancestorId, targetId);
                if (found) return true;
            }
        }
        return false;
    },

    _containsNode(children, targetId) {
        if (!children) return false;
        for (const c of children) {
            if (String(c.id) === targetId) return true;
            if (c.children && this._containsNode(c.children, targetId)) return true;
        }
        return false;
    },

    _setBusy(busy) {
        this.submitBtn.disabled = busy;
        this.cancelBtn.disabled = busy;
        this.treePicker.style.pointerEvents = busy ? 'none' : '';
        this.treePicker.style.opacity = busy ? '0.5' : '';
        if (busy) {
            this.submitBtn.innerHTML = '<span class="detail-spinner" style="width:1rem;height:1rem;display:inline-block;vertical-align:middle;margin-right:0.4rem"></span>Moving\u2026';
        } else {
            this.submitBtn.textContent = 'Move';
        }
    },

    async _doSubmit() {
        if (!this._selectedDest || !this._file) return;

        if (this.errorEl) {
            this.errorEl.textContent = '';
            this.errorEl.classList.add('hidden');
        }

        this._setBusy(true);
        const result = await this.onMove(this._file, this._selectedDest);
        if (result && result.error) {
            this._setBusy(false);
            if (this.errorEl) {
                this.errorEl.textContent = result.error;
                this.errorEl.classList.remove('hidden');
            }
            return;
        }
        this._setBusy(false);
        this.close();
    },
};

export default MoveFileModal;
