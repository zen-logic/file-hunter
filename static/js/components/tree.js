import API from '../api.js';
import ConfirmModal from './confirm.js';
import icons from '../icons.js';
import Keyboard from '../keyboard.js';

function formatSize(bytes) {
    if (!bytes) return '';
    if (bytes < 1024) return bytes + ' B';
    if (bytes < 1048576) return (bytes / 1024).toFixed(1) + ' KB';
    if (bytes < 1073741824) return (bytes / 1048576).toFixed(1) + ' MB';
    if (bytes < 1099511627776) return (bytes / 1073741824).toFixed(1) + ' GB';
    if (bytes < 1125899906842624) return (bytes / 1099511627776).toFixed(1) + ' TB';
    return (bytes / 1125899906842624).toFixed(1) + ' PB';
}

const Tree = {
    el: null,
    filterEl: null,
    selected: null,
    onSelect: null,
    onDeselect: null,
    filterText: '',
    treeData: [],

    getLocationLabel(locationId) {
        // locationId can be "loc-49" or just 49
        const id = String(locationId).startsWith('loc-') ? locationId : `loc-${locationId}`;
        const node = this.treeData.find(n => n.id === id);
        return node ? node.label : null;
    },
    _expandedIds: new Set(),
    _scanningLocations: new Set(),
    _scanningPhases: new Map(),
    _queuedLocations: new Map(),  // node id -> queue_id
    _backfillingLocations: new Set(),
    _deletingLocations: new Set(),
    _mergingLocations: new Map(),  // node id -> badge label
    _paused: false,

    init(onSelect, onDeselect) {
        this.el = document.getElementById('tree-content');
        this.filterEl = document.getElementById('tree-filter');
        this.onSelect = onSelect;
        this.onDeselect = onDeselect;

        this.filterEl.addEventListener('input', () => {
            this.filterText = this.filterEl.value.toLowerCase();
            this.render();
        });

        this.el.addEventListener('click', () => {
            if (this.selected) {
                this._updateSelection(null);
                if (this.onDeselect) this.onDeselect();
            }
        });

        Keyboard.registerPanel('tree', (e) => this.handleKey(e));

        this.loadTree();
    },

    getLocation(nodeId) {
        const path = this._findPath(this.treeData, nodeId);
        return path ? path[0] : null;
    },

    async navigateTo(nodeId) {
        let path = this._findPath(this.treeData, nodeId);
        if (!path) {
            // Node not loaded yet — fetch and merge the expand path
            const loaded = await this._expandToNode(nodeId);
            if (!loaded) return;
            path = this._findPath(this.treeData, nodeId);
            if (!path) return;
        }
        for (let i = 0; i < path.length - 1; i++) {
            path[i].expanded = true;
            this._expandedIds.add(path[i].id);
        }
        const target = path[path.length - 1];
        this.selected = target.id;
        this.render();
        if (this.onSelect) this.onSelect(target);
    },

    async revealNode(nodeId) {
        let path = this._findPath(this.treeData, nodeId);
        if (!path) {
            const loaded = await this._expandToNode(nodeId);
            if (!loaded) return null;
            path = this._findPath(this.treeData, nodeId);
            if (!path) return null;
        }
        for (let i = 0; i < path.length - 1; i++) {
            path[i].expanded = true;
            this._expandedIds.add(path[i].id);
        }
        const target = path[path.length - 1];
        this.selected = target.id;
        this.render();
        return target;
    },

    async _expandToNode(nodeId) {
        // Extract numeric ID from "fld-123"
        const numId = String(nodeId).replace('fld-', '');
        const res = await API.get(`/api/tree/expand?target=${numId}`);
        if (!res.ok || !res.data) return false;

        const { locationId, path, childrenByParent } = res.data;

        // Merge in strict top-down order: location root first, then each
        // ancestor from shallowest to deepest.  Order matters because each
        // merge replaces the parent's children array with fresh nodes —
        // processing a child before its parent would be overwritten.
        if (childrenByParent[locationId]) {
            const locNode = this.treeData.find(n => n.id === locationId);
            if (locNode) locNode.children = childrenByParent[locationId];
        }
        for (const pid of path) {
            if (childrenByParent[pid]) {
                const node = this._findNode(pid);
                if (node) node.children = childrenByParent[pid];
            }
        }

        // Mark all path nodes as expanded
        for (const pid of path) {
            this._expandedIds.add(pid);
        }

        return true;
    },

    _mergeChildrenByParent(childrenByParent) {
        for (const [parentId, children] of Object.entries(childrenByParent)) {
            const parentNode = parentId.startsWith('loc-')
                ? this.treeData.find(n => n.id === parentId)
                : this._findNode(parentId);
            if (parentNode) {
                parentNode.children = children;
            }
        }
    },

    _findPath(nodes, nodeId, trail) {
        trail = trail || [];
        for (const node of nodes) {
            const current = trail.concat(node);
            if (node.id === nodeId) return current;
            if (node.children) {
                const found = this._findPath(node.children, nodeId, current);
                if (found) return found;
            }
        }
        return null;
    },

    async loadTree() {
        const res = await API.get('/api/locations');
        if (res.ok) {
            this.treeData = res.data;
            this.render();
        }
    },

    collapseAll() {
        this._expandedIds.clear();
        this.selected = null;
        const collapse = (nodes) => {
            for (const n of nodes) {
                n.expanded = false;
                if (n.children) collapse(n.children);
            }
        };
        collapse(this.treeData);
        this.render();
    },

    setScanningLocation(locationId, phase) {
        const key = 'loc-' + locationId;
        const isNew = !this._scanningLocations.has(key);
        this._scanningLocations.add(key);
        const oldPhase = this._scanningPhases.get(key);
        if (phase) this._scanningPhases.set(key, phase);
        if (isNew || oldPhase !== phase) this._updateLocationBadges(key);
    },

    clearScanningLocation(locationId) {
        const key = 'loc-' + locationId;
        if (!this._scanningLocations.has(key)) return;
        this._scanningLocations.delete(key);
        this._scanningPhases.delete(key);
        this._updateLocationBadges(key);
    },

    setMergingLocation(locationId, label) {
        const key = 'loc-' + locationId;
        this._mergingLocations.set(key, label);
        this._updateLocationBadges(key);
    },

    clearMergingLocation(locationId) {
        const key = 'loc-' + locationId;
        if (!this._mergingLocations.has(key)) return;
        this._mergingLocations.delete(key);
        this._updateLocationBadges(key);
    },

    setLocationChildren(locationId, children) {
        const node = this._findNode('loc-' + locationId);
        if (node) {
            node.children = children;
            this.render();
        }
    },

    updateOnlineStatus(locationIds, online, diskStats) {
        for (const id of locationIds) {
            const node = this._findNode(id);
            if (!node) continue;
            const changed = node.online !== online;
            node.online = online;
            if (diskStats && diskStats[id]) node.diskStats = diskStats[id];
            if (!changed && !(diskStats && diskStats[id])) continue;

            const el = this._findItemEl(id);
            if (!el) continue;

            // Toggle offline class
            el.classList.toggle('offline', online === false);

            // Update offline badge in meta-row
            const badges = el.querySelector('.tree-badges');
            if (badges) {
                const existing = badges.querySelector('.tree-badge.offline');
                if (online === false && !existing) {
                    const b = document.createElement('span');
                    b.className = 'tree-badge offline';
                    b.textContent = 'offline';
                    badges.appendChild(b);
                } else if (online !== false && existing) {
                    existing.remove();
                }
            }

            // Update capacity bar if disk stats changed
            if (diskStats && diskStats[id] && node.diskStats) {
                this._updateCapacityBar(el, node);
            }
        }
    },

    setFavourite(nodeId, favourite) {
        const node = this._findNode(nodeId);
        if (node) node.favourite = favourite;

        const el = this._findItemEl(nodeId);
        if (!el) return;

        // Update or add/remove the favourite badge
        const label = el.querySelector('.tree-label');
        if (!label) return;
        const existing = label.querySelector('.tree-fav');
        if (favourite && !existing) {
            const fav = document.createElement('span');
            fav.className = 'tree-fav';
            fav.innerHTML = icons.heart;
            label.appendChild(fav);
        } else if (!favourite && existing) {
            existing.remove();
        }
    },

    _updateCapacityBar(el, node) {
        const meta = el.querySelector('.tree-location-meta');
        if (!meta) return;

        // Remove existing capacity bar
        const oldBar = meta.querySelector('.tree-capacity-bar');
        const oldRo = meta.querySelector('.tree-badge.readonly');
        if (oldBar) oldBar.remove();
        if (oldRo) oldRo.remove();

        if (node.diskStats && node.diskStats.mount) {
            const pct = ((node.diskStats.total - node.diskStats.free) / node.diskStats.total * 100).toFixed(1);
            const bar = document.createElement('span');
            bar.className = 'tree-capacity-bar';
            bar.title = `${formatSize(node.diskStats.free)} free of ${formatSize(node.diskStats.total)}`;
            const fill = document.createElement('span');
            fill.className = 'tree-capacity-fill';
            fill.style.width = pct + '%';
            bar.appendChild(fill);
            // Insert before .tree-badges
            const badges = meta.querySelector('.tree-badges');
            meta.insertBefore(bar, badges);
            if (node.diskStats.readonly) {
                const ro = document.createElement('span');
                ro.className = 'tree-badge readonly';
                ro.textContent = 'RO';
                meta.insertBefore(ro, badges);
            }
        }
    },

    updateLocationSize(locationId, totalSize) {
        const key = 'loc-' + locationId;
        const node = this._findNode(key);
        if (!node) return;
        node.totalSize = totalSize;

        const el = this._findItemEl(key);
        if (!el) return;
        const sizeSpan = el.querySelector('.tree-size');
        if (sizeSpan) {
            // If scanning with no totalSize, phase text is shown instead — handled by _updateLocationBadges
            if (this._scanningLocations.has(key) && !totalSize) return;
            sizeSpan.textContent = totalSize != null ? formatSize(totalSize) : '';
            sizeSpan.classList.remove('tree-size-scanning');
        }
    },

    setQueuedLocation(locationId, queueId) {
        const key = 'loc-' + locationId;
        if (this._queuedLocations.has(key)) return;
        this._queuedLocations.set(key, queueId);
        this._updateLocationBadges(key);
    },

    clearQueuedLocation(locationId) {
        const key = 'loc-' + locationId;
        if (!this._queuedLocations.has(key)) return;
        this._queuedLocations.delete(key);
        this._updateLocationBadges(key);
    },

    setBackfillingLocation(locationId) {
        const key = 'loc-' + locationId;
        if (this._backfillingLocations.has(key)) return;
        this._backfillingLocations.add(key);
        this._updateLocationBadges(key);
    },

    clearBackfillingLocation(locationId) {
        const key = 'loc-' + locationId;
        if (!this._backfillingLocations.has(key)) return;
        this._backfillingLocations.delete(key);
        this._updateLocationBadges(key);
    },

    setDeletingLocation(locationId) {
        const key = typeof locationId === 'string' && locationId.startsWith('loc-') ? locationId : 'loc-' + locationId;
        if (this._deletingLocations.has(key)) return;
        this._deletingLocations.add(key);
        this._updateLocationBadges(key);
    },

    clearDeletingLocation(locationId) {
        const key = typeof locationId === 'string' && locationId.startsWith('loc-') ? locationId : 'loc-' + locationId;
        if (!this._deletingLocations.has(key)) return;
        this._deletingLocations.delete(key);
        this._updateLocationBadges(key);
    },

    async reload() {
        const res = await API.get('/api/locations');
        if (!res.ok) return;
        this.treeData = res.data;

        // Restore expanded state
        if (this._expandedIds.size > 0) {
            // Filter to IDs that are still in the tree (nodes may have been deleted)
            const validIds = [];
            for (const eid of this._expandedIds) {
                if (this._findNode(eid)) validIds.push(eid);
            }
            this._expandedIds = new Set(validIds);

            // Locations already have children from /api/locations — just expand them
            const folderIds = [];
            for (const eid of validIds) {
                if (eid.startsWith('loc-')) {
                    const node = this._findNode(eid);
                    if (node) node.expanded = true;
                } else {
                    folderIds.push(eid);
                }
            }

            // Batch-fetch children for expanded folders
            if (folderIds.length > 0) {
                const numericIds = folderIds.map(id => id.replace('fld-', ''));
                const childRes = await API.get(`/api/tree/children?ids=${numericIds.join(',')}`);
                if (childRes.ok) {
                    this._mergeChildrenTopDown(childRes.data);
                }
            }
        }

        // Re-expand nodes
        for (const eid of this._expandedIds) {
            const node = this._findNode(eid);
            if (node) node.expanded = true;
        }

        this.render();
    },

    _mergeChildrenTopDown(childrenMap) {
        // Sort keys so that shallower nodes (closer to root) are processed first.
        // This ensures parent children arrays exist before we try to find deeper nodes.
        // We do multiple passes: merge what we can, repeat until nothing new merges.
        const keys = Object.keys(childrenMap);
        const merged = new Set();
        let progress = true;
        while (progress) {
            progress = false;
            for (const key of keys) {
                if (merged.has(key)) continue;
                const node = this._findNode(key);
                if (node) {
                    node.children = childrenMap[key];
                    merged.add(key);
                    progress = true;
                }
            }
        }
    },

    async _loadChildren(nodeId) {
        const numId = nodeId.replace('fld-', '');
        const res = await API.get(`/api/tree/children?ids=${numId}`);
        if (res.ok && res.data[nodeId]) {
            const node = this._findNode(nodeId);
            if (node) {
                node.children = res.data[nodeId];
            }
        }
    },

    _nodeMatches(node) {
        if (!this.filterText) return true;
        if (node.label.toLowerCase().includes(this.filterText)) return true;
        if (node.children) {
            return node.children.some(child => this._nodeMatches(child));
        }
        return false;
    },

    _getVisibleNodes() {
        const result = [];
        const walk = (nodes) => {
            for (const node of nodes) {
                if (this.filterText && !this._nodeMatches(node)) continue;
                result.push(node);
                if (node.children && (node.expanded || this.filterText)) {
                    walk(node.children);
                }
            }
        };
        walk(this.treeData);
        return result;
    },

    _findNode(nodeId, nodes) {
        nodes = nodes || this.treeData;
        for (const node of nodes) {
            if (node.id === nodeId) return node;
            if (node.children) {
                const found = this._findNode(nodeId, node.children);
                if (found) return found;
            }
        }
        return null;
    },

    _findItemEl(nodeId) {
        return this.el.querySelector(`[data-node-id="${nodeId}"]`);
    },

    _updateSelection(newId) {
        const oldEl = this.el.querySelector('.tree-item.selected');
        if (oldEl) oldEl.classList.remove('selected');
        this.selected = newId;
        if (newId) {
            const newEl = this._findItemEl(newId);
            if (newEl) {
                newEl.classList.add('selected');
                newEl.scrollIntoView({ block: 'nearest', behavior: 'instant' });
            }
        }
    },

    // Rebuild operational badges (scanning/queued/backfilling/deleting/paused)
    // and meta-row phase text on a single location's DOM element in-place.
    _updateLocationBadges(nodeId) {
        const el = this._findItemEl(nodeId);
        if (!el) return;
        const node = this._findNode(nodeId);
        if (!node || node.type !== 'location') return;

        // Remove existing operational badges and cancel buttons
        el.querySelectorAll('.tree-badge.scanning, .tree-badge.queued, .tree-badge.backfilling, .tree-badge.deleting, .tree-badge.merging, .tree-badge.cancel').forEach(b => b.remove());

        // Re-add the appropriate badge
        // Insert point: after the .tree-label, before .tree-location-meta
        const meta = el.querySelector('.tree-location-meta');
        const insertBefore = meta || null;

        if (this._scanningLocations.has(nodeId)) {
            const sb = document.createElement('span');
            sb.className = 'tree-badge scanning';
            sb.textContent = 'scanning';
            el.insertBefore(sb, insertBefore);
            const cb = document.createElement('span');
            cb.className = 'tree-badge cancel tree-badge-clickable';
            cb.textContent = 'cancel';
            cb.title = 'Cancel scan';
            cb.addEventListener('click', async (e) => {
                e.stopPropagation();
                const ok = await ConfirmModal.open({
                    title: 'Cancel Scan',
                    message: `Stop scanning "${node.label}"? Files already cataloged will be kept.`,
                    confirmLabel: 'Cancel Scan',
                });
                if (!ok) return;
                await API.post('/api/scan/cancel', { location_id: nodeId });
            });
            el.insertBefore(cb, insertBefore);
        } else if (this._queuedLocations.has(nodeId)) {
            const qb = document.createElement('span');
            qb.className = 'tree-badge queued';
            qb.textContent = 'queued';
            el.insertBefore(qb, insertBefore);
            const cb = document.createElement('span');
            cb.className = 'tree-badge cancel tree-badge-clickable';
            cb.textContent = 'cancel';
            cb.title = 'Remove from queue';
            const queueId = this._queuedLocations.get(nodeId);
            cb.addEventListener('click', async (e) => {
                e.stopPropagation();
                const ok = await ConfirmModal.open({
                    title: 'Remove from Queue',
                    message: `Remove "${node.label}" from the scan queue?`,
                    confirmLabel: 'Remove',
                });
                if (!ok) return;
                await API.post('/api/scan/cancel', { queue_id: queueId });
            });
            el.insertBefore(cb, insertBefore);
        } else if (this._backfillingLocations.has(nodeId)) {
            const bb = document.createElement('span');
            bb.className = 'tree-badge backfilling';
            bb.textContent = 'backfilling';
            el.insertBefore(bb, insertBefore);
            const cb = document.createElement('span');
            cb.className = 'tree-badge cancel tree-badge-clickable';
            cb.textContent = 'cancel';
            cb.title = 'Cancel backfill';
            cb.addEventListener('click', async (e) => {
                e.stopPropagation();
                const ok = await ConfirmModal.open({
                    title: 'Cancel Backfill',
                    message: `Stop backfilling hashes on "${node.label}"? Hashes already computed will be kept.`,
                    confirmLabel: 'Cancel Backfill',
                });
                if (!ok) return;
                await API.post('/api/scan/cancel', { location_id: nodeId, type: 'backfill' });
            });
            el.insertBefore(cb, insertBefore);
        } else if (this._deletingLocations.has(nodeId)) {
            const db = document.createElement('span');
            db.className = 'tree-badge deleting';
            db.textContent = 'deleting';
            el.insertBefore(db, insertBefore);
        } else if (this._mergingLocations.has(nodeId)) {
            const mb = document.createElement('span');
            mb.className = 'tree-badge merging';
            mb.textContent = this._mergingLocations.get(nodeId);
            el.insertBefore(mb, insertBefore);
        }

        if (this._paused && !this._scanningLocations.has(nodeId) && !this._deletingLocations.has(nodeId)) {
            const pb = document.createElement('span');
            pb.className = 'tree-badge queued';
            pb.textContent = 'paused';
            el.insertBefore(pb, insertBefore);
        }

        // Update meta-row size/phase text
        const sizeSpan = el.querySelector('.tree-size');
        if (sizeSpan) {
            if (this._scanningLocations.has(nodeId) && !node.totalSize) {
                const phaseLabels = {
                    scanning: 'metadata...',
                    comparing: 'comparing...',
                    hashing: 'partials...',
                    cataloging: 'ingest...',
                    cataloging_hashes: 'hashing...',
                    checking_duplicates: 'hashing...',
                    recounting: 'finalizing...',
                    rebuilding: 'finalizing...',
                };
                const phase = this._scanningPhases.get(nodeId);
                sizeSpan.textContent = phaseLabels[phase] || 'scanning...';
                sizeSpan.classList.add('tree-size-scanning');
            } else {
                sizeSpan.textContent = node.totalSize != null ? formatSize(node.totalSize) : '';
                sizeSpan.classList.remove('tree-size-scanning');
            }
        }
    },

    _findParent(nodeId, nodes, parent) {
        nodes = nodes || this.treeData;
        for (const node of nodes) {
            if (node.id === nodeId) return parent || null;
            if (node.children) {
                const found = this._findParent(nodeId, node.children, node);
                if (found) return found;
            }
        }
        return null;
    },

    handleKey(e) {
        const visible = this._getVisibleNodes();
        if (visible.length === 0) return;

        const curIdx = this.selected
            ? visible.findIndex(n => n.id === this.selected)
            : -1;

        switch (e.key) {
            case 'ArrowDown': {
                e.preventDefault();
                const newIdx = curIdx < visible.length - 1 ? curIdx + 1 : curIdx;
                const newId = (curIdx === -1 && visible.length > 0) ? visible[0].id : visible[newIdx].id;
                this._updateSelection(newId);
                break;
            }
            case 'ArrowUp': {
                e.preventDefault();
                const newIdx = curIdx > 0 ? curIdx - 1 : 0;
                const newId = (curIdx === -1 && visible.length > 0) ? visible[0].id : visible[newIdx].id;
                this._updateSelection(newId);
                break;
            }
            case 'ArrowRight': {
                e.preventDefault();
                if (curIdx === -1) return;
                const node = visible[curIdx];
                const hasChildren = node.hasChildren || (node.children && node.children.length > 0);
                if (!hasChildren) return;
                if (!node.expanded) {
                    node.expanded = true;
                    this._expandedIds.add(node.id);
                    if (node.children === null) {
                        this._loadChildren(node.id).then(() => this.render());
                    } else {
                        this.render();
                    }
                } else {
                    const firstChild = node.children && node.children[0];
                    if (firstChild) {
                        this._updateSelection(firstChild.id);
                    }
                }
                break;
            }
            case 'ArrowLeft': {
                e.preventDefault();
                if (curIdx === -1) return;
                const node = visible[curIdx];
                const hasChildren = node.children && node.children.length > 0;
                if (hasChildren && node.expanded) {
                    node.expanded = false;
                    this._expandedIds.delete(node.id);
                    this.render();
                } else {
                    const parent = this._findParent(node.id);
                    if (parent) {
                        this._updateSelection(parent.id);
                    }
                }
                break;
            }
            case 'Home':
                e.preventDefault();
                this._updateSelection(visible[0].id);
                break;
            case 'End':
                e.preventDefault();
                this._updateSelection(visible[visible.length - 1].id);
                break;
            case 'Enter': {
                e.preventDefault();
                if (curIdx === -1) return;
                const node = visible[curIdx];
                if (this.onSelect) this.onSelect(node);
                break;
            }
            default:
                return;
        }
    },

    _scrollSelectedIntoView() {
        const el = this.el.querySelector('.selected');
        if (el) el.scrollIntoView({ block: 'nearest', behavior: 'instant' });
    },

    render() {
        this.el.innerHTML = '';
        const container = document.createElement('div');
        container.className = 'panel-body';
        this.treeData.forEach(location => {
            if (this._nodeMatches(location)) {
                this._renderNode(container, location, 0);
            }
        });
        this.el.appendChild(container);
        this._scrollSelectedIntoView();
    },

    _renderNode(parent, node, depth) {
        if (this.filterText && !this._nodeMatches(node)) return;

        const item = document.createElement('div');
        item.dataset.nodeId = node.id;
        item.className = 'tree-item' + (node.online === false ? ' offline' : '');
        if (this.selected === node.id) item.classList.add('selected');
        if (node.hidden) item.classList.add('hidden-item');
        if (node.stale) item.classList.add('stale');

        // indentation
        for (let i = 0; i < depth; i++) {
            const indent = document.createElement('span');
            indent.className = 'tree-indent';
            item.appendChild(indent);
        }

        // expand/collapse icon — use hasChildren flag for unloaded nodes
        const hasChildren = node.hasChildren || (node.children && node.children.length > 0);
        const toggle = document.createElement('span');
        toggle.className = 'tree-icon';
        if (hasChildren) {
            const showExpanded = node.expanded || !!this.filterText;
            toggle.textContent = showExpanded ? '\u25BE' : '\u25B8';
        }
        item.appendChild(toggle);

        // node icon
        const icon = document.createElement('span');
        icon.className = 'tree-icon';
        icon.innerHTML = node.type === 'location' ? icons.location : icons.folder;
        item.appendChild(icon);

        // label
        const label = document.createElement('span');
        label.className = 'tree-label';
        label.textContent = node.label;
        if (node.favourite) {
            const fav = document.createElement('span');
            fav.className = 'tree-fav';
            fav.innerHTML = icons.heart;
            label.appendChild(fav);
        }
        item.appendChild(label);

        // scanning/queued badge on the name line
        if (node.type === 'location') {
            if (this._scanningLocations.has(node.id)) {
                const sb = document.createElement('span');
                sb.className = 'tree-badge scanning';
                sb.textContent = 'scanning';
                item.appendChild(sb);
                const cb = document.createElement('span');
                cb.className = 'tree-badge cancel tree-badge-clickable';
                cb.textContent = 'cancel';
                cb.title = 'Cancel scan';
                cb.addEventListener('click', async (e) => {
                    e.stopPropagation();
                    const ok = await ConfirmModal.open({
                        title: 'Cancel Scan',
                        message: `Stop scanning "${node.label}"? Files already cataloged will be kept.`,
                        confirmLabel: 'Cancel Scan',
                    });
                    if (!ok) return;
                    await API.post('/api/scan/cancel', { location_id: node.id });
                });
                item.appendChild(cb);
            } else if (this._queuedLocations.has(node.id)) {
                const qb = document.createElement('span');
                qb.className = 'tree-badge queued';
                qb.textContent = 'queued';
                item.appendChild(qb);
                const cb = document.createElement('span');
                cb.className = 'tree-badge cancel tree-badge-clickable';
                cb.textContent = 'cancel';
                cb.title = 'Remove from queue';
                const queueId = this._queuedLocations.get(node.id);
                cb.addEventListener('click', async (e) => {
                    e.stopPropagation();
                    const ok = await ConfirmModal.open({
                        title: 'Remove from Queue',
                        message: `Remove "${node.label}" from the scan queue?`,
                        confirmLabel: 'Remove',
                    });
                    if (!ok) return;
                    await API.post('/api/scan/cancel', { queue_id: queueId });
                });
                item.appendChild(cb);
            } else if (this._backfillingLocations.has(node.id)) {
                const bb = document.createElement('span');
                bb.className = 'tree-badge backfilling';
                bb.textContent = 'backfilling';
                item.appendChild(bb);
                const cb = document.createElement('span');
                cb.className = 'tree-badge cancel tree-badge-clickable';
                cb.textContent = 'cancel';
                cb.title = 'Cancel backfill';
                cb.addEventListener('click', async (e) => {
                    e.stopPropagation();
                    const ok = await ConfirmModal.open({
                        title: 'Cancel Backfill',
                        message: `Stop backfilling hashes on "${node.label}"? Hashes already computed will be kept.`,
                        confirmLabel: 'Cancel Backfill',
                    });
                    if (!ok) return;
                    await API.post('/api/scan/cancel', { location_id: node.id, type: 'backfill' });
                });
                item.appendChild(cb);
            } else if (this._deletingLocations.has(node.id)) {
                const db = document.createElement('span');
                db.className = 'tree-badge deleting';
                db.textContent = 'deleting';
                item.appendChild(db);
            } else if (this._mergingLocations.has(node.id)) {
                const mb = document.createElement('span');
                mb.className = 'tree-badge merging';
                mb.textContent = this._mergingLocations.get(node.id);
                item.appendChild(mb);
            }
            if (this._paused && !this._scanningLocations.has(node.id) && !this._deletingLocations.has(node.id)) {
                const pb = document.createElement('span');
                pb.className = 'tree-badge queued';
                pb.textContent = 'paused';
                item.appendChild(pb);
            }
        }

        if (node.type === 'location') {
            // Two-line layout for locations — always show meta row
            item.classList.add('tree-location');
            const meta = document.createElement('div');
            meta.className = 'tree-location-meta';
            const sizeSpan = document.createElement('span');
            sizeSpan.className = 'tree-size';
            if (this._scanningLocations.has(node.id) && !node.totalSize) {
                const phaseLabels = {
                    scanning: 'metadata...',
                    comparing: 'comparing...',
                    hashing: 'partials...',
                    cataloging: 'ingest...',
                    cataloging_hashes: 'hashing...',
                    checking_duplicates: 'hashing...',
                    recounting: 'finalizing...',
                    rebuilding: 'finalizing...',
                };
                const phase = this._scanningPhases.get(node.id);
                sizeSpan.textContent = phaseLabels[phase] || 'scanning...';
                sizeSpan.classList.add('tree-size-scanning');
            } else {
                sizeSpan.textContent = node.totalSize != null ? formatSize(node.totalSize) : '';
            }
            meta.appendChild(sizeSpan);
            if (node.diskStats && node.diskStats.mount) {
                const pct = ((node.diskStats.total - node.diskStats.free) / node.diskStats.total * 100).toFixed(1);
                const bar = document.createElement('span');
                bar.className = 'tree-capacity-bar';
                bar.title = `${formatSize(node.diskStats.free)} free of ${formatSize(node.diskStats.total)}`;
                const fill = document.createElement('span');
                fill.className = 'tree-capacity-fill';
                fill.style.width = pct + '%';
                bar.appendChild(fill);
                meta.appendChild(bar);
                if (node.diskStats.readonly) {
                    const ro = document.createElement('span');
                    ro.className = 'tree-badge readonly';
                    ro.textContent = 'RO';
                    meta.appendChild(ro);
                }
            }
            const badges = document.createElement('span');
            badges.className = 'tree-badges';
            if (node.agent) {
                const b = document.createElement('span');
                b.className = 'tree-badge agent';
                b.textContent = node.agent === 'local' ? 'local' : 'remote';
                badges.appendChild(b);
            }
            if (node.online === false) {
                const b = document.createElement('span');
                b.className = 'tree-badge offline';
                b.textContent = 'offline';
                badges.appendChild(b);
            }
            meta.appendChild(badges);
            item.appendChild(meta);
        } else if (node.totalSize > 0 || node.dupExcluded) {
            // Inline size for folder nodes
            if (node.totalSize > 0) {
                const sizeSpan = document.createElement('span');
                sizeSpan.className = 'tree-size';
                sizeSpan.textContent = formatSize(node.totalSize);
                item.appendChild(sizeSpan);
            }
            if (node.dupExcluded) {
                const exBadge = document.createElement('span');
                exBadge.className = 'tree-badge excluded';
                exBadge.textContent = 'excluded';
                item.appendChild(exBadge);
            }
        }

        item.addEventListener('click', async (e) => {
            e.stopPropagation();
            // Block interaction with deleting locations
            if (node.type === 'location' && this._deletingLocations.has(node.id)) return;
            let structural = false;
            if (hasChildren) {
                if (node.expanded) {
                    node.expanded = false;
                    this._expandedIds.delete(node.id);
                    structural = true;
                } else {
                    node.expanded = true;
                    this._expandedIds.add(node.id);
                    if (node.children === null) {
                        await this._loadChildren(node.id);
                    }
                    structural = true;
                }
            }
            if (structural) {
                this.selected = node.id;
                this.render();
            } else {
                this._updateSelection(node.id);
            }
            if (this.onSelect) this.onSelect(node);
        });

        parent.appendChild(item);

        if (node.children && node.children.length > 0 && (node.expanded || this.filterText)) {
            node.children.forEach(child => this._renderNode(parent, child, depth + 1));
        }
    },
};

export default Tree;
