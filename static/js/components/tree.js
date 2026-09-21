import API from '../api.js';
import ConfirmModal from './confirm.js';
import icons from '../icons.js';
import Keyboard from '../keyboard.js';
import Toast from './toast.js';

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
    expandedIds: new Set(),
    scanningLocations: new Set(),
    scanningPhases: new Map(),
    queuedLocations: new Map(),  // node id -> queue_id
    backfillingLocations: new Set(),
    deletingLocations: new Set(),
    mergingLocations: new Map(),  // node id -> badge label
    paused: false,

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
                this.updateSelection(null);
                if (this.onDeselect) this.onDeselect();
            }
        });

        Keyboard.registerPanel('tree', (e) => this.handleKey(e));

        this.loadTree();
    },

    getLocation(nodeId) {
        const path = this.findPath(this.treeData, nodeId);
        return path ? path[0] : null;
    },

    async navigateTo(nodeId) {
        let path = this.findPath(this.treeData, nodeId);
        if (!path) {
            // Node not loaded yet — fetch and merge the expand path
            const loaded = await this.expandToNode(nodeId);
            if (!loaded) return;
            path = this.findPath(this.treeData, nodeId);
            if (!path) return;
        }
        for (let i = 0; i < path.length - 1; i++) {
            path[i].expanded = true;
            this.expandedIds.add(path[i].id);
        }
        const target = path[path.length - 1];
        this.selected = target.id;
        this.render();
        if (this.onSelect) this.onSelect(target);
    },

    async revealNode(nodeId) {
        let path = this.findPath(this.treeData, nodeId);
        if (!path) {
            const loaded = await this.expandToNode(nodeId);
            if (!loaded) return null;
            path = this.findPath(this.treeData, nodeId);
            if (!path) return null;
        }
        for (let i = 0; i < path.length - 1; i++) {
            path[i].expanded = true;
            this.expandedIds.add(path[i].id);
        }
        const target = path[path.length - 1];
        this.selected = target.id;
        this.render();
        return target;
    },

    async expandToNode(nodeId) {
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
                const node = this.findNode(pid);
                if (node) node.children = childrenByParent[pid];
            }
        }

        // Mark all path nodes as expanded
        for (const pid of path) {
            this.expandedIds.add(pid);
        }

        return true;
    },

    mergeChildrenByParent(childrenByParent) {
        for (const [parentId, children] of Object.entries(childrenByParent)) {
            const parentNode = parentId.startsWith('loc-')
                ? this.treeData.find(n => n.id === parentId)
                : this.findNode(parentId);
            if (parentNode) {
                parentNode.children = children;
            }
        }
    },

    findPath(nodes, nodeId, trail) {
        trail = trail || [];
        for (const node of nodes) {
            const current = trail.concat(node);
            if (node.id === nodeId) return current;
            if (node.children) {
                const found = this.findPath(node.children, nodeId, current);
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
        this.expandedIds.clear();
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
        const isNew = !this.scanningLocations.has(key);
        this.scanningLocations.add(key);
        const oldPhase = this.scanningPhases.get(key);
        if (phase) this.scanningPhases.set(key, phase);
        if (isNew || oldPhase !== phase) this.updateLocationBadges(key);
    },

    clearScanningLocation(locationId) {
        const key = 'loc-' + locationId;
        if (!this.scanningLocations.has(key)) return;
        this.scanningLocations.delete(key);
        this.scanningPhases.delete(key);
        this.updateLocationBadges(key);
    },

    setMergingLocation(locationId, label) {
        const key = 'loc-' + locationId;
        this.mergingLocations.set(key, label);
        this.updateLocationBadges(key);
    },

    clearMergingLocation(locationId) {
        const key = 'loc-' + locationId;
        if (!this.mergingLocations.has(key)) return;
        this.mergingLocations.delete(key);
        this.updateLocationBadges(key);
    },

    setLocationChildren(locationId, children) {
        const node = this.findNode('loc-' + locationId);
        if (node) {
            node.children = children;
            this.render();
        }
    },

    updateOnlineStatus(locationIds, online, diskStats) {
        for (const id of locationIds) {
            const node = this.findNode(id);
            if (!node) continue;
            const changed = node.online !== online;
            node.online = online;
            if (diskStats && diskStats[id]) node.diskStats = diskStats[id];
            if (!changed && !(diskStats && diskStats[id])) continue;

            const el = this.findItemEl(id);
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
                this.updateCapacityBar(el, node);
            }
        }
    },

    setFavourite(nodeId, favourite) {
        const node = this.findNode(nodeId);
        if (node) node.favourite = favourite;

        const el = this.findItemEl(nodeId);
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

    updateCapacityBar(el, node) {
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
        const node = this.findNode(key);
        if (!node) return;
        node.totalSize = totalSize;

        const el = this.findItemEl(key);
        if (!el) return;
        const sizeSpan = el.querySelector('.tree-size');
        if (sizeSpan) {
            // If scanning with no totalSize, phase text is shown instead — handled by updateLocationBadges
            if (this.scanningLocations.has(key) && !totalSize) return;
            sizeSpan.textContent = totalSize != null ? formatSize(totalSize) : '';
            sizeSpan.classList.remove('tree-size-scanning');
        }
    },

    setQueuedLocation(locationId, queueId) {
        const key = 'loc-' + locationId;
        if (this.queuedLocations.has(key)) return;
        this.queuedLocations.set(key, queueId);
        this.updateLocationBadges(key);
    },

    clearQueuedLocation(locationId) {
        const key = 'loc-' + locationId;
        if (!this.queuedLocations.has(key)) return;
        this.queuedLocations.delete(key);
        this.updateLocationBadges(key);
    },

    setBackfillingLocation(locationId) {
        const key = 'loc-' + locationId;
        if (this.backfillingLocations.has(key)) return;
        this.backfillingLocations.add(key);
        this.updateLocationBadges(key);
    },

    clearBackfillingLocation(locationId) {
        const key = 'loc-' + locationId;
        if (!this.backfillingLocations.has(key)) return;
        this.backfillingLocations.delete(key);
        this.updateLocationBadges(key);
    },

    setDeletingLocation(locationId) {
        const key = typeof locationId === 'string' && locationId.startsWith('loc-') ? locationId : 'loc-' + locationId;
        if (this.deletingLocations.has(key)) return;
        this.deletingLocations.add(key);
        this.updateLocationBadges(key);
    },

    clearDeletingLocation(locationId) {
        const key = typeof locationId === 'string' && locationId.startsWith('loc-') ? locationId : 'loc-' + locationId;
        if (!this.deletingLocations.has(key)) return;
        this.deletingLocations.delete(key);
        this.updateLocationBadges(key);
    },

    async reload() {
        const res = await API.get('/api/locations');
        if (!res.ok) return;
        this.treeData = res.data;

        // Restore expanded state
        if (this.expandedIds.size > 0) {
            // Expand locations (they're in the fresh data from /api/locations)
            // Collect ALL folder IDs for batch fetch — including deep ones
            // not yet in the tree. mergeChildrenTopDown cascades through
            // multiple passes so children appear as their parents are merged.
            const folderIds = [];
            for (const eid of this.expandedIds) {
                if (eid.startsWith('loc-')) {
                    const node = this.findNode(eid);
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
                    this.mergeChildrenTopDown(childRes.data);
                }
            }

            // Now prune IDs for nodes that genuinely no longer exist
            // (deleted by the operation that triggered this reload)
            const validIds = new Set();
            for (const eid of this.expandedIds) {
                if (this.findNode(eid)) validIds.add(eid);
            }
            this.expandedIds = validIds;
        }

        // Re-expand nodes
        for (const eid of this.expandedIds) {
            const node = this.findNode(eid);
            if (node) node.expanded = true;
        }

        this.render();
    },

    mergeChildrenTopDown(childrenMap) {
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
                const node = this.findNode(key);
                if (node) {
                    node.children = childrenMap[key];
                    merged.add(key);
                    progress = true;
                }
            }
        }
    },

    async loadChildren(nodeId) {
        const numId = nodeId.replace('fld-', '');
        const res = await API.get(`/api/tree/children?ids=${numId}`);
        if (res.ok && res.data[nodeId]) {
            const node = this.findNode(nodeId);
            if (node) {
                node.children = res.data[nodeId];
            }
        }
    },

    nodeMatches(node) {
        if (!this.filterText) return true;
        if (node.label.toLowerCase().includes(this.filterText)) return true;
        if (node.children) {
            return node.children.some(child => this.nodeMatches(child));
        }
        return false;
    },

    getVisibleNodes() {
        const result = [];
        const walk = (nodes) => {
            for (const node of nodes) {
                if (this.filterText && !this.nodeMatches(node)) continue;
                result.push(node);
                if (node.children && (node.expanded || this.filterText)) {
                    walk(node.children);
                }
            }
        };
        walk(this.treeData);
        return result;
    },

    findNode(nodeId, nodes) {
        nodes = nodes || this.treeData;
        for (const node of nodes) {
            if (node.id === nodeId) return node;
            if (node.children) {
                const found = this.findNode(nodeId, node.children);
                if (found) return found;
            }
        }
        return null;
    },

    findItemEl(nodeId) {
        return this.el.querySelector(`[data-node-id="${nodeId}"]`);
    },

    updateSelection(newId) {
        const oldEl = this.el.querySelector('.tree-item.selected');
        if (oldEl) oldEl.classList.remove('selected');
        this.selected = newId;
        if (newId) {
            const newEl = this.findItemEl(newId);
            if (newEl) {
                newEl.classList.add('selected');
                newEl.scrollIntoView({ block: 'nearest', behavior: 'instant' });
            }
        }
    },

    // Rebuild operational badges (scanning/queued/backfilling/deleting/paused)
    // and meta-row phase text on a single location's DOM element in-place.
    updateLocationBadges(nodeId) {
        const el = this.findItemEl(nodeId);
        if (!el) return;
        const node = this.findNode(nodeId);
        if (!node || node.type !== 'location') return;

        // Remove existing operational badges and cancel buttons
        el.querySelectorAll('.tree-badge.scanning, .tree-badge.queued, .tree-badge.backfilling, .tree-badge.deleting, .tree-badge.merging, .tree-badge.cancel').forEach(b => b.remove());

        // Re-add the appropriate badge
        // Insert point: after the .tree-label, before .tree-location-meta
        const meta = el.querySelector('.tree-location-meta');
        const insertBefore = meta || null;

        if (this.scanningLocations.has(nodeId)) {
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
        } else if (this.queuedLocations.has(nodeId)) {
            const qb = document.createElement('span');
            qb.className = 'tree-badge queued';
            qb.textContent = 'queued';
            el.insertBefore(qb, insertBefore);
            const cb = document.createElement('span');
            cb.className = 'tree-badge cancel tree-badge-clickable';
            cb.textContent = 'cancel';
            cb.title = 'Remove from queue';
            const queueId = this.queuedLocations.get(nodeId);
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
        } else if (this.backfillingLocations.has(nodeId)) {
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
        } else if (this.deletingLocations.has(nodeId)) {
            const db = document.createElement('span');
            db.className = 'tree-badge deleting';
            db.textContent = 'deleting';
            el.insertBefore(db, insertBefore);
        } else if (this.mergingLocations.has(nodeId)) {
            const mb = document.createElement('span');
            mb.className = 'tree-badge merging';
            mb.textContent = this.mergingLocations.get(nodeId);
            el.insertBefore(mb, insertBefore);
        }

        if (this.paused && !this.scanningLocations.has(nodeId) && !this.deletingLocations.has(nodeId)) {
            const pb = document.createElement('span');
            pb.className = 'tree-badge queued';
            pb.textContent = 'paused';
            el.insertBefore(pb, insertBefore);
        }

        // Update meta-row size/phase text
        const sizeSpan = el.querySelector('.tree-size');
        if (sizeSpan) {
            if (this.scanningLocations.has(nodeId) && !node.totalSize) {
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
                const phase = this.scanningPhases.get(nodeId);
                sizeSpan.textContent = phaseLabels[phase] || 'scanning...';
                sizeSpan.classList.add('tree-size-scanning');
            } else {
                sizeSpan.textContent = node.totalSize != null ? formatSize(node.totalSize) : '';
                sizeSpan.classList.remove('tree-size-scanning');
            }
        }
    },

    findParent(nodeId, nodes, parent) {
        nodes = nodes || this.treeData;
        for (const node of nodes) {
            if (node.id === nodeId) return parent || null;
            if (node.children) {
                const found = this.findParent(nodeId, node.children, node);
                if (found) return found;
            }
        }
        return null;
    },

    handleKey(e) {
        const visible = this.getVisibleNodes();
        if (visible.length === 0) return;

        const curIdx = this.selected
            ? visible.findIndex(n => n.id === this.selected)
            : -1;

        switch (e.key) {
            case 'ArrowDown': {
                e.preventDefault();
                const newIdx = curIdx < visible.length - 1 ? curIdx + 1 : curIdx;
                const newId = (curIdx === -1 && visible.length > 0) ? visible[0].id : visible[newIdx].id;
                this.updateSelection(newId);
                break;
            }
            case 'ArrowUp': {
                e.preventDefault();
                const newIdx = curIdx > 0 ? curIdx - 1 : 0;
                const newId = (curIdx === -1 && visible.length > 0) ? visible[0].id : visible[newIdx].id;
                this.updateSelection(newId);
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
                    this.expandedIds.add(node.id);
                    if (node.children === null) {
                        this.loadChildren(node.id).then(() => this.render());
                    } else {
                        this.render();
                    }
                } else {
                    const firstChild = node.children && node.children[0];
                    if (firstChild) {
                        this.updateSelection(firstChild.id);
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
                    this.expandedIds.delete(node.id);
                    this.render();
                } else {
                    const parent = this.findParent(node.id);
                    if (parent) {
                        this.updateSelection(parent.id);
                    }
                }
                break;
            }
            case 'Home':
                e.preventDefault();
                this.updateSelection(visible[0].id);
                break;
            case 'End':
                e.preventDefault();
                this.updateSelection(visible[visible.length - 1].id);
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

    scrollSelectedIntoView() {
        const el = this.el.querySelector('.selected');
        if (el) el.scrollIntoView({ block: 'nearest', behavior: 'instant' });
    },

    render() {
        this.el.innerHTML = '';
        const container = document.createElement('div');
        container.className = 'panel-body';
        this.treeData.forEach(location => {
            if (this.nodeMatches(location)) {
                this.renderNode(container, location, 0);
            }
        });
        this.el.appendChild(container);
        this.scrollSelectedIntoView();
    },

    renderNode(parent, node, depth) {
        if (this.filterText && !this.nodeMatches(node)) return;

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
            if (this.scanningLocations.has(node.id)) {
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
            } else if (this.queuedLocations.has(node.id)) {
                const qb = document.createElement('span');
                qb.className = 'tree-badge queued';
                qb.textContent = 'queued';
                item.appendChild(qb);
                const cb = document.createElement('span');
                cb.className = 'tree-badge cancel tree-badge-clickable';
                cb.textContent = 'cancel';
                cb.title = 'Remove from queue';
                const queueId = this.queuedLocations.get(node.id);
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
            } else if (this.backfillingLocations.has(node.id)) {
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
            } else if (this.deletingLocations.has(node.id)) {
                const db = document.createElement('span');
                db.className = 'tree-badge deleting';
                db.textContent = 'deleting';
                item.appendChild(db);
            } else if (this.mergingLocations.has(node.id)) {
                const mb = document.createElement('span');
                mb.className = 'tree-badge merging';
                mb.textContent = this.mergingLocations.get(node.id);
                item.appendChild(mb);
            }
            if (this.paused && !this.scanningLocations.has(node.id) && !this.deletingLocations.has(node.id)) {
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
            if (this.scanningLocations.has(node.id) && !node.totalSize) {
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
                const phase = this.scanningPhases.get(node.id);
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

        // Disclosure arrow: always toggles expand/collapse
        if (hasChildren) {
            toggle.addEventListener('click', async (e) => {
                e.stopPropagation();
                if (node.type === 'location' && this.deletingLocations.has(node.id)) return;
                if (node.expanded) {
                    node.expanded = false;
                    this.expandedIds.delete(node.id);
                } else {
                    node.expanded = true;
                    this.expandedIds.add(node.id);
                    if (node.children === null) {
                        await this.loadChildren(node.id);
                    }
                }
                this.selected = node.id;
                this.render();
                if (this.onSelect) this.onSelect(node);
            });
        }

        // Row click: expand if closed, select only if already open
        item.addEventListener('click', async (e) => {
            e.stopPropagation();
            if (node.type === 'location' && this.deletingLocations.has(node.id)) return;
            let structural = false;
            if (hasChildren && !node.expanded) {
                node.expanded = true;
                this.expandedIds.add(node.id);
                if (node.children === null) {
                    await this.loadChildren(node.id);
                }
                structural = true;
            }
            if (structural) {
                this.selected = node.id;
                this.render();
            } else {
                this.updateSelection(node.id);
            }
            if (this.onSelect) this.onSelect(node);
        });

        // Double-click: collapse if expanded
        if (hasChildren) {
            item.addEventListener('dblclick', (e) => {
                e.stopPropagation();
                if (!node.expanded) return;
                node.expanded = false;
                this.expandedIds.delete(node.id);
                this.render();
            });
        }

        // Drop target for file moves
        item.addEventListener('dragover', (e) => {
            if (!e.dataTransfer.types.includes('application/x-filehunter-move')) return;
            if (node.online === false) return;
            e.preventDefault();
            e.dataTransfer.dropEffect = 'move';
            item.classList.add('drop-target');
        });
        item.addEventListener('dragleave', () => {
            item.classList.remove('drop-target');
        });
        item.addEventListener('drop', async (e) => {
            item.classList.remove('drop-target');
            if (!e.dataTransfer.types.includes('application/x-filehunter-move')) return;
            e.preventDefault();
            e.stopPropagation();
            if (node.online === false) return;

            let payload;
            try {
                payload = JSON.parse(e.dataTransfer.getData('application/x-filehunter-move'));
            } catch { return; }

            const fileIds = payload.file_ids || [];
            const folderIds = payload.folder_ids || [];
            const count = fileIds.length + folderIds.length;
            if (count === 0) return;

            API.post('/api/batch/move', {
                file_ids: fileIds,
                folder_ids: folderIds,
                destination_folder_id: node.id,
            });
            Toast.info(`Moving ${count} item${count !== 1 ? 's' : ''} to ${node.label}`);
        });

        parent.appendChild(item);

        if (node.children && node.children.length > 0 && (node.expanded || this.filterText)) {
            node.children.forEach(child => this.renderNode(parent, child, depth + 1));
        }
    },
};

export default Tree;
