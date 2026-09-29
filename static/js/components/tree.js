import API from '../api.js';
import ConfirmModal from './confirm.js';
import icons from '../icons.js';
import Keyboard from '../keyboard.js';
import Toast from './toast.js';
import { formatSize } from '../format.js';


const Tree = {
    el: null,
    filterEl: null,
    selected: null,
    onSelect: null,
    onDeselect: null,
    filterText: '',
    treeData: [],

    /** Top-level location node. locationId can be "loc-49" or just 49. */
    locationNode(locationId) {
        const id = String(locationId).startsWith('loc-') ? locationId : `loc-${locationId}`;
        return this.treeData.find(n => n.id === id) || null;
    },
    getLocationLabel(locationId) {
        const node = this.locationNode(locationId);
        return node ? node.label : null;
    },
    /** False only when the location is known to be offline. */
    isLocationOnline(locationId) {
        const node = this.locationNode(locationId);
        return !node || node.online !== false;
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

        return this.loadTree();
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

        const badges = meta.querySelector('.tree-badges');
        for (const e of this.capacityElements(node)) meta.insertBefore(e, badges);
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
            sizeSpan.textContent = totalSize ? formatSize(totalSize) : '';
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

        for (const b of this.locationBadges(node)) el.insertBefore(b, insertBefore);

        // Update meta-row size/phase text
        const sizeSpan = el.querySelector('.tree-size');
        if (sizeSpan) this.setLocationSizeText(sizeSpan, node);
    },

    /** The status badges after a location's name: its operation (with a
     *  cancel badge where it can be cancelled), and "paused". */
    locationBadges(node) {
        const id = node.id;
        const badges = [];
        const badge = (cls, text) => {
            const b = document.createElement('span');
            b.className = `tree-badge ${cls}`;
            b.textContent = text;
            badges.push(b);
        };
        const cancelBadge = (title, confirm, body) => {
            const cb = document.createElement('span');
            cb.className = 'tree-badge cancel tree-badge-clickable';
            cb.textContent = 'cancel';
            cb.title = title;
            cb.addEventListener('click', async (e) => {
                e.stopPropagation();
                const ok = await ConfirmModal.open(confirm);
                if (!ok) return;
                await API.post('/api/scan/cancel', body);
            });
            badges.push(cb);
        };
        if (this.scanningLocations.has(id)) {
            badge('scanning', 'scanning');
            cancelBadge('Cancel scan', {
                title: 'Cancel Scan',
                message: `Stop scanning "${node.label}"? Files already cataloged will be kept.`,
                confirmLabel: 'Cancel Scan',
            }, { location_id: id });
        } else if (this.queuedLocations.has(id)) {
            badge('queued', 'queued');
            cancelBadge('Remove from queue', {
                title: 'Remove from Queue',
                message: `Remove "${node.label}" from the scan queue?`,
                confirmLabel: 'Remove',
            }, { queue_id: this.queuedLocations.get(id) });
        } else if (this.backfillingLocations.has(id)) {
            badge('backfilling', 'backfilling');
            cancelBadge('Cancel backfill', {
                title: 'Cancel Backfill',
                message: `Stop backfilling hashes on "${node.label}"? Hashes already computed will be kept.`,
                confirmLabel: 'Cancel Backfill',
            }, { location_id: id, type: 'backfill' });
        } else if (this.deletingLocations.has(id)) {
            badge('deleting', 'deleting');
        } else if (this.mergingLocations.has(id)) {
            badge('merging', this.mergingLocations.get(id));
        }
        if (this.paused && !this.scanningLocations.has(id) && !this.deletingLocations.has(id)) {
            badge('queued', 'paused');
        }
        return badges;
    },

    /** A location's size in the tree, or its scan phase while its first
     *  scan hasn't produced a size yet. */
    setLocationSizeText(sizeSpan, node) {
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
            sizeSpan.textContent = node.totalSize ? formatSize(node.totalSize) : '';
            sizeSpan.classList.remove('tree-size-scanning');
        }
    },

    /** A location's disk capacity bar, and "RO" if it's read-only; none
     *  without disk stats. */
    capacityElements(node) {
        const ds = node.diskStats;
        if (!ds || !ds.mount) return [];
        const pct = ((ds.total - ds.free) / ds.total * 100).toFixed(1);
        const bar = document.createElement('span');
        bar.className = 'tree-capacity-bar';
        bar.title = `${formatSize(ds.free)} free of ${formatSize(ds.total)}`;
        const fill = document.createElement('span');
        fill.className = 'tree-capacity-fill';
        fill.style.width = pct + '%';
        bar.appendChild(fill);
        const elements = [bar];
        if (ds.readonly) {
            const ro = document.createElement('span');
            ro.className = 'tree-badge readonly';
            ro.textContent = 'RO';
            elements.push(ro);
        }
        return elements;
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
            for (const b of this.locationBadges(node)) item.appendChild(b);
        }

        if (node.type === 'location') {
            // Two-line layout for locations — always show meta row
            item.classList.add('tree-location');
            const meta = document.createElement('div');
            meta.className = 'tree-location-meta';
            const sizeSpan = document.createElement('span');
            sizeSpan.className = 'tree-size';
            this.setLocationSizeText(sizeSpan, node);
            meta.appendChild(sizeSpan);
            for (const e of this.capacityElements(node)) meta.appendChild(e);
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
                sizeSpan.textContent = node.totalSize ? formatSize(node.totalSize) : '';
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
