import API from '../api.js';
import icons from '../icons.js';

/** After expanding the selected row of a ct-node tree, scroll so its last
 *  child row is visible. */
export function revealExpanded(container) {
    const sel = container.querySelector('.ct-selected');
    if (!sel) return;
    const selDepth = sel.querySelectorAll('.ct-indent').length;
    let last = sel;
    let sib = sel.nextElementSibling;
    while (sib && sib.querySelectorAll('.ct-indent').length > selDepth) {
        last = sib;
        sib = sib.nextElementSibling;
    }
    if (last !== sel) last.scrollIntoView({ block: 'nearest', behavior: 'instant' });
}

/**
 * A destination folder picker: favourites, then the locations tree, whose
 * folders load their children when first expanded.
 *
 * options:
 *   isDisabled(node)   - node (or favourite) can't be picked
 *   offlineSelectable  - offline nodes can still be picked (marked as a hint)
 *   onPick(id, label, node) - called when a destination is picked
 *   renderTop(picker)  - optional: adds rows above the favourites
 */
export function createFolderPicker(container, options) {
    const isDisabled = options.isDisabled || (() => false);

    const picker = {
        container,
        selected: null,
        treeData: [],
        favourites: [],
        expandedNodes: new Set(),

        /** Load the locations and favourites, clear the selection, render. */
        async load() {
            this.selected = null;
            this.expandedNodes = new Set();
            const [res, favRes] = await Promise.all([
                API.get('/api/locations'),
                API.get('/api/favourites'),
            ]);
            this.treeData = res.ok ? res.data : [];
            this.favourites = favRes.ok ? favRes.data : [];
            this.render();
        },

        select(id, label, node) {
            this.selected = id;
            options.onPick(id, label, node);
            this.render();
        },

        render() {
            container.innerHTML = '';
            if (!this.treeData) return;
            if (options.renderTop) options.renderTop(this);
            this.renderFavourites();
            this.treeData.forEach(loc => this.renderNode(loc, 0));
        },

        row(depth) {
            const div = document.createElement('div');
            div.className = 'ct-node';
            for (let i = 0; i < depth; i++) {
                const indent = document.createElement('span');
                indent.className = 'ct-indent';
                div.appendChild(indent);
            }
            return div;
        },

        addIcon(div, html, text) {
            const span = document.createElement('span');
            span.className = 'ct-icon';
            if (html) span.innerHTML = html;
            if (text) span.textContent = text;
            div.appendChild(span);
        },

        addLabel(div, text) {
            const label = document.createElement('span');
            label.className = 'ct-label';
            label.textContent = text;
            div.appendChild(label);
            return label;
        },

        addDivider() {
            const divider = document.createElement('div');
            divider.className = 'ct-divider';
            container.appendChild(divider);
        },

        renderFavourites() {
            if (!this.favourites || this.favourites.length === 0) return;

            const header = document.createElement('div');
            header.className = 'ct-section-header';
            header.textContent = 'Favourites';
            container.appendChild(header);

            for (const fav of this.favourites) {
                const disabled = isDisabled(fav);
                const div = this.row(0);
                if (disabled) div.classList.add('ct-offline');
                if (this.selected === fav.id) div.classList.add('ct-selected');
                this.addIcon(div, icons.heart);
                this.addLabel(div, fav.path);
                div.addEventListener('click', (e) => {
                    e.stopPropagation();
                    if (disabled) return;
                    this.select(fav.id, fav.path, fav);
                });
                container.appendChild(div);
            }

            this.addDivider();
        },

        renderNode(node, depth) {
            const offline = node.online === false;
            const disabled = isDisabled(node) || (offline && !options.offlineSelectable);
            const div = this.row(depth);
            if (disabled) div.classList.add('ct-offline');
            if (offline && options.offlineSelectable) div.classList.add('ct-offline-hint');
            if (this.selected === node.id) div.classList.add('ct-selected');

            const hasChildren = node.hasChildren || (node.children && node.children.length > 0);
            this.addIcon(div, null, hasChildren
                ? (this.expandedNodes.has(node.id) ? '▾' : '▸')
                : null);
            this.addIcon(div, node.type === 'location' ? icons.location : icons.folder);
            this.addLabel(div, node.label);

            div.addEventListener('click', async (e) => {
                e.stopPropagation();
                if (disabled) return;

                let expanded = false;
                if (hasChildren) {
                    if (this.expandedNodes.has(node.id)) {
                        this.expandedNodes.delete(node.id);
                    } else {
                        expanded = true;
                        this.expandedNodes.add(node.id);
                        if (node.children === null) {
                            const numId = node.id.replace('fld-', '');
                            const res = await API.get(`/api/tree/children?ids=${numId}`);
                            node.children = res.ok && res.data[node.id] ? res.data[node.id] : [];
                        }
                    }
                }
                this.select(node.id, node.label, node);
                if (expanded) revealExpanded(container);
            });

            container.appendChild(div);

            if (node.children && node.children.length > 0 && this.expandedNodes.has(node.id)) {
                node.children.forEach(child => this.renderNode(child, depth + 1));
            }
        },

        /** True if nodeId is rootId or somewhere below it in the loaded tree. */
        inSubtree(rootId, nodeId) {
            if (!rootId) return false;
            const root = String(rootId);
            const target = String(nodeId);
            if (target === root) return true;
            const contains = (nodes) => (nodes || []).some(
                c => String(c.id) === target || contains(c.children)
            );
            const find = (nodes) => {
                for (const n of nodes || []) {
                    if (String(n.id) === root) return contains(n.children);
                    if (n.children) {
                        const found = find(n.children);
                        if (found) return true;
                    }
                }
                return false;
            };
            return find(this.treeData);
        },
    };
    return picker;
}
