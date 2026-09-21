import API from '../api.js';
import Toast from './toast.js';
import icons from '../icons.js';

const SlideshowTriage = {
    // Delete dialog elements
    delOverlay: null,
    delText: null,
    delList: null,
    delDupsCheck: null,
    delCancel: null,
    delSubmit: null,

    // Consolidate — delegated to unified Consolidate component
    consolidateOpen: null,

    // Tag dialog elements
    tagOverlay: null,
    tagText: null,
    tagList: null,
    tagInput: null,
    tagCancel: null,
    tagSubmit: null,

    // Move dialog elements
    movOverlay: null,
    movText: null,
    movList: null,
    movTree: null,
    movDest: null,
    movCancel: null,
    movSubmit: null,

    // State
    deleteItems: [],
    consolidateItems: [],
    tagItems: [],
    moveItems: [],
    treeData: null,
    favourites: [],
    expandedNodes: new Set(),
    selectedDest: null,

    init() {
        // Delete dialog
        this.delOverlay = document.getElementById('slideshow-delete-modal');
        this.delText = document.getElementById('slideshow-delete-text');
        this.delList = document.getElementById('slideshow-delete-list');
        this.delDupsCheck = document.getElementById('slideshow-delete-dups-check');
        this.delCancel = document.getElementById('slideshow-delete-cancel');
        this.delSubmit = document.getElementById('slideshow-delete-submit');

        this.delCancel.addEventListener('click', () => this.closeDelete());
        this.delOverlay.addEventListener('click', (e) => {
            if (e.target === this.delOverlay) this.closeDelete();
        });
        this.delSubmit.addEventListener('click', () => this.doDelete());

        // Tag dialog
        this.tagOverlay = document.getElementById('slideshow-tag-modal');
        this.tagText = document.getElementById('slideshow-tag-text');
        this.tagList = document.getElementById('slideshow-tag-list');
        this.tagInput = document.getElementById('slideshow-tag-input');
        this.tagCancel = document.getElementById('slideshow-tag-cancel');
        this.tagSubmit = document.getElementById('slideshow-tag-submit');

        this.tagCancel.addEventListener('click', () => this.closeTag());
        this.tagOverlay.addEventListener('click', (e) => {
            if (e.target === this.tagOverlay) this.closeTag();
        });
        this.tagSubmit.addEventListener('click', () => this.doTag());

        // Move dialog
        this.movOverlay = document.getElementById('slideshow-move-modal');
        this.movText = document.getElementById('slideshow-move-text');
        this.movList = document.getElementById('slideshow-move-list');
        this.movTree = document.getElementById('slideshow-move-tree');
        this.movDest = document.getElementById('slideshow-move-dest');
        this.movCancel = document.getElementById('slideshow-move-cancel');
        this.movSubmit = document.getElementById('slideshow-move-submit');
        this.movCopy = document.getElementById('slideshow-move-copy');

        this.movCancel.addEventListener('click', () => this.closeMove());
        this.movOverlay.addEventListener('click', (e) => {
            if (e.target === this.movOverlay) this.closeMove();
        });
        this.movSubmit.addEventListener('click', () => this.doMove());
        this.movCopy.addEventListener('change', () => {
            this.movSubmit.textContent = this.movCopy.checked ? 'Copy' : 'Move';
        });

        // Keyboard shortcuts for all dialogs (consolidate handled by unified component)
        document.addEventListener('keydown', (e) => {
            if (e.key === 'Escape') {
                if (!this.delOverlay.classList.contains('hidden')) {
                    this.closeDelete();
                } else if (!this.tagOverlay.classList.contains('hidden')) {
                    this.closeTag();
                } else if (!this.movOverlay.classList.contains('hidden')) {
                    this.closeMove();
                }
            } else if (e.key === 'Enter') {
                if (!this.tagOverlay.classList.contains('hidden')) {
                    e.preventDefault();
                    this.doTag();
                } else if (!this.delOverlay.classList.contains('hidden')) {
                    e.preventDefault();
                    this.doDelete();
                } else if (!this.movOverlay.classList.contains('hidden')) {
                    e.preventDefault();
                    this.doMove();
                }
            }
        });
    },

    show(deleteItems, consolidateItems, tagItems, moveItems) {
        this.deleteItems = deleteItems || [];
        this.consolidateItems = consolidateItems || [];
        this.tagItems = tagItems || [];
        this.moveItems = moveItems || [];

        this.showNext();
    },

    showNext() {
        if (this.deleteItems.length > 0) {
            this.showDeleteDialog();
        } else if (this.moveItems.length > 0) {
            this.showMoveDialog();
        } else if (this.consolidateItems.length > 0) {
            const items = this.consolidateItems;
            this.consolidateItems = [];
            if (this.consolidateOpen) {
                this.consolidateOpen(items, () => this.showNext());
            }
        } else if (this.tagItems.length > 0) {
            this.showTagDialog();
        } else {
            this.finish();
        }
    },

    // ── Capped file list ──

    renderCappedList(container, items) {
        container.innerHTML = '';
        const max = 5;
        const shown = items.slice(0, max);
        for (const item of shown) {
            const div = document.createElement('div');
            div.textContent = item.name;
            container.appendChild(div);
        }
        if (items.length > max) {
            const more = document.createElement('div');
            more.textContent = `...and ${items.length - max} more`;
            more.style.opacity = '0.5';
            container.appendChild(more);
        }
    },

    // ── Delete dialog ──

    showDeleteDialog() {
        const n = this.deleteItems.length;
        this.delText.textContent = `Delete ${n} file${n !== 1 ? 's' : ''}? Files will be removed from disk and the catalog.`;
        this.renderCappedList(this.delList, this.deleteItems);
        this.delDupsCheck.checked = true;
        this.delSubmit.textContent = 'Delete';
        this.delSubmit.disabled = false;
        this.delOverlay.classList.remove('hidden');
    },

    closeDelete() {
        this.delOverlay.classList.add('hidden');
        this.deleteItems = [];
        this.showNext();
    },

    doDelete() {
        const allDups = this.delDupsCheck.checked;
        const fileIds = this.deleteItems.map(item => item.id);
        const n = fileIds.length;

        // Fire-and-forget — WS batch_deleted handles UI refresh
        API.post('/api/batch/delete', { file_ids: fileIds, all_duplicates: allDups });
        Toast.info(`Deleting ${n} file${n !== 1 ? 's' : ''}...`);

        this.delOverlay.classList.add('hidden');
        this.deleteItems = [];
        this.showNext();
    },

    // ── Tag dialog ──

    showTagDialog() {
        const n = this.tagItems.length;
        this.tagText.textContent = `Tag ${n} file${n !== 1 ? 's' : ''}.`;
        this.renderCappedList(this.tagList, this.tagItems);
        this.tagInput.value = '';
        this.tagSubmit.textContent = 'Tag';
        this.tagSubmit.disabled = false;
        this.tagOverlay.classList.remove('hidden');
        this.tagInput.focus();
    },

    closeTag() {
        this.tagOverlay.classList.add('hidden');
        this.tagItems = [];
        this.showNext();
    },

    doTag() {
        const tags = this.tagInput.value.split(',').map(t => t.trim()).filter(Boolean);
        if (tags.length === 0) return;
        const fileIds = this.tagItems.map(item => item.id);
        const n = fileIds.length;
        const label = tags.length === 1 ? `"${tags[0]}"` : `${tags.length} tags`;

        API.post('/api/batch/tag', { file_ids: fileIds, add_tags: tags });
        Toast.info(`Tagging ${n} file${n !== 1 ? 's' : ''} with ${label}`);

        this.tagOverlay.classList.add('hidden');
        this.tagItems = [];
        this.showNext();
    },

    finish() {
        this.deleteItems = [];
        this.consolidateItems = [];
        this.tagItems = [];
        this.moveItems = [];
    },

    // ── Move dialog ──

    async showMoveDialog() {
        const n = this.moveItems.length;
        this.movText.textContent = `Move or copy ${n} file${n !== 1 ? 's' : ''} to a new location.`;
        this.renderCappedList(this.movList, this.moveItems);

        this.selectedDest = null;
        this.expandedNodes = new Set();
        this.activeDest = this.movDest;
        this.activeTree = this.movTree;
        this.movDest.textContent = 'No folder selected';
        this.movCopy.checked = false;
        this.movSubmit.textContent = 'Move';
        this.movSubmit.disabled = false;

        const [res, favRes] = await Promise.all([
            API.get('/api/locations'),
            API.get('/api/favourites'),
        ]);
        this.treeData = res.ok ? res.data : [];
        this.favourites = favRes.ok ? favRes.data : [];
        this.renderTree();

        this.movOverlay.classList.remove('hidden');
    },

    closeMove() {
        this.movOverlay.classList.add('hidden');
        this.moveItems = [];
        this.showNext();
    },

    doMove() {
        if (!this.selectedDest) return;
        const fileIds = this.moveItems.map(item => item.id);
        const n = fileIds.length;
        const copy = this.movCopy.checked;
        const verb = copy ? 'Copying' : 'Moving';

        API.post('/api/batch/move', {
            file_ids: fileIds,
            destination_folder_id: this.selectedDest,
            copy: copy,
        });
        Toast.info(`${verb} ${n} file${n !== 1 ? 's' : ''}...`);

        this.movOverlay.classList.add('hidden');
        this.moveItems = [];
        this.showNext();
    },

    // ── Tree picker (used by move) ──

    renderTree() {
        const treeEl = this.activeTree;
        treeEl.innerHTML = '';
        if (!this.treeData) return;
        this.renderFavourites(treeEl);
        this.treeData.forEach(loc => {
            this.renderTreeNode(treeEl, loc, 0);
        });
    },

    renderFavourites(container) {
        if (!this.favourites || this.favourites.length === 0) return;
        const destEl = this.activeDest;

        const header = document.createElement('div');
        header.className = 'ct-section-header';
        header.textContent = 'Favourites';
        container.appendChild(header);

        for (const fav of this.favourites) {
            const div = document.createElement('div');
            div.className = 'ct-node';
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
                this.selectedDest = fav.id;
                destEl.textContent = fav.path;
                this.renderTree();
            });

            container.appendChild(div);
        }

        const divider = document.createElement('div');
        divider.className = 'ct-divider';
        container.appendChild(divider);
    },

    renderTreeNode(container, node, depth) {
        const destEl = this.activeDest;
        const div = document.createElement('div');
        div.className = 'ct-node';
        if (node.online === false) div.classList.add('ct-offline');
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
            if (node.online === false) return;

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
                        if (res.ok && res.data[node.id]) {
                            node.children = res.data[node.id];
                        } else {
                            node.children = [];
                        }
                    }
                }
            }
            this.selectedDest = node.id;
            destEl.textContent = node.label;
            this.renderTree();
            if (expanded) {
                const sel = this.movTree.querySelector('.ct-selected');
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
};

export default SlideshowTriage;
