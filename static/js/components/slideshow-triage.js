import API from '../api.js';
import Toast from './toast.js';
import icons from '../icons.js';

const SlideshowTriage = {
    // Delete dialog elements
    _delOverlay: null,
    _delText: null,
    _delList: null,
    _delDupsCheck: null,
    _delCancel: null,
    _delSubmit: null,

    // Consolidate — delegated to unified Consolidate component
    _consolidateOpen: null,

    // Tag dialog elements
    _tagOverlay: null,
    _tagText: null,
    _tagList: null,
    _tagInput: null,
    _tagCancel: null,
    _tagSubmit: null,

    // Move dialog elements
    _movOverlay: null,
    _movText: null,
    _movList: null,
    _movTree: null,
    _movDest: null,
    _movCancel: null,
    _movSubmit: null,

    // State
    _deleteItems: [],
    _consolidateItems: [],
    _tagItems: [],
    _moveItems: [],
    _treeData: null,
    _favourites: [],
    _expandedNodes: new Set(),
    _selectedDest: null,

    init() {
        // Delete dialog
        this._delOverlay = document.getElementById('slideshow-delete-modal');
        this._delText = document.getElementById('slideshow-delete-text');
        this._delList = document.getElementById('slideshow-delete-list');
        this._delDupsCheck = document.getElementById('slideshow-delete-dups-check');
        this._delCancel = document.getElementById('slideshow-delete-cancel');
        this._delSubmit = document.getElementById('slideshow-delete-submit');

        this._delCancel.addEventListener('click', () => this._closeDelete());
        this._delOverlay.addEventListener('click', (e) => {
            if (e.target === this._delOverlay) this._closeDelete();
        });
        this._delSubmit.addEventListener('click', () => this._doDelete());

        // Tag dialog
        this._tagOverlay = document.getElementById('slideshow-tag-modal');
        this._tagText = document.getElementById('slideshow-tag-text');
        this._tagList = document.getElementById('slideshow-tag-list');
        this._tagInput = document.getElementById('slideshow-tag-input');
        this._tagCancel = document.getElementById('slideshow-tag-cancel');
        this._tagSubmit = document.getElementById('slideshow-tag-submit');

        this._tagCancel.addEventListener('click', () => this._closeTag());
        this._tagOverlay.addEventListener('click', (e) => {
            if (e.target === this._tagOverlay) this._closeTag();
        });
        this._tagSubmit.addEventListener('click', () => this._doTag());

        // Move dialog
        this._movOverlay = document.getElementById('slideshow-move-modal');
        this._movText = document.getElementById('slideshow-move-text');
        this._movList = document.getElementById('slideshow-move-list');
        this._movTree = document.getElementById('slideshow-move-tree');
        this._movDest = document.getElementById('slideshow-move-dest');
        this._movCancel = document.getElementById('slideshow-move-cancel');
        this._movSubmit = document.getElementById('slideshow-move-submit');
        this._movCopy = document.getElementById('slideshow-move-copy');

        this._movCancel.addEventListener('click', () => this._closeMove());
        this._movOverlay.addEventListener('click', (e) => {
            if (e.target === this._movOverlay) this._closeMove();
        });
        this._movSubmit.addEventListener('click', () => this._doMove());
        this._movCopy.addEventListener('change', () => {
            this._movSubmit.textContent = this._movCopy.checked ? 'Copy' : 'Move';
        });

        // Escape key for all dialogs (consolidate handled by unified component)
        document.addEventListener('keydown', (e) => {
            if (e.key !== 'Escape') return;
            if (!this._delOverlay.classList.contains('hidden')) {
                this._closeDelete();
            } else if (!this._tagOverlay.classList.contains('hidden')) {
                this._closeTag();
            } else if (!this._movOverlay.classList.contains('hidden')) {
                this._closeMove();
            }
        });
    },

    show(deleteItems, consolidateItems, tagItems, moveItems) {
        this._deleteItems = deleteItems || [];
        this._consolidateItems = consolidateItems || [];
        this._tagItems = tagItems || [];
        this._moveItems = moveItems || [];

        this._showNext();
    },

    _showNext() {
        if (this._deleteItems.length > 0) {
            this._showDeleteDialog();
        } else if (this._moveItems.length > 0) {
            this._showMoveDialog();
        } else if (this._consolidateItems.length > 0) {
            const items = this._consolidateItems;
            this._consolidateItems = [];
            if (this._consolidateOpen) {
                this._consolidateOpen(items, () => this._showNext());
            }
        } else if (this._tagItems.length > 0) {
            this._showTagDialog();
        } else {
            this._finish();
        }
    },

    // ── Capped file list ──

    _renderCappedList(container, items) {
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

    _showDeleteDialog() {
        const n = this._deleteItems.length;
        this._delText.textContent = `Delete ${n} file${n !== 1 ? 's' : ''}? Files will be removed from disk and the catalog.`;
        this._renderCappedList(this._delList, this._deleteItems);
        this._delDupsCheck.checked = true;
        this._delSubmit.textContent = 'Delete';
        this._delSubmit.disabled = false;
        this._delOverlay.classList.remove('hidden');
    },

    _closeDelete() {
        this._delOverlay.classList.add('hidden');
        this._deleteItems = [];
        this._showNext();
    },

    _doDelete() {
        const allDups = this._delDupsCheck.checked;
        const fileIds = this._deleteItems.map(item => item.id);
        const n = fileIds.length;

        // Fire-and-forget — WS batch_deleted handles UI refresh
        API.post('/api/batch/delete', { file_ids: fileIds, all_duplicates: allDups });
        Toast.info(`Deleting ${n} file${n !== 1 ? 's' : ''}...`);

        this._delOverlay.classList.add('hidden');
        this._deleteItems = [];
        this._showNext();
    },

    // ── Tag dialog ──

    _showTagDialog() {
        const n = this._tagItems.length;
        this._tagText.textContent = `Tag ${n} file${n !== 1 ? 's' : ''}.`;
        this._renderCappedList(this._tagList, this._tagItems);
        this._tagInput.value = '';
        this._tagSubmit.textContent = 'Tag';
        this._tagSubmit.disabled = false;
        this._tagOverlay.classList.remove('hidden');
        this._tagInput.focus();
    },

    _closeTag() {
        this._tagOverlay.classList.add('hidden');
        this._tagItems = [];
        this._showNext();
    },

    _doTag() {
        const tags = this._tagInput.value.split(',').map(t => t.trim()).filter(Boolean);
        if (tags.length === 0) return;
        const fileIds = this._tagItems.map(item => item.id);
        const n = fileIds.length;
        const label = tags.length === 1 ? `"${tags[0]}"` : `${tags.length} tags`;

        API.post('/api/batch/tag', { file_ids: fileIds, add_tags: tags });
        Toast.info(`Tagging ${n} file${n !== 1 ? 's' : ''} with ${label}`);

        this._tagOverlay.classList.add('hidden');
        this._tagItems = [];
        this._showNext();
    },

    _finish() {
        this._deleteItems = [];
        this._consolidateItems = [];
        this._tagItems = [];
        this._moveItems = [];
    },

    // ── Move dialog ──

    async _showMoveDialog() {
        const n = this._moveItems.length;
        this._movText.textContent = `Move or copy ${n} file${n !== 1 ? 's' : ''} to a new location.`;
        this._renderCappedList(this._movList, this._moveItems);

        this._selectedDest = null;
        this._expandedNodes = new Set();
        this._activeDest = this._movDest;
        this._activeTree = this._movTree;
        this._movDest.textContent = 'No folder selected';
        this._movCopy.checked = false;
        this._movSubmit.textContent = 'Move';
        this._movSubmit.disabled = false;

        const [res, favRes] = await Promise.all([
            API.get('/api/locations'),
            API.get('/api/favourites'),
        ]);
        this._treeData = res.ok ? res.data : [];
        this._favourites = favRes.ok ? favRes.data : [];
        this._renderTree();

        this._movOverlay.classList.remove('hidden');
    },

    _closeMove() {
        this._movOverlay.classList.add('hidden');
        this._moveItems = [];
        this._showNext();
    },

    _doMove() {
        if (!this._selectedDest) return;
        const fileIds = this._moveItems.map(item => item.id);
        const n = fileIds.length;
        const copy = this._movCopy.checked;
        const verb = copy ? 'Copying' : 'Moving';

        API.post('/api/batch/move', {
            file_ids: fileIds,
            destination_folder_id: this._selectedDest,
            copy: copy,
        });
        Toast.info(`${verb} ${n} file${n !== 1 ? 's' : ''}...`);

        this._movOverlay.classList.add('hidden');
        this._moveItems = [];
        this._showNext();
    },

    // ── Tree picker (used by move) ──

    _renderTree() {
        const treeEl = this._activeTree;
        treeEl.innerHTML = '';
        if (!this._treeData) return;
        this._renderFavourites(treeEl);
        this._treeData.forEach(loc => {
            this._renderTreeNode(treeEl, loc, 0);
        });
    },

    _renderFavourites(container) {
        if (!this._favourites || this._favourites.length === 0) return;
        const destEl = this._activeDest;

        const header = document.createElement('div');
        header.className = 'ct-section-header';
        header.textContent = 'Favourites';
        container.appendChild(header);

        for (const fav of this._favourites) {
            const div = document.createElement('div');
            div.className = 'ct-node';
            if (this._selectedDest === fav.id) div.classList.add('ct-selected');

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
                this._selectedDest = fav.id;
                destEl.textContent = fav.path;
                this._renderTree();
            });

            container.appendChild(div);
        }

        const divider = document.createElement('div');
        divider.className = 'ct-divider';
        container.appendChild(divider);
    },

    _renderTreeNode(container, node, depth) {
        const destEl = this._activeDest;
        const div = document.createElement('div');
        div.className = 'ct-node';
        if (node.online === false) div.classList.add('ct-offline');
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
            if (node.online === false) return;

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
            destEl.textContent = node.label;
            this._renderTree();
        });

        container.appendChild(div);

        if (node.children && node.children.length > 0 && this._expandedNodes.has(node.id)) {
            node.children.forEach(child => this._renderTreeNode(container, child, depth + 1));
        }
    },
};

export default SlideshowTriage;
