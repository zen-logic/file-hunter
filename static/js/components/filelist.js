import API from '../api.js';
import icons from '../icons.js';
import Keyboard from '../keyboard.js';
import Tree from './tree.js';
import Triage from './triage.js';

const PAGE_SIZE = 120;
const GALLERY_MAX_CONCURRENT = 4;

// ── Gallery image loader with concurrency cap ──
const galleryLoader = {
    queue: [],       // [{img, src}]
    active: 0,
    gen: 0,          // generation — incremented on reset to abandon old loads

    reset() {
        this.queue = [];
        this.active = 0;
        this.gen++;
    },

    enqueue(img, src) {
        this.queue.push({ img, src, gen: this.gen });
        this.pump();
    },

    pump() {
        while (this.active < GALLERY_MAX_CONCURRENT && this.queue.length > 0) {
            const entry = this.queue.shift();
            if (entry.gen !== this.gen) continue;  // stale
            this.active++;
            const done = () => {
                this.active--;
                this.pump();
            };
            entry.img.onload = done;
            entry.img.onerror = done;
            entry.img.src = entry.src;
        }
    },
};

function formatSize(bytes) {
    if (bytes === null || bytes === undefined) return '';
    if (bytes < 1024) return bytes + ' B';
    if (bytes < 1048576) return (bytes / 1024).toFixed(1) + ' KB';
    if (bytes < 1073741824) return (bytes / 1048576).toFixed(1) + ' MB';
    if (bytes < 1099511627776) return (bytes / 1073741824).toFixed(1) + ' GB';
    if (bytes < 1125899906842624) return (bytes / 1099511627776).toFixed(1) + ' TB';
    return (bytes / 1125899906842624).toFixed(1) + ' PB';
}

function formatDate(isoStr) {
    if (!isoStr) return '';
    const d = new Date(isoStr);
    if (isNaN(d)) return isoStr;
    return d.toLocaleDateString();
}

function fileIcon(file) {
    if (file.type === 'folder') return icons.folder;
    const map = {
        image: icons.image,
        video: icons.video,
        audio: icons.audio,
        text: icons.file,
        document: icons.document,
    };
    return map[file.typeHigh] || icons.file;
}

const COLUMNS = [
    { key: 'name',  label: 'Name',     class: '',         width: null   },
    { key: 'type',  label: 'Type',     class: 'col-type', width: '5rem' },
    { key: 'size',  label: 'Size',     class: 'col-size', width: '6rem' },
    { key: 'date',  label: 'Modified', class: 'col-date', width: '8rem' },
];

/** Unique key for an item (files use numeric id, folders use "fld-N") */
function itemKey(item) {
    return String(item.id);
}

const FileList = {
    el: null,
    filterEl: null,
    currentFolder: null,
    currentItems: null,    // current page of files
    currentFolders: null,  // all subfolders (not paged)
    totalFiles: 0,
    currentPage: 0,
    selectedItems: new Map(),   // key → item object
    anchorIdx: null,           // for Shift+click range select
    selectAllGen: 0,           // generation counter — incremented by clearSelection to abort in-flight selectAll
    onSelect: null,
    onFolderOpen: null,
    onDeselect: null,
    onMultiSelect: null,
    onPreview: null,            // space — enlarge the selected file
    onSelectingAll: null,
    onBreadcrumbNav: null,
    currentBreadcrumb: null,
    filterText: '',
    sortKey: 'name',
    sortDir: 1,  // 1 = ascending, -1 = descending
    filterTimer: null,
    searchMode: false,
    searchParams: null,
    dupGroupMode: false,
    favouritesMode: false,
    searchId: null,
    ac: null,
    pendingFocusFile: null,
    viewMode: 'list',  // 'list' or 'gallery'
    viewToggleEl: null,

    init(onSelect, onFolderOpen, onDeselect, onMultiSelect) {
        this.el = document.getElementById('file-content');
        this.breadcrumbEl = document.getElementById('file-breadcrumb');
        this.filterEl = document.getElementById('file-filter');
        this.onSelect = onSelect;
        this.onFolderOpen = onFolderOpen;
        this.onDeselect = onDeselect;
        this.onMultiSelect = onMultiSelect;

        this.viewToggleEl = document.getElementById('file-view-toggle');
        this.viewToggleEl.innerHTML = icons.grid;
        this.viewToggleEl.addEventListener('click', () => {
            this.viewMode = this.viewMode === 'list' ? 'gallery' : 'list';
            this.viewToggleEl.innerHTML = this.viewMode === 'list' ? icons.grid : icons.list;
            this.viewToggleEl.title = this.viewMode === 'list' ? 'Gallery view' : 'List view';
            this.render();
        });

        this.filterEl.addEventListener('input', () => {
            if (this.searchMode) return;  // search has its own filters
            clearTimeout(this.filterTimer);
            this.filterTimer = setTimeout(() => {
                this.filterText = this.filterEl.value;
                this.currentPage = 0;
                this.clearSelection();
                if (this.favouritesMode) {
                    this.applyFavouritesFilter();
                } else {
                    this.fetchFolder();
                }
            }, 300);
        });

        this.el.addEventListener('click', (e) => {
            // Only deselect when clicking empty space (not rows/checkboxes)
            if (e.target === this.el || e.target.closest('.file-table') === null) {
                this.clearSelection();
                this.fireSelectionChange();
                this.render();
            }
        });

        Keyboard.registerPanel('filelist', (e) => this.handleKey(e));

        // Mount triage bar between breadcrumb and file content
        Triage.mount(this.el.parentElement, this.el);

        this.renderFavourites();
    },

    // ── Selection helpers ──

    getSelection() {
        return Array.from(this.selectedItems.values());
    },

    getSelectionCount() {
        return this.selectedItems.size;
    },

    clearSelection() {
        this.selectedItems.clear();
        this.anchorIdx = null;
        this.selectAllGen++;
    },

    selectOnly(item, idx) {
        this.selectedItems.clear();
        this.selectedItems.set(itemKey(item), item);
        this.anchorIdx = idx !== undefined ? idx : this.indexOfItem(item);
    },

    toggleItem(item, idx) {
        const key = itemKey(item);
        if (this.selectedItems.has(key)) {
            this.selectedItems.delete(key);
        } else {
            this.selectedItems.set(key, item);
        }
        this.anchorIdx = idx !== undefined ? idx : this.indexOfItem(item);
    },

    selectRange(fromIdx, toIdx) {
        const items = this.getDisplayItems();
        const lo = Math.min(fromIdx, toIdx);
        const hi = Math.max(fromIdx, toIdx);
        for (let i = lo; i <= hi; i++) {
            if (i >= 0 && i < items.length) {
                this.selectedItems.set(itemKey(items[i]), items[i]);
            }
        }
    },

    indexOfItem(item) {
        const items = this.getDisplayItems();
        const key = itemKey(item);
        return items.findIndex(f => itemKey(f) === key);
    },

    isSelected(item) {
        return this.selectedItems.has(itemKey(item));
    },

    updateHeaderCheckbox() {
        const hcb = this.el.querySelector('thead input[type="checkbox"]');
        if (!hcb) return;
        const selCount = this.selectedItems.size;
        const totalItems = this.totalFiles + (this.currentFolders ? this.currentFolders.length : 0);
        if (selCount === 0) {
            hcb.checked = false;
            hcb.indeterminate = false;
        } else if (selCount >= totalItems && totalItems > 0) {
            hcb.checked = true;
            hcb.indeterminate = false;
        } else {
            hcb.checked = false;
            hcb.indeterminate = true;
        }
    },

    fireSelectionChange() {
        const count = this.selectedItems.size;
        if (count === 0) {
            if (this.onDeselect) this.onDeselect();
        } else if (count === 1) {
            const item = this.getSelection()[0];
            if (this.onSelect) this.onSelect(item);
        } else {
            if (this.onMultiSelect) this.onMultiSelect(this.getSelection());
        }
    },

    async selectAll() {
        const gen = ++this.selectAllGen;

        // Select current page items immediately
        const items = this.getDisplayItems();
        items.forEach(item => {
            this.selectedItems.set(itemKey(item), item);
        });

        // Fetch remaining pages if multi-page
        const totalPages = this.totalPages();
        if (totalPages <= 1) {
            this.fireSelectionChange();
            this.render();
            return;
        }

        // Multi-page: show "selecting..." state while fetching
        if (this.onSelectingAll) this.onSelectingAll();
        this.render();

        for (let page = 0; page < totalPages; page++) {
            if (gen !== this.selectAllGen) return;  // selection was cleared — abort
            if (page === this.currentPage) continue;
            let res;
            if (this.searchMode) {
                const params = new URLSearchParams(this.searchParams);
                params.set('page', page);
                params.set('sort', this.sortKey);
                params.set('sortDir', this.sortDirStr());
                if (this.searchId) params.set('searchId', this.searchId);
                res = await API.get(`/api/search?${params.toString()}`);
            } else {
                const params = new URLSearchParams({
                    folder_id: this.currentFolder,
                    page,
                    sort: this.sortKey,
                    sortDir: this.sortDirStr(),
                });
                if (this.filterText) params.set('filter', this.filterText);
                res = await API.get(`/api/files?${params.toString()}`);
            }
            if (gen !== this.selectAllGen) return;  // check again after await
            if (res.ok && res.data.items) {
                res.data.items.forEach(item => {
                    this.selectedItems.set(itemKey(item), item);
                });
            }
        }
        if (gen !== this.selectAllGen) return;
        // Also select all folders on current page
        if (this.currentFolders) {
            this.currentFolders.forEach(f => {
                this.selectedItems.set(itemKey(f), f);
            });
        }
        this.fireSelectionChange();
        this.render();
    },

    deselectAll() {
        this.clearSelection();
        this.fireSelectionChange();
        this.render();
    },

    // ── Compatibility: selectedFile getter for keyboard nav ──

    get selectedFile() {
        if (this.selectedItems.size === 1) {
            return this.getSelection()[0].name;
        }
        return null;
    },

    set selectedFile(val) {
        // Legacy setter — used by showFolder, showSingleFile etc. to clear
        if (val === null) {
            this.clearSelection();
        }
    },

    // ── Display items ──

    getDisplayItems() {
        // Folders first, then file items — no client-side sort/filter
        const items = [];
        if (this.currentFolders) {
            items.push(...this.currentFolders);
        }
        if (this.currentItems) {
            items.push(...this.currentItems);
        }
        return items;
    },

    totalPages() {
        return Math.max(1, Math.ceil(this.totalFiles / PAGE_SIZE));
    },

    handleKey(e) {
        const items = this.getDisplayItems();
        if (!items || items.length === 0) return;

        // Find current cursor position based on last single-selected or anchor
        let curIdx = -1;
        if (this.selectedItems.size > 0) {
            // Use the last item in selection order or anchor
            const sel = this.getSelection();
            const lastItem = sel[sel.length - 1];
            curIdx = this.indexOfItem(lastItem);
        }

        let newIdx = curIdx;
        const totalPages = this.totalPages();

        // In gallery mode, left/right arrows navigate like up/down
        const key = this.viewMode === 'gallery'
            ? (e.key === 'ArrowRight' ? 'ArrowDown' : e.key === 'ArrowLeft' ? 'ArrowUp' : e.key)
            : e.key;

        switch (key) {
            case 'ArrowDown':
                e.preventDefault();
                if (curIdx === items.length - 1 && this.currentPage < totalPages - 1) {
                    this.goToPage(this.currentPage + 1, 'first', e.shiftKey);
                    return;
                }
                newIdx = curIdx < items.length - 1 ? curIdx + 1 : curIdx;
                if (curIdx === -1) newIdx = 0;

                if (e.shiftKey) {
                    // Extend selection
                    const anchor = this.anchorIdx !== null ? this.anchorIdx : curIdx;
                    this.selectedItems.clear();
                    this.selectRange(anchor, newIdx);
                    this.anchorIdx = anchor;
                    this.fireSelectionChange();
                    this.render();
                    this.scrollSelectedIntoView();
                    return;
                }
                break;
            case 'ArrowUp':
                e.preventDefault();
                if (curIdx === 0 && this.currentPage > 0) {
                    this.goToPage(this.currentPage - 1, 'last', e.shiftKey);
                    return;
                }
                newIdx = curIdx > 0 ? curIdx - 1 : 0;
                if (curIdx === -1) newIdx = 0;

                if (e.shiftKey) {
                    const anchor = this.anchorIdx !== null ? this.anchorIdx : curIdx;
                    this.selectedItems.clear();
                    this.selectRange(anchor, newIdx);
                    this.anchorIdx = anchor;
                    this.fireSelectionChange();
                    this.render();
                    this.scrollSelectedIntoView();
                    return;
                }
                break;
            case 'Home':
                e.preventDefault();
                if (this.currentPage !== 0) {
                    this.goToPage(0, 'first');
                    return;
                }
                newIdx = 0;
                break;
            case 'End':
                e.preventDefault();
                if (this.currentPage !== totalPages - 1) {
                    this.goToPage(totalPages - 1, 'last');
                    return;
                }
                newIdx = items.length - 1;
                break;
            case 'PageDown':
                e.preventDefault();
                if (this.currentPage < totalPages - 1) {
                    this.goToPage(this.currentPage + 1, 'first');
                    return;
                }
                newIdx = items.length - 1;
                break;
            case 'PageUp':
                e.preventDefault();
                if (this.currentPage > 0) {
                    this.goToPage(this.currentPage - 1, 'first');
                    return;
                }
                newIdx = 0;
                break;
            case 'Enter':
                if (curIdx === -1) return;
                if (items[curIdx].type === 'folder') {
                    e.preventDefault();
                    if (this.onFolderOpen) this.onFolderOpen(items[curIdx]);
                }
                return;
            case ' ':
                // Enlarge the selected file without leaving the keyboard —
                // space or escape closes it and the list is still where it was.
                // Only swallows the keypress when a preview actually opened.
                if (curIdx === -1 || items[curIdx].type === 'folder') return;
                if (this.onPreview && this.onPreview(items[curIdx])) {
                    e.preventDefault();
                    // The modal's own keydown listener is registered after the
                    // keyboard manager's, so without this the very keypress
                    // that opened the preview would reach it and close it again.
                    e.stopImmediatePropagation();
                }
                return;
            default:
                // Triage keys: d, c, t, m, z
                if ('dctmz'.includes(e.key)) {
                    e.preventDefault();
                    const targets = this.selectedItems.size > 1
                        ? this.getSelection()
                        : (curIdx >= 0 ? [items[curIdx]] : []);
                    Triage.handleKey(e.key, targets);
                }
                return;
        }

        const file = items[newIdx];
        if (!file) return;
        this.selectOnly(file, newIdx);
        this.render();
        this.fireSelectionChange();
    },

    /** Move the selection one row, as if the arrow key had been pressed.
     *  Used by the preview modal, which swallows keys before the keyboard
     *  manager can route them here. Goes through handleKey so paging at the
     *  list boundaries behaves identically. */
    moveSelection(delta) {
        this.handleKey({
            key: delta > 0 ? 'ArrowDown' : 'ArrowUp',
            shiftKey: false,
            preventDefault() {},
            stopImmediatePropagation() {},
        });
    },

    scrollSelectedIntoView() {
        if (this.viewMode === 'gallery') {
            const el = this.el.querySelector('.gallery-item.selected');
            if (el) el.scrollIntoView({ block: 'nearest', behavior: 'instant' });
            return;
        }
        const el = this.el.querySelector('tr.selected:last-child')
            || this.el.querySelector('tr.selected');
        if (!el) return;

        // The column header is sticky, so it overlays the top of the scroll
        // container. scrollIntoView knows nothing about that and tucks the
        // row underneath it when arrowing back up to the first entry — hence
        // scrolling by hand with the header height excluded from the viewport.
        const head = this.el.querySelector('thead');
        const headerHeight = head ? head.getBoundingClientRect().height : 0;

        const row = el.getBoundingClientRect();
        const view = this.el.getBoundingClientRect();
        const topLimit = view.top + headerHeight;

        if (row.top < topLimit) {
            this.el.scrollTop -= topLimit - row.top;
        } else if (row.bottom > view.bottom) {
            this.el.scrollTop += row.bottom - view.bottom;
        }
    },

    renderEmpty() {
        galleryLoader.reset();
        this.galleryDirty = true;
        this.currentItems = null;
        this.currentFolders = null;
        this.currentBreadcrumb = null;
        this.totalFiles = 0;
        this.currentPage = 0;
        this.searchMode = false;
        this.searchParams = null;
        this.dupGroupMode = false;
        this.favouritesMode = false;
        this.clearSelection();
        this.breadcrumbEl.innerHTML = '';
        this.el.innerHTML = '<div class="panel-body" style="padding: 1rem; color: var(--color-text-placeholder);">Select a folder to view files.</div>';
    },

    async renderFavourites() {
        this.currentItems = null;
        this.currentFolder = null;
        this.currentBreadcrumb = null;
        this.totalFiles = 0;
        this.currentPage = 0;
        this.searchMode = false;
        this.searchParams = null;
        this.dupGroupMode = false;
        this.clearSelection();
        this.breadcrumbEl.innerHTML = '';

        let res;
        try {
            res = await API.get('/api/favourites');
        } catch (_) {
            this.renderEmpty();
            return;
        }

        const items = res.ok ? res.data : [];
        if (items.length === 0) {
            this.renderEmpty();
            return;
        }

        this.favouritesMode = true;
        this.allFavourites = items.map(item => ({
            id: item.id,
            name: item.name,
            type: 'folder',
            location: item.path,
            locationId: item.locationId,
        }));
        this.currentFolders = this.allFavourites;
        this.renderContent();
    },

    showLoading() {
        galleryLoader.reset();
        this.galleryDirty = true;
        this.breadcrumbEl.innerHTML = '';
        this.el.innerHTML = '<div class="detail-loading"><div class="detail-spinner"></div><span>Searching\u2026</span></div>';
    },

    toggleSort(key) {
        if (this.sortKey === key) {
            this.sortDir *= -1;
        } else {
            this.sortKey = key;
            this.sortDir = 1;
        }
        this.currentPage = 0;
        this.clearSelection();
        this.refetch();
    },

    sortDirStr() {
        return this.sortDir === 1 ? 'asc' : 'desc';
    },

    applyFavouritesFilter() {
        if (!this.allFavourites) return;
        const q = this.filterText.toLowerCase();
        this.currentFolders = q
            ? this.allFavourites.filter(f =>
                f.name.toLowerCase().includes(q) || f.location.toLowerCase().includes(q))
            : this.allFavourites;
        this.renderContent();
    },

    async fetchFolder(focusFileId) {
        if (!this.currentFolder) return;
        if (this.ac) this.ac.abort();
        this.ac = new AbortController();
        const signal = this.ac.signal;
        const params = new URLSearchParams({
            folder_id: this.currentFolder,
            page: this.currentPage,
            sort: this.sortKey,
            sortDir: this.sortDirStr(),
        });
        if (this.filterText) params.set('filter', this.filterText);
        if (focusFileId) params.set('focusFile', focusFileId);
        if (this.fresh) {
            params.set('fresh', '1');
            this.fresh = false;
        }

        let res;
        try {
            res = await API.get(`/api/files?${params.toString()}`, { signal });
        } catch (e) {
            if (e.name === 'AbortError') return;
            throw e;
        }
        if (res.ok) {
            this.currentItems = res.data.items;
            this.currentFolders = res.data.folders;
            this.totalFiles = res.data.total;
            this.currentPage = res.data.page;
            this.currentBreadcrumb = res.data.breadcrumb || null;
        } else {
            this.currentItems = [];
            this.currentFolders = [];
            this.totalFiles = 0;
            this.currentBreadcrumb = null;
        }
        this.renderContent();

        if (focusFileId && res.ok && res.data.focusFileId) {
            const foldersLen = this.currentFolders ? this.currentFolders.length : 0;
            const idx = this.currentItems.findIndex(f => f.id === res.data.focusFileId);
            if (idx >= 0) {
                this.selectOnly(this.currentItems[idx], foldersLen + idx);
                this.render();
                this.fireSelectionChange();
                this.scrollSelectedIntoView();
            }
        }
    },

    async fetchSearch(focusFileId) {
        if (!this.searchParams) return;
        if (this.ac) this.ac.abort();
        this.ac = new AbortController();
        const signal = this.ac.signal;
        const params = new URLSearchParams(this.searchParams);
        params.set('page', this.currentPage);
        params.set('sort', this.sortKey);
        params.set('sortDir', this.sortDirStr());
        if (this.searchId) {
            params.set('searchId', this.searchId);
        }
        if (focusFileId) params.set('focusFile', focusFileId);

        let res;
        try {
            res = await API.get(`/api/search?${params.toString()}`, { signal });
        } catch (e) {
            if (e.name === 'AbortError') return;
            throw e;
        }
        if (res.ok) {
            this.currentItems = res.data.items;
            this.currentFolders = res.data.folders && res.data.folders.length ? res.data.folders : null;
            this.totalFiles = res.data.total;
            this.currentPage = res.data.page;
            if (res.data.searchId) this.searchId = res.data.searchId;
        } else {
            this.currentItems = [];
            this.currentFolders = null;
            this.totalFiles = 0;
        }
        this.renderContent();

        if (focusFileId && res.ok && res.data.focusFileId) {
            const foldersLen = this.currentFolders ? this.currentFolders.length : 0;
            const idx = this.currentItems.findIndex(f => f.id === res.data.focusFileId);
            if (idx >= 0) {
                this.selectOnly(this.currentItems[idx], foldersLen + idx);
                this.render();
                this.fireSelectionChange();
                this.scrollSelectedIntoView();
            }
        }
    },

    refetch() {
        if (this.searchMode) {
            this.fetchSearch();
        } else {
            this.fetchFolder();
        }
    },

    renderBreadcrumb() {
        this.breadcrumbEl.innerHTML = '';
        if (!this.currentBreadcrumb || this.currentBreadcrumb.length === 0 || this.searchMode) return;
        this.currentBreadcrumb.forEach((entry, i) => {
            if (i > 0) {
                const sep = document.createElement('span');
                sep.className = 'breadcrumb-sep';
                sep.textContent = '/';
                this.breadcrumbEl.appendChild(sep);
            }
            const seg = document.createElement('span');
            seg.textContent = entry.name;
            seg.className = i < this.currentBreadcrumb.length - 1
                ? 'breadcrumb-segment'
                : 'breadcrumb-segment breadcrumb-current';
            seg.addEventListener('click', () => {
                if (this.onBreadcrumbNav) this.onBreadcrumbNav(entry.nodeId);
            });
            this.breadcrumbEl.appendChild(seg);
        });
    },

    renderContent() {
        this.renderBreadcrumb();
        const items = this.getDisplayItems();
        if (items.length === 0 && this.totalFiles === 0) {
            const msg = this.searchMode ? 'No results found.' : 'Empty folder.';
            this.el.innerHTML = `<div class="panel-body" style="padding: 1rem; color: var(--color-text-placeholder);">${msg}</div>`;
            return;
        }
        this.el.scrollTop = 0;
        this.galleryDirty = true;
        this.render();
    },

    async showFolder(folderId) {
        const focusFileId = this.pendingFocusFile;
        this.pendingFocusFile = null;

        this.currentFolder = folderId;
        this.clearSelection();
        this.searchId = null;
        this.filterText = '';
        this.filterEl.value = '';
        this.sortKey = 'name';
        this.sortDir = 1;
        this.currentPage = 0;
        this.searchMode = false;
        this.searchParams = null;
        this.dupGroupMode = false;
        this.favouritesMode = false;

        await this.fetchFolder(focusFileId);
    },

    async focusFile(fileId) {
        // Try current page first — no round-trip needed
        const items = this.getDisplayItems();
        const idx = items.findIndex(f => f.id === fileId);
        if (idx >= 0) {
            this.selectOnly(items[idx], idx);
            this.render();
            this.fireSelectionChange();
            this.scrollSelectedIntoView();
            return;
        }
        // File is on a different page — refetch with focusFile
        if (this.searchMode) {
            await this.fetchSearch(fileId);
        } else if (this.currentFolder) {
            this.pendingFocusFile = fileId;
            await this.refreshFolder();
        }
    },

    async refreshFolder() {
        if (!this.currentFolder) return;
        const focusFileId = this.pendingFocusFile;
        this.pendingFocusFile = null;
        this.clearSelection();
        await this.fetchFolder(focusFileId);
    },

    showSingleFile(file) {
        this.currentFolder = null;
        this.currentBreadcrumb = null;
        this.filterText = '';
        this.filterEl.value = '';
        this.sortKey = 'name';
        this.sortDir = 1;
        this.currentPage = 0;
        this.searchMode = false;
        this.searchParams = null;
        this.dupGroupMode = false;
        this.currentItems = [file];
        this.currentFolders = null;
        this.totalFiles = 1;
        this.clearSelection();
        this.selectOnly(file, 0);
        this.renderContent();
    },

    async showDuplicateGroup(hash, sourceFileId) {
        this.currentFolder = null;
        this.currentBreadcrumb = null;
        this.clearSelection();
        this.searchId = null;
        this.filterText = '';
        this.filterEl.value = '';
        this.sortKey = 'name';
        this.sortDir = 1;
        this.currentPage = 0;
        this.searchMode = true;
        this.searchParams = { hash };
        this.dupGroupMode = true;
        this.dupGroupSourceId = sourceFileId || null;

        await this.fetchSearch();
    },

    showSearchResults(data, searchParams) {
        this.currentFolder = null;
        this.currentBreadcrumb = null;
        this.clearSelection();
        this.searchId = data.searchId || null;
        this.filterText = '';
        this.filterEl.value = '';
        this.sortKey = 'name';
        this.sortDir = 1;
        this.currentPage = data.page;
        this.searchMode = true;
        this.searchParams = searchParams;
        this.dupGroupMode = false;

        this.currentItems = data.items;
        this.currentFolders = data.folders && data.folders.length ? data.folders : null;
        this.totalFiles = data.total;

        this.renderContent();
    },

    async goToPage(n, selectPosition, extend = false) {
        const totalPages = this.totalPages();
        this.currentPage = Math.max(0, Math.min(n, totalPages - 1));

        await (this.searchMode ? this.fetchSearch() : this.fetchFolder());

        // Keyboard navigation: set cursor on the first/last item of the new page.
        if (selectPosition) {
            const items = this.getDisplayItems();
            if (items.length > 0) {
                const file = selectPosition === 'last' ? items[items.length - 1] : items[0];
                const idx = selectPosition === 'last' ? items.length - 1 : 0;
                this.anchorIdx = idx;
                // Only shift-extend carries the previous page's selection over.
                // Plain navigation moves a cursor, so crossing a page boundary
                // must leave exactly one row selected — otherwise it reports a
                // multi-selection and the detail panel stops following along.
                if (!extend) this.selectedItems.clear();
                this.selectedItems.set(itemKey(file), file);
                this.render();
                this.fireSelectionChange();
            }
        }
    },

    renderPagingBar() {
        if (this.totalFiles <= PAGE_SIZE) return null;

        const totalPages = this.totalPages();
        const bar = document.createElement('div');
        bar.className = 'paging-bar';

        const prevBtn = document.createElement('button');
        prevBtn.className = 'btn btn-sm';
        prevBtn.textContent = '\u2039';
        prevBtn.disabled = this.currentPage === 0;
        prevBtn.addEventListener('click', (e) => {
            e.stopPropagation();
            this.goToPage(this.currentPage - 1);
        });

        const nextBtn = document.createElement('button');
        nextBtn.className = 'btn btn-sm';
        nextBtn.textContent = '\u203A';
        nextBtn.disabled = this.currentPage >= totalPages - 1;
        nextBtn.addEventListener('click', (e) => {
            e.stopPropagation();
            this.goToPage(this.currentPage + 1);
        });

        const info = document.createElement('span');
        info.className = 'paging-info';
        info.textContent = `Page ${this.currentPage + 1} of ${totalPages.toLocaleString()}`;

        const total = document.createElement('span');
        total.className = 'paging-total';
        total.textContent = `(${this.totalFiles.toLocaleString()} files)`;

        bar.appendChild(prevBtn);
        bar.appendChild(info);
        bar.appendChild(nextBtn);
        bar.appendChild(total);

        return bar;
    },

    buildGalleryBadges(cell, file) {
        const marks = Triage.getMarks(file.id);
        const hasDups = file.type !== 'folder' && file.dups > 0 && file.size > 0;
        const isFav = file.type === 'folder' && file.favourite;
        let badges = cell.querySelector('.gallery-badges');
        if (marks.length > 0 || hasDups || isFav) {
            if (!badges) {
                badges = document.createElement('div');
                badges.className = 'gallery-badges';
                cell.appendChild(badges);
            }
            badges.innerHTML = '';
            if (isFav) {
                const fav = document.createElement('span');
                fav.className = 'triage-mark gallery-fav-mark';
                fav.innerHTML = icons.heart;
                badges.appendChild(fav);
            }
            if (hasDups) {
                const dup = document.createElement('span');
                dup.className = 'dup-indicator';
                dup.textContent = `${file.dups} dup${file.dups > 1 ? 's' : ''}`;
                dup.addEventListener('click', (e) => {
                    e.stopPropagation();
                    this.showDuplicateGroup(file.hashStrong || file.hashFast, file.id);
                });
                badges.appendChild(dup);
            }
            marks.forEach(op => {
                const badge = document.createElement('span');
                badge.className = `triage-mark triage-mark-${op}`;
                badge.textContent = op[0].toUpperCase();
                badges.appendChild(badge);
            });
        } else if (badges) {
            badges.remove();
        }
    },

    render() {
        if (this.viewMode === 'gallery') {
            // If the gallery grid already exists with the right items,
            // just update selection classes instead of rebuilding the DOM
            const grid = this.el.querySelector('.file-gallery');
            if (grid && grid.childElementCount > 0 && !this.galleryDirty) {
                const items = this.getDisplayItems();
                grid.querySelectorAll('.gallery-item').forEach((cell, idx) => {
                    const key = cell.dataset.key;
                    cell.classList.toggle('selected', key != null && this.selectedItems.has(key));
                    const file = items[idx];
                    if (file && file.type !== 'folder') this.buildGalleryBadges(cell, file);
                });
                this.scrollSelectedIntoView();
                return;
            }
            this.galleryDirty = false;
            this.renderGallery();
            return;
        }
        const items = this.getDisplayItems();

        const table = document.createElement('table');
        table.className = 'file-table';

        // Build header with sort indicators
        const thead = document.createElement('thead');
        const headerRow = document.createElement('tr');

        // Checkbox header cell
        const thCheck = document.createElement('th');
        thCheck.className = 'col-check';
        const headerCheckbox = document.createElement('input');
        headerCheckbox.type = 'checkbox';
        const selCount = this.selectedItems.size;
        const totalItems = this.totalFiles + (this.currentFolders ? this.currentFolders.length : 0);
        if (selCount === 0) {
            headerCheckbox.checked = false;
            headerCheckbox.indeterminate = false;
        } else if (selCount >= totalItems && totalItems > 0) {
            headerCheckbox.checked = true;
            headerCheckbox.indeterminate = false;
        } else {
            headerCheckbox.checked = false;
            headerCheckbox.indeterminate = true;
        }
        headerCheckbox.addEventListener('click', async (e) => {
            e.stopPropagation();
            if (selCount >= totalItems && totalItems > 0) {
                this.deselectAll();
            } else {
                await this.selectAll();
            }
        });
        thCheck.appendChild(headerCheckbox);
        headerRow.appendChild(thCheck);

        // Icon column header
        const thIcon = document.createElement('th');
        thIcon.style.width = '2rem';
        headerRow.appendChild(thIcon);

        COLUMNS.forEach(col => {
            const th = document.createElement('th');
            if (col.class) th.className = col.class;
            if (col.width) th.style.width = col.width;

            th.textContent = col.label;
            if (this.sortKey === col.key) {
                th.classList.add('sort-active');
                const arrow = document.createElement('span');
                arrow.className = 'sort-indicator';
                arrow.innerHTML = this.sortDir === 1
                    ? '<svg width="8" height="8" viewBox="0 0 8 8"><path d="M4 1L7 6H1z" fill="currentColor"/></svg>'
                    : '<svg width="8" height="8" viewBox="0 0 8 8"><path d="M4 7L1 2h6z" fill="currentColor"/></svg>';
                th.appendChild(arrow);
            }
            th.addEventListener('click', () => this.toggleSort(col.key));
            headerRow.appendChild(th);
        });
        thead.appendChild(headerRow);
        table.appendChild(thead);

        const tbody = document.createElement('tbody');
        items.forEach((file, idx) => {
            const tr = document.createElement('tr');
            const selected = this.isSelected(file);
            if (selected) tr.classList.add('selected');
            if (file.pendingOp) tr.classList.add('pending-op');
            else if (file.stale) tr.classList.add('stale');
            else if (file.missing) tr.classList.add('missing');
            if (file.hidden) tr.classList.add('hidden-item');

            // Checkbox cell
            const tdCheck = document.createElement('td');
            tdCheck.className = 'col-check';
            const cb = document.createElement('input');
            cb.type = 'checkbox';
            cb.checked = selected;
            cb.addEventListener('click', (e) => {
                e.stopPropagation();
                this.toggleItem(file, idx);
                this.fireSelectionChange();
                // Update row highlight + header checkbox without full re-render
                const nowSelected = this.isSelected(file);
                tr.classList.toggle('selected', nowSelected);
                cb.checked = nowSelected;
                this.updateHeaderCheckbox();
            });
            tdCheck.appendChild(cb);
            tr.appendChild(tdCheck);

            let dupHtml = '';
            if (this.dupGroupMode) {
                if (file.id === this.dupGroupSourceId) {
                    dupHtml = '<span class="dup-indicator dup-selected">selected</span>';
                } else {
                    dupHtml = `<span class="dup-indicator" data-dup-file-id="${file.id}">duplicate</span>`;
                }
            } else if (file.dups > 0 && file.size > 0) {
                dupHtml = `<span class="dup-indicator" data-dup-file-id="${file.id}">${file.dups} dup${file.dups > 1 ? 's' : ''}</span>`;
            }
            const staleHtml = file.stale
                ? '<span class="stale-indicator">stale</span>'
                : '';
            const missingHtml = (!file.stale && file.missing)
                ? '<span class="missing-indicator">missing</span>'
                : '';
            const pendingHtml = file.pendingOp
                ? `<span class="pending-indicator">pending ${file.pendingOp}</span>`
                : '';
            const locLabel = this.favouritesMode
                ? file.location
                : (file.locationId && Tree.getLocationLabel(file.locationId)) || file.location;
            const locHtml = locLabel
                ? `<span class="file-location-label">${locLabel}</span>`
                : '';

            // Triage badges
            const marks = Triage.getMarks(file.id);
            const triageHtml = marks.map(op => `<span class="triage-mark triage-mark-${op}">${op[0].toUpperCase()}</span>`).join('');

            const favHtml = (file.type === 'folder' && file.favourite)
                ? `<span class="tree-fav">${icons.heart}</span>`
                : '';

            // Remaining cells via innerHTML on a temp fragment
            const tempRow = document.createElement('tr');
            tempRow.innerHTML = `
                <td class="col-icon">${fileIcon(file)}</td>
                <td><span class="file-name">${file.name}${favHtml}${dupHtml}${staleHtml}${missingHtml}${pendingHtml}${triageHtml}</span>${locHtml}</td>
                <td class="col-type">${file.typeLow || ''}</td>
                <td class="col-size">${formatSize(file.size)}</td>
                <td class="col-date">${formatDate(file.date)}</td>
            `;
            while (tempRow.firstChild) {
                tr.appendChild(tempRow.firstChild);
            }

            // Dup indicator click — show duplicate group or update selected
            const dupEl = tr.querySelector('.dup-indicator:not(.dup-selected)');
            if (dupEl) {
                dupEl.addEventListener('click', (e) => {
                    e.stopPropagation();
                    if (this.dupGroupMode) {
                        // Already in dup group view — change selected and show detail
                        this.dupGroupSourceId = file.id;
                        this.selectOnly(file, idx);
                        this.fireSelectionChange();
                        this.render();
                    } else {
                        this.showDuplicateGroup(file.hashStrong || file.hashFast, file.id);
                    }
                });
            }

            tr.addEventListener('click', (e) => {
                e.stopPropagation();
                // Don't handle if checkbox was clicked (already handled)
                if (e.target.tagName === 'INPUT') return;

                if (e.shiftKey && this.anchorIdx !== null) {
                    // Range select
                    this.selectedItems.clear();
                    this.selectRange(this.anchorIdx, idx);
                    this.fireSelectionChange();
                    this.render();
                } else if (e.ctrlKey || e.metaKey) {
                    // Toggle item
                    this.toggleItem(file, idx);
                    this.fireSelectionChange();
                    this.render();
                } else {
                    // Single select
                    this.selectOnly(file, idx);
                    if (this.dupGroupMode) this.dupGroupSourceId = file.id;
                    this.render();
                    this.fireSelectionChange();
                }
            });

            if (file.type === 'folder') {
                tr.addEventListener('dblclick', () => {
                    if (this.onFolderOpen) this.onFolderOpen(file);
                });
            }

            this.makeDraggable(tr, file, idx);
            tbody.appendChild(tr);
        });

        table.appendChild(tbody);
        this.el.innerHTML = '';
        this.el.appendChild(table);

        // Paging bar sits outside the scroll container, fixed at bottom of panel
        const existing = this.el.parentElement.querySelector('.paging-bar');
        if (existing) existing.remove();
        const pagingBar = this.renderPagingBar();
        if (pagingBar) this.el.parentElement.appendChild(pagingBar);

        this.scrollSelectedIntoView();
    },

    renderGallery() {
        galleryLoader.reset();
        const items = this.getDisplayItems();
        const grid = document.createElement('div');
        grid.className = 'file-gallery';
        const loc = this.currentFolder ? Tree.getLocation(this.currentFolder) : null;
        const locationOnline = !loc || loc.online !== false;

        items.forEach((file, idx) => {
            const cell = document.createElement('div');
            cell.className = 'gallery-item';
            cell.dataset.key = itemKey(file);
            if (this.isSelected(file)) cell.classList.add('selected');
            if (file.stale) cell.classList.add('stale');
            if (file.pendingOp) cell.classList.add('pending-op');

            if (file.type === 'folder') {
                cell.classList.add('gallery-folder');
                cell.innerHTML = icons.folder;
                const label = document.createElement('div');
                label.className = 'gallery-name';
                label.textContent = file.name;
                cell.appendChild(label);

                cell.addEventListener('dblclick', () => {
                    if (this.onFolderOpen) this.onFolderOpen(file);
                });
            } else if (file.typeHigh === 'image') {
                if (locationOnline) {
                    const img = document.createElement('img');
                    const token = localStorage.getItem('fh-token');
                    let src = `/api/files/${file.id}/content`;
                    if (token) src += `?token=${encodeURIComponent(token)}`;
                    img.alt = file.name;
                    galleryLoader.enqueue(img, src);
                    cell.appendChild(img);
                } else {
                    cell.classList.add('gallery-folder');
                    cell.innerHTML = icons.image || icons.file;
                }

                const label = document.createElement('div');
                label.className = 'gallery-name';
                label.textContent = file.name;
                cell.appendChild(label);
            } else {
                // Non-image file: show icon + name
                cell.classList.add('gallery-folder');
                cell.innerHTML = fileIcon(file);
                const label = document.createElement('div');
                label.className = 'gallery-name';
                label.textContent = file.name;
                cell.appendChild(label);
            }

            this.buildGalleryBadges(cell, file);

            cell.addEventListener('click', (e) => {
                e.stopPropagation();
                if (e.ctrlKey || e.metaKey) {
                    this.toggleItem(file, idx);
                    this.fireSelectionChange();
                    this.render();
                } else if (e.shiftKey && this.anchorIdx !== null) {
                    this.selectedItems.clear();
                    this.selectRange(this.anchorIdx, idx);
                    this.fireSelectionChange();
                    this.render();
                } else {
                    this.selectOnly(file, idx);
                    this.render();
                    this.fireSelectionChange();
                }
            });

            this.makeDraggable(cell, file, idx);
            grid.appendChild(cell);
        });

        this.el.innerHTML = '';
        this.el.appendChild(grid);

        const existing = this.el.parentElement.querySelector('.paging-bar');
        if (existing) existing.remove();
        const pagingBar = this.renderPagingBar();
        if (pagingBar) this.el.parentElement.appendChild(pagingBar);

        this.scrollSelectedIntoView();
    },

    makeDraggable(el, file, idx) {
        el.draggable = true;
        el.addEventListener('dragstart', (e) => {
            // If the dragged item isn't selected, select it first
            if (!this.isSelected(file)) {
                this.selectOnly(file, idx);
                this.fireSelectionChange();
            }
            const ids = [];
            const folderIds = [];
            for (const item of this.selectedItems.values()) {
                if (item.type === 'folder') {
                    const numId = String(item.id).replace(/^fld-/, '');
                    folderIds.push(parseInt(numId, 10));
                } else {
                    ids.push(item.id);
                }
            }
            const payload = JSON.stringify({ file_ids: ids, folder_ids: folderIds });
            e.dataTransfer.setData('application/x-filehunter-move', payload);
            e.dataTransfer.effectAllowed = 'move';

            const count = ids.length + folderIds.length;
            const label = `${count} file${count !== 1 ? 's' : ''}`;
            const ghost = document.createElement('div');
            ghost.textContent = label;
            ghost.style.cssText = 'position:absolute;top:-999px;padding:4px 8px;background:var(--color-surface);border:1px solid var(--color-primary);border-radius:4px;font-size:12px;color:var(--color-text);';
            document.body.appendChild(ghost);
            e.dataTransfer.setDragImage(ghost, 0, 0);
            requestAnimationFrame(() => ghost.remove());
        });
    },

    setFolderFavourite(nodeId, favourite) {
        if (!this.currentFolders) return;
        const folder = this.currentFolders.find(f => f.id === nodeId);
        if (!folder) return;
        folder.favourite = favourite;
        this.render();
    },

    async refreshDupCounts() {
        if (!this.currentItems || this.currentItems.length === 0) return;
        const hashes = [...new Set(
            this.currentItems
                .map(f => f.hashStrong || f.hashFast)
                .filter(Boolean)
        )];
        if (hashes.length === 0) return;

        let res;
        try {
            res = await API.post('/api/files/dup-counts', { hashes });
        } catch (_) {
            return;
        }
        if (!res.ok) return;

        const counts = res.data.counts;
        let changed = false;

        for (const item of this.currentItems) {
            const h = item.hashStrong || item.hashFast;
            if (!h) continue;
            const newDups = counts[h] || 0;
            if (item.dups !== newDups) {
                item.dups = newDups;
                changed = true;
            }
        }

        if (!changed) return;

        // Update dup indicators in-place without re-rendering the table
        const rows = this.el.querySelectorAll('tbody tr');
        const folders = this.currentFolders ? this.currentFolders.length : 0;
        for (let i = 0; i < this.currentItems.length; i++) {
            const item = this.currentItems[i];
            const row = rows[folders + i];
            if (!row) continue;

            const nameCell = row.querySelector('.file-name');
            if (!nameCell) continue;
            const existing = nameCell.querySelector('.dup-indicator');

            if (item.dups > 0 && item.size > 0) {
                const text = `${item.dups} dup${item.dups > 1 ? 's' : ''}`;
                if (existing) {
                    existing.textContent = text;
                } else {
                    const span = document.createElement('span');
                    span.className = 'dup-indicator';
                    span.dataset.dupFileId = item.id;
                    span.textContent = text;
                    span.addEventListener('click', (e) => {
                        e.stopPropagation();
                        this.showDuplicateGroup(item.hashStrong || item.hashFast, item.id);
                    });
                    nameCell.appendChild(span);
                }
            } else if (existing) {
                existing.remove();
            }
        }
    },
};

export default FileList;
