import API from '../api.js';
import icons from '../icons.js';
import Keyboard from '../keyboard.js';

const PAGE_SIZE = 120;

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
    _anchorIdx: null,           // for Shift+click range select
    onSelect: null,
    onFolderOpen: null,
    onDeselect: null,
    onMultiSelect: null,
    onBreadcrumbNav: null,
    currentBreadcrumb: null,
    filterText: '',
    sortKey: 'name',
    sortDir: 1,  // 1 = ascending, -1 = descending
    _filterTimer: null,
    _searchMode: false,
    _searchParams: null,
    _dupGroupMode: false,
    _ac: null,
    pendingFocusFile: null,

    init(onSelect, onFolderOpen, onDeselect, onMultiSelect) {
        this.el = document.getElementById('file-content');
        this.breadcrumbEl = document.getElementById('file-breadcrumb');
        this.filterEl = document.getElementById('file-filter');
        this.onSelect = onSelect;
        this.onFolderOpen = onFolderOpen;
        this.onDeselect = onDeselect;
        this.onMultiSelect = onMultiSelect;

        this.filterEl.addEventListener('input', () => {
            if (this._searchMode) return;  // search has its own filters
            clearTimeout(this._filterTimer);
            this._filterTimer = setTimeout(() => {
                this.filterText = this.filterEl.value;
                this.currentPage = 0;
                this._clearSelection();
                this._fetchFolder();
            }, 300);
        });

        this.el.addEventListener('click', (e) => {
            // Only deselect when clicking empty space (not rows/checkboxes)
            if (e.target === this.el || e.target.closest('.file-table') === null) {
                this._clearSelection();
                this._fireSelectionChange();
                this.render();
            }
        });

        Keyboard.registerPanel('filelist', (e) => this.handleKey(e));

        this.renderEmpty();
    },

    // ── Selection helpers ──

    getSelection() {
        return Array.from(this.selectedItems.values());
    },

    getSelectionCount() {
        return this.selectedItems.size;
    },

    _clearSelection() {
        this.selectedItems.clear();
        this._anchorIdx = null;
    },

    _selectOnly(item, idx) {
        this.selectedItems.clear();
        this.selectedItems.set(itemKey(item), item);
        this._anchorIdx = idx !== undefined ? idx : this._indexOfItem(item);
    },

    _toggleItem(item, idx) {
        const key = itemKey(item);
        if (this.selectedItems.has(key)) {
            this.selectedItems.delete(key);
        } else {
            this.selectedItems.set(key, item);
        }
        this._anchorIdx = idx !== undefined ? idx : this._indexOfItem(item);
    },

    _selectRange(fromIdx, toIdx) {
        const items = this._getDisplayItems();
        const lo = Math.min(fromIdx, toIdx);
        const hi = Math.max(fromIdx, toIdx);
        for (let i = lo; i <= hi; i++) {
            if (i >= 0 && i < items.length) {
                this.selectedItems.set(itemKey(items[i]), items[i]);
            }
        }
    },

    _indexOfItem(item) {
        const items = this._getDisplayItems();
        const key = itemKey(item);
        return items.findIndex(f => itemKey(f) === key);
    },

    _isSelected(item) {
        return this.selectedItems.has(itemKey(item));
    },

    _fireSelectionChange() {
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

    _selectAll() {
        const items = this._getDisplayItems();
        items.forEach(item => {
            this.selectedItems.set(itemKey(item), item);
        });
        this._fireSelectionChange();
        this.render();
    },

    _deselectAll() {
        this._clearSelection();
        this._fireSelectionChange();
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
            this._clearSelection();
        }
    },

    // ── Display items ──

    _getDisplayItems() {
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

    _totalPages() {
        return Math.max(1, Math.ceil(this.totalFiles / PAGE_SIZE));
    },

    handleKey(e) {
        const items = this._getDisplayItems();
        if (!items || items.length === 0) return;

        // Find current cursor position based on last single-selected or anchor
        let curIdx = -1;
        if (this.selectedItems.size > 0) {
            // Use the last item in selection order or anchor
            const sel = this.getSelection();
            const lastItem = sel[sel.length - 1];
            curIdx = this._indexOfItem(lastItem);
        }

        let newIdx = curIdx;
        const totalPages = this._totalPages();

        switch (e.key) {
            case 'ArrowDown':
                e.preventDefault();
                if (curIdx === items.length - 1 && this.currentPage < totalPages - 1) {
                    this._goToPage(this.currentPage + 1, 'first');
                    return;
                }
                newIdx = curIdx < items.length - 1 ? curIdx + 1 : curIdx;
                if (curIdx === -1) newIdx = 0;

                if (e.shiftKey) {
                    // Extend selection
                    const anchor = this._anchorIdx !== null ? this._anchorIdx : curIdx;
                    this.selectedItems.clear();
                    this._selectRange(anchor, newIdx);
                    this._anchorIdx = anchor;
                    this._fireSelectionChange();
                    this.render();
                    this._scrollSelectedIntoView();
                    return;
                }
                break;
            case 'ArrowUp':
                e.preventDefault();
                if (curIdx === 0 && this.currentPage > 0) {
                    this._goToPage(this.currentPage - 1, 'last');
                    return;
                }
                newIdx = curIdx > 0 ? curIdx - 1 : 0;
                if (curIdx === -1) newIdx = 0;

                if (e.shiftKey) {
                    const anchor = this._anchorIdx !== null ? this._anchorIdx : curIdx;
                    this.selectedItems.clear();
                    this._selectRange(anchor, newIdx);
                    this._anchorIdx = anchor;
                    this._fireSelectionChange();
                    this.render();
                    this._scrollSelectedIntoView();
                    return;
                }
                break;
            case 'Home':
                e.preventDefault();
                if (this.currentPage !== 0) {
                    this._goToPage(0, 'first');
                    return;
                }
                newIdx = 0;
                break;
            case 'End':
                e.preventDefault();
                if (this.currentPage !== totalPages - 1) {
                    this._goToPage(totalPages - 1, 'last');
                    return;
                }
                newIdx = items.length - 1;
                break;
            case 'PageDown':
                e.preventDefault();
                if (this.currentPage < totalPages - 1) {
                    this._goToPage(this.currentPage + 1, 'first');
                    return;
                }
                newIdx = items.length - 1;
                break;
            case 'PageUp':
                e.preventDefault();
                if (this.currentPage > 0) {
                    this._goToPage(this.currentPage - 1, 'first');
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
            default:
                return;
        }

        const file = items[newIdx];
        if (!file) return;
        this._selectOnly(file, newIdx);
        this.render();
        this._fireSelectionChange();
    },

    _scrollSelectedIntoView() {
        const el = this.el.querySelector('tr.selected:last-child') || this.el.querySelector('tr.selected');
        if (el) el.scrollIntoView({ block: 'nearest', behavior: 'instant' });
    },

    renderEmpty() {
        this.currentItems = null;
        this.currentFolders = null;
        this.currentBreadcrumb = null;
        this.totalFiles = 0;
        this.currentPage = 0;
        this._searchMode = false;
        this._searchParams = null;
        this._dupGroupMode = false;
        this._clearSelection();
        this.breadcrumbEl.innerHTML = '';
        this.el.innerHTML = '<div class="panel-body" style="padding: 1rem; color: var(--color-text-placeholder);">Select a folder to view files.</div>';
    },

    showLoading() {
        this.el.innerHTML = '<div class="detail-loading"><div class="detail-spinner"></div><span>Searching\u2026</span></div>';
    },

    _toggleSort(key) {
        if (this.sortKey === key) {
            this.sortDir *= -1;
        } else {
            this.sortKey = key;
            this.sortDir = 1;
        }
        this.currentPage = 0;
        this._clearSelection();
        this._refetch();
    },

    _sortDirStr() {
        return this.sortDir === 1 ? 'asc' : 'desc';
    },

    async _fetchFolder(focusFileId) {
        if (!this.currentFolder) return;
        if (this._ac) this._ac.abort();
        this._ac = new AbortController();
        const signal = this._ac.signal;
        const params = new URLSearchParams({
            folder_id: this.currentFolder,
            page: this.currentPage,
            sort: this.sortKey,
            sortDir: this._sortDirStr(),
        });
        if (this.filterText) params.set('filter', this.filterText);
        if (focusFileId) params.set('focusFile', focusFileId);

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
        this._renderContent();

        if (focusFileId && res.ok && res.data.focusFileId) {
            const foldersLen = this.currentFolders ? this.currentFolders.length : 0;
            const idx = this.currentItems.findIndex(f => f.id === res.data.focusFileId);
            if (idx >= 0) {
                this._selectOnly(this.currentItems[idx], foldersLen + idx);
                this.render();
                this._fireSelectionChange();
                this._scrollSelectedIntoView();
            }
        }
    },

    async _fetchSearch() {
        if (!this._searchParams) return;
        if (this._ac) this._ac.abort();
        this._ac = new AbortController();
        const signal = this._ac.signal;
        const params = new URLSearchParams(this._searchParams);
        params.set('page', this.currentPage);
        params.set('sort', this.sortKey);
        params.set('sortDir', this._sortDirStr());

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
        } else {
            this.currentItems = [];
            this.currentFolders = null;
            this.totalFiles = 0;
        }
        this._renderContent();
    },

    _refetch() {
        if (this._searchMode) {
            this._fetchSearch();
        } else {
            this._fetchFolder();
        }
    },

    _renderBreadcrumb() {
        this.breadcrumbEl.innerHTML = '';
        if (!this.currentBreadcrumb || this.currentBreadcrumb.length === 0 || this._searchMode) return;
        this.currentBreadcrumb.forEach((entry, i) => {
            if (i > 0) {
                const sep = document.createElement('span');
                sep.className = 'breadcrumb-sep';
                sep.textContent = '/';
                this.breadcrumbEl.appendChild(sep);
            }
            const seg = document.createElement('span');
            seg.textContent = entry.name;
            if (i < this.currentBreadcrumb.length - 1) {
                seg.className = 'breadcrumb-segment';
                seg.addEventListener('click', () => {
                    if (this.onBreadcrumbNav) this.onBreadcrumbNav(entry.nodeId);
                });
            } else {
                seg.style.cursor = 'default';
                seg.style.color = 'var(--color-text)';
            }
            this.breadcrumbEl.appendChild(seg);
        });
    },

    _renderContent() {
        this._renderBreadcrumb();
        const items = this._getDisplayItems();
        if (items.length === 0 && this.totalFiles === 0) {
            const msg = this._searchMode ? 'No results found.' : 'Empty folder.';
            this.el.innerHTML = `<div class="panel-body" style="padding: 1rem; color: var(--color-text-placeholder);">${msg}</div>`;
            return;
        }
        this.render();
    },

    async showFolder(folderId) {
        const focusFileId = this.pendingFocusFile;
        this.pendingFocusFile = null;

        this.currentFolder = folderId;
        this._clearSelection();
        this.filterText = '';
        this.filterEl.value = '';
        this.sortKey = 'name';
        this.sortDir = 1;
        this.currentPage = 0;
        this._searchMode = false;
        this._searchParams = null;
        this._dupGroupMode = false;

        await this._fetchFolder(focusFileId);
    },

    showSingleFile(file) {
        this.currentFolder = null;
        this.currentBreadcrumb = null;
        this.filterText = '';
        this.filterEl.value = '';
        this.sortKey = 'name';
        this.sortDir = 1;
        this.currentPage = 0;
        this._searchMode = false;
        this._searchParams = null;
        this._dupGroupMode = false;
        this.currentItems = [file];
        this.currentFolders = null;
        this.totalFiles = 1;
        this._clearSelection();
        this._selectOnly(file, 0);
        this._renderContent();
    },

    async showDuplicateGroup(hash) {
        this.currentFolder = null;
        this.currentBreadcrumb = null;
        this._clearSelection();
        this.filterText = '';
        this.filterEl.value = '';
        this.sortKey = 'name';
        this.sortDir = 1;
        this.currentPage = 0;
        this._searchMode = true;
        this._searchParams = { hash };
        this._dupGroupMode = true;

        await this._fetchSearch();
    },

    showSearchResults(data, searchParams) {
        this.currentFolder = null;
        this.currentBreadcrumb = null;
        this._clearSelection();
        this.filterText = '';
        this.filterEl.value = '';
        this.sortKey = 'name';
        this.sortDir = 1;
        this.currentPage = data.page;
        this._searchMode = true;
        this._searchParams = searchParams;
        this._dupGroupMode = false;

        this.currentItems = data.items;
        this.currentFolders = data.folders && data.folders.length ? data.folders : null;
        this.totalFiles = data.total;

        this._renderContent();
    },

    async _goToPage(n, selectPosition) {
        const totalPages = this._totalPages();
        this.currentPage = Math.max(0, Math.min(n, totalPages - 1));
        this._clearSelection();

        await (this._searchMode ? this._fetchSearch() : this._fetchFolder());

        // Select first or last item after page load
        if (selectPosition) {
            const items = this._getDisplayItems();
            if (items.length > 0) {
                const file = selectPosition === 'last' ? items[items.length - 1] : items[0];
                this._selectOnly(file, selectPosition === 'last' ? items.length - 1 : 0);
                this.render();
                this._fireSelectionChange();
            }
        }
    },

    _renderPagingBar() {
        if (this.totalFiles <= PAGE_SIZE) return null;

        const totalPages = this._totalPages();
        const bar = document.createElement('div');
        bar.className = 'paging-bar';

        const prevBtn = document.createElement('button');
        prevBtn.className = 'btn btn-sm';
        prevBtn.textContent = '\u2039';
        prevBtn.disabled = this.currentPage === 0;
        prevBtn.addEventListener('click', (e) => {
            e.stopPropagation();
            this._goToPage(this.currentPage - 1);
        });

        const nextBtn = document.createElement('button');
        nextBtn.className = 'btn btn-sm';
        nextBtn.textContent = '\u203A';
        nextBtn.disabled = this.currentPage >= totalPages - 1;
        nextBtn.addEventListener('click', (e) => {
            e.stopPropagation();
            this._goToPage(this.currentPage + 1);
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

    render() {
        const items = this._getDisplayItems();

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
        if (selCount === 0) {
            headerCheckbox.checked = false;
            headerCheckbox.indeterminate = false;
        } else if (selCount === items.length && items.length > 0) {
            headerCheckbox.checked = true;
            headerCheckbox.indeterminate = false;
        } else {
            headerCheckbox.checked = false;
            headerCheckbox.indeterminate = true;
        }
        headerCheckbox.addEventListener('click', (e) => {
            e.stopPropagation();
            if (selCount === items.length && items.length > 0) {
                this._deselectAll();
            } else {
                this._selectAll();
            }
        });
        thCheck.appendChild(headerCheckbox);
        headerRow.appendChild(thCheck);

        // Icon column header
        headerRow.innerHTML += '<th style="width: 2rem;"></th>';

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
            th.addEventListener('click', () => this._toggleSort(col.key));
            headerRow.appendChild(th);
        });
        thead.appendChild(headerRow);
        table.appendChild(thead);

        const tbody = document.createElement('tbody');
        items.forEach((file, idx) => {
            const tr = document.createElement('tr');
            const selected = this._isSelected(file);
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
                this._toggleItem(file, idx);
                this._fireSelectionChange();
                this.render();
            });
            tdCheck.appendChild(cb);
            tr.appendChild(tdCheck);

            const dupHtml = file.dups > 0 && file.size > 0
                ? `<span class="dup-indicator" data-dup-file-id="${file.id}">${file.dups} dup${file.dups > 1 ? 's' : ''}</span>`
                : '';
            const staleHtml = file.stale
                ? '<span class="stale-indicator">stale</span>'
                : '';
            const missingHtml = (!file.stale && file.missing)
                ? '<span class="missing-indicator">missing</span>'
                : '';
            const pendingHtml = file.pendingOp
                ? `<span class="pending-indicator">pending ${file.pendingOp}</span>`
                : '';
            const locHtml = file.location
                ? `<span class="file-location-label">${file.location}</span>`
                : '';

            // Remaining cells via innerHTML on a temp fragment
            const tempRow = document.createElement('tr');
            tempRow.innerHTML = `
                <td class="col-icon">${fileIcon(file)}</td>
                <td><span class="file-name">${file.name}${dupHtml}${staleHtml}${missingHtml}${pendingHtml}</span>${locHtml}</td>
                <td class="col-type">${file.typeLow || ''}</td>
                <td class="col-size">${formatSize(file.size)}</td>
                <td class="col-date">${formatDate(file.date)}</td>
            `;
            while (tempRow.firstChild) {
                tr.appendChild(tempRow.firstChild);
            }

            // Dup indicator click — show duplicate group
            const dupEl = tr.querySelector('.dup-indicator');
            if (dupEl) {
                dupEl.addEventListener('click', (e) => {
                    e.stopPropagation();
                    this.showDuplicateGroup(file.hashStrong || file.hashFast);
                });
            }

            tr.addEventListener('click', (e) => {
                e.stopPropagation();
                // Don't handle if checkbox was clicked (already handled)
                if (e.target.tagName === 'INPUT') return;

                if (e.shiftKey && this._anchorIdx !== null) {
                    // Range select
                    this.selectedItems.clear();
                    this._selectRange(this._anchorIdx, idx);
                    this._fireSelectionChange();
                    this.render();
                } else if (e.ctrlKey || e.metaKey) {
                    // Toggle item
                    this._toggleItem(file, idx);
                    this._fireSelectionChange();
                    this.render();
                } else {
                    // Single select
                    this._selectOnly(file, idx);
                    this.render();
                    this._fireSelectionChange();
                }
            });

            if (file.type === 'folder') {
                tr.addEventListener('dblclick', () => {
                    if (this.onFolderOpen) this.onFolderOpen(file);
                });
            }

            tbody.appendChild(tr);
        });

        table.appendChild(tbody);
        this.el.innerHTML = '';
        this.el.appendChild(table);

        const pagingBar = this._renderPagingBar();
        if (pagingBar) this.el.appendChild(pagingBar);

        this._scrollSelectedIntoView();
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
                        this.showDuplicateGroup(item.hashStrong || item.hashFast);
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
