import API from '../api.js';
import ConfirmModal from './confirm.js';

function _authUrl(url) {
    const token = localStorage.getItem('fh-token');
    return token ? `${url}${url.includes('?') ? '&' : '?'}token=${encodeURIComponent(token)}` : url;
}

function _authHeaders() {
    const h = {};
    const token = localStorage.getItem('fh-token');
    if (token) h['Authorization'] = `Bearer ${token}`;
    return h;
}

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
    return d.toLocaleString();
}

const zoomIcon = `<svg viewBox="0 0 20 20" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="8.5" cy="8.5" r="5.5"/><line x1="13" y1="13" x2="18" y2="18"/><line x1="8.5" y1="6" x2="8.5" y2="11"/><line x1="6" y1="8.5" x2="11" y2="8.5"/></svg>`;

const Detail = {
    el: null,
    _lastDetail: null,
    _renderGen: 0,
    _ac: null,
    _locationActivityFn: null,
    _currentLocationNode: null,
    _previewModal: null,
    _slideshowTotal: 0,
    _slideshowOffset: 0,
    _slideshowWindow: [],
    _slideshowWindowStart: 0,
    _slideshowParams: null,
    _slideshowCache: {},
    _slideshowTimer: null,
    _slideshowPlaying: false,
    _slideshowNavGen: 0,
    _slideshowDeleteSet: new Set(),
    _slideshowConsolidateSet: new Set(),
    _slideshowTagSet: new Set(),
    slideshowTriage: null,
    onNavigateToFile: null,
    onNavigateToFolder: null,
    onShowDuplicates: null,

    init(opts) {
        this.el = document.getElementById('detail-content');
        if (opts && opts.onNavigateToFile) this.onNavigateToFile = opts.onNavigateToFile;
        if (opts && opts.onNavigateToFolder) this.onNavigateToFolder = opts.onNavigateToFolder;
        if (opts && opts.onShowDuplicates) this.onShowDuplicates = opts.onShowDuplicates;
        this._initPreviewModal();
        this.renderDashboard();
    },

    updateFromScanProgress(msg) {
        if (!this.el) return;
        const locId = msg.locationId;

        // Dashboard: update global counters
        const gfc = this.el.querySelector('[data-stat="globalFileCount"]');
        if (gfc && msg.globalFileCount !== undefined) {
            gfc.textContent = msg.globalFileCount.toLocaleString();
        }
        const gts = this.el.querySelector('[data-stat="globalTotalSize"]');
        if (gts && msg.globalTotalSize !== undefined) {
            gts.textContent = formatSize(msg.globalTotalSize);
        }
        const gdup = this.el.querySelector('[data-stat="globalDuplicates"]');
        if (gdup && msg.globalDuplicateCount !== undefined) {
            gdup.textContent = msg.globalDuplicateCount.toLocaleString();
        }
        const gtb = this.el.querySelector('[data-stat="globalTypeBreakdown"]');
        if (gtb && msg.globalTypeBreakdown) {
            gtb.innerHTML = '<h3>Files by Type</h3>' + msg.globalTypeBreakdown.map(t =>
                `<div class="detail-field">
                    <span class="label">${t.type || 'other'}</span>
                    <span class="value">${t.count.toLocaleString()}</span>
                </div>`
            ).join('');
        }

        // Location / folder: update if viewing the scanning location
        const isViewingLocation = this._currentLocationNode &&
            String(this._currentLocationNode.id).replace('loc-', '') === String(locId);
        if (!isViewingLocation) return;

        const fc = this.el.querySelector('[data-stat="fileCount"]');
        if (fc && msg.fileCount !== undefined) {
            fc.textContent = msg.fileCount.toLocaleString();
        }
        const flc = this.el.querySelector('[data-stat="folderCount"]');
        if (flc && msg.folderCount !== undefined) {
            flc.textContent = msg.folderCount.toLocaleString();
        }
        const ts = this.el.querySelector('[data-stat="totalSize"]');
        if (ts && msg.totalSize !== undefined) {
            ts.textContent = formatSize(msg.totalSize);
        }
        const dup = this.el.querySelector('[data-stat="duplicates"]');
        if (dup && msg.duplicateCount !== undefined) {
            dup.textContent = msg.duplicateCount.toLocaleString();
        }
        const tb = this.el.querySelector('[data-stat="typeBreakdown"]');
        if (tb && msg.typeBreakdown) {
            tb.innerHTML = '<h3>File Types</h3>' + msg.typeBreakdown.map(t =>
                `<div class="detail-field">
                    <span class="label">${t.type || 'other'}</span>
                    <span class="value">${t.count.toLocaleString()}</span>
                </div>`
            ).join('');
        }
    },

    async refreshStats() {
        if (!this.el) return;

        // Dashboard view — patch global counters from /api/stats
        const gfc = this.el.querySelector('[data-stat="globalFileCount"]');
        if (gfc) {
            const res = await API.get('/api/stats');
            if (res.ok) {
                const s = res.data;
                gfc.textContent = s.totalFiles.toLocaleString();
                const gloc = this.el.querySelector('[data-stat="globalLocations"]');
                if (gloc) gloc.textContent = s.totalLocations;
                const gts = this.el.querySelector('[data-stat="globalTotalSize"]');
                if (gts) gts.textContent = s.totalSizeFormatted;
                const gdup = this.el.querySelector('[data-stat="globalDuplicates"]');
                if (gdup) gdup.textContent = s.duplicateFiles.toLocaleString();
                const gtb = this.el.querySelector('[data-stat="globalTypeBreakdown"]');
                if (gtb && s.typeBreakdown) {
                    gtb.innerHTML = '<h3>Files by Type</h3>' + s.typeBreakdown.map(t =>
                        `<div class="detail-field">
                            <span class="label">${t.type || 'other'}</span>
                            <span class="value">${t.count.toLocaleString()}</span>
                        </div>`
                    ).join('');
                }
            }
            return;
        }

        // Location view — patch location counters
        if (!this._currentLocationNode) return;
        const locId = String(this._currentLocationNode.id).replace('loc-', '');
        const res = await API.get(`/api/locations/${locId}/stats`);
        if (!res.ok) return;
        const s = res.data;

        const fc = this.el.querySelector('[data-stat="fileCount"]');
        if (fc) fc.textContent = s.fileCount.toLocaleString();
        const flc = this.el.querySelector('[data-stat="folderCount"]');
        if (flc) flc.textContent = s.folderCount.toLocaleString();
        const ts = this.el.querySelector('[data-stat="totalSize"]');
        if (ts) ts.textContent = s.totalSizeFormatted;
        const dup = this.el.querySelector('[data-stat="duplicates"]');
        if (dup) dup.textContent = s.duplicateFiles.toLocaleString();
        const ls = this.el.querySelector('[data-stat="lastScanned"]');
        if (ls) ls.textContent = s.dateLastScanned ? _timeAgo(s.dateLastScanned) + (s.lastScanStatus && s.lastScanStatus !== 'completed' ? ' (' + s.lastScanStatus + ')' : '') : 'Never';
        const tb = this.el.querySelector('[data-stat="typeBreakdown"]');
        if (tb && s.typeBreakdown) {
            tb.innerHTML = '<h3>File Types</h3>' + s.typeBreakdown.map(t =>
                `<div class="detail-field">
                    <span class="label">${t.type || 'other'}</span>
                    <span class="value">${t.count.toLocaleString()}</span>
                </div>`
            ).join('');
        }
    },

    _abortPrevious() {
        if (this._ac) this._ac.abort();
        this._ac = new AbortController();
        return this._ac.signal;
    },

    _initPreviewModal() {
        const overlay = document.getElementById('preview-modal');
        const content = document.getElementById('preview-modal-content');
        const title = document.getElementById('preview-modal-title');
        const closeBtn = document.getElementById('preview-modal-close');
        const downloadBtn = document.getElementById('preview-modal-download');
        const fullscreenBtn = document.getElementById('preview-modal-fullscreen');
        const prevBtn = document.getElementById('preview-modal-prev');
        const nextBtn = document.getElementById('preview-modal-next');
        const counter = document.getElementById('preview-modal-counter');
        const autoplayWrap = document.getElementById('preview-modal-autoplay');
        const playBtn = document.getElementById('preview-modal-play');
        const speedSelect = document.getElementById('preview-modal-speed');
        const dialog = document.querySelector('.preview-modal-dialog');
        const markBadge = document.getElementById('preview-modal-mark');
        const triageCounter = document.getElementById('preview-modal-triage');
        this._previewModal = { overlay, content, title, downloadBtn, fullscreenBtn, prevBtn, nextBtn, counter, autoplayWrap, playBtn, speedSelect, dialog, markBadge, triageCounter };

        closeBtn.addEventListener('click', () => this._closePreviewModal());
        downloadBtn.addEventListener('click', () => this._downloadPreviewFile());
        fullscreenBtn.addEventListener('click', () => this._toggleFullscreen());
        prevBtn.addEventListener('click', () => this._slideshowNav(-1));
        nextBtn.addEventListener('click', () => this._slideshowNav(1));
        playBtn.addEventListener('click', () => this._toggleAutoplay());
        const savedSpeed = localStorage.getItem('slideshowSpeed');
        if (savedSpeed) speedSelect.value = savedSpeed;
        speedSelect.addEventListener('change', () => {
            localStorage.setItem('slideshowSpeed', speedSelect.value);
            if (this._slideshowPlaying) {
                this._stopAutoplay();
                this._startAutoplay();
            }
        });
        overlay.addEventListener('click', (e) => {
            if (e.target === overlay) this._closePreviewModal();
        });
        document.addEventListener('fullscreenchange', () => {
            const isFs = document.fullscreenElement === dialog;
            fullscreenBtn.querySelector('.fs-expand').classList.toggle('hidden', isFs);
            fullscreenBtn.querySelector('.fs-compress').classList.toggle('hidden', !isFs);
        });
        document.addEventListener('keydown', (e) => {
            if (overlay.classList.contains('hidden')) return;
            if (e.key === 'Escape') {
                if (document.fullscreenElement) return; // browser handles fullscreen exit
                this._closePreviewModal();
            } else if (this._slideshowTotal > 0) {
                if (e.key === 'ArrowLeft') { this._stopAutoplay(); this._slideshowNav(-1); }
                else if (e.key === 'ArrowRight') { this._stopAutoplay(); this._slideshowNav(1); }
                else if (e.key === ' ') { e.preventDefault(); this._toggleAutoplay(); }
                else if (e.key === 'd') { this._slideshowToggleMark('delete'); }
                else if (e.key === 'c') { this._slideshowToggleMark('consolidate'); }
                else if (e.key === 't') { this._slideshowToggleMark('tag'); }
            }
        });
    },

    _openPreviewModal(detail) {
        const m = this._previewModal;
        const type = (detail.typeHigh || '').toLowerCase();
        const url = _authUrl(`/api/files/${detail.id}/content`);
        m.title.textContent = detail.name;
        m._fileId = detail.id;
        m._fileName = detail.name;

        if (type === 'image') {
            m.content.innerHTML = `<img src="${url}" alt="${detail.name}">`;
        } else if (type === 'video') {
            m.content.innerHTML = `<video src="${url}" controls autoplay></video>`;
        } else if (type === 'audio') {
            m.content.innerHTML = `<audio src="${url}" controls autoplay></audio>`;
        } else if (type === 'document' && (detail.typeLow || '').toLowerCase() === 'pdf') {
            m.content.innerHTML = `<iframe src="${url}" title="${detail.name}"></iframe>`;
        } else if (type === 'text') {
            m.content.innerHTML = `<pre>Loading...</pre>`;
            fetch(url, { headers: _authHeaders() }).then(r => r.ok ? r.text() : '(Preview not available)').then(text => {
                m.content.querySelector('pre').textContent = text;
            }).catch(() => {
                m.content.querySelector('pre').textContent = '(Preview not available)';
            });
        } else {
            m.content.innerHTML = `<pre>Loading...</pre>`;
            fetch(url, { headers: _authHeaders() }).then(r => r.ok ? r.text() : '(Preview not available)').then(text => {
                m.content.querySelector('pre').textContent = text;
            }).catch(() => {
                m.content.querySelector('pre').textContent = '(Preview not available)';
            });
        }
        m.downloadBtn.classList.remove('hidden');
        m.fullscreenBtn.classList.remove('hidden');
        m.overlay.classList.remove('hidden');
    },

    async _downloadPreviewFile() {
        const m = this._previewModal;
        if (!m._fileId) return;
        m.downloadBtn.disabled = true;
        try {
            const resp = await fetch(_authUrl(`/api/files/${m._fileId}/content`), { headers: _authHeaders() });
            if (resp.ok) {
                const blob = await resp.blob();
                const blobUrl = URL.createObjectURL(blob);
                const a = document.createElement('a');
                a.href = blobUrl;
                a.download = m._fileName || '';
                document.body.appendChild(a);
                a.click();
                a.remove();
                URL.revokeObjectURL(blobUrl);
            }
        } catch { /* ignore */ }
        m.downloadBtn.disabled = false;
    },

    _toggleFullscreen() {
        const m = this._previewModal;
        if (document.fullscreenElement) {
            document.exitFullscreen();
        } else {
            m.dialog.requestFullscreen();
        }
    },

    _closePreviewModal() {
        const m = this._previewModal;
        if (document.fullscreenElement) document.exitFullscreen();
        this._stopAutoplay();

        // Capture triage sets before clearing state
        const cache = this._slideshowCache;
        const deleteItems = [...this._slideshowDeleteSet].map(id => ({
            id,
            name: (cache[id] && cache[id].name) || `File ${id}`,
        }));
        const consolidateItems = [...this._slideshowConsolidateSet].map(id => ({
            id,
            name: (cache[id] && cache[id].name) || `File ${id}`,
        }));
        const tagItems = [...this._slideshowTagSet].map(id => ({
            id,
            name: (cache[id] && cache[id].name) || `File ${id}`,
        }));

        m.overlay.classList.add('hidden');
        m.downloadBtn.classList.add('hidden');
        m.fullscreenBtn.classList.add('hidden');
        m.prevBtn.classList.add('hidden');
        m.nextBtn.classList.add('hidden');
        m.counter.classList.add('hidden');
        m.autoplayWrap.classList.add('hidden');
        m.markBadge.classList.add('hidden');
        m.triageCounter.classList.add('hidden');
        this._slideshowTotal = 0;
        this._slideshowOffset = 0;
        this._slideshowWindow = [];
        this._slideshowWindowStart = 0;
        this._slideshowParams = null;
        this._slideshowCache = {};
        this._slideshowNavGen = 0;
        this._slideshowDeleteSet = new Set();
        this._slideshowConsolidateSet = new Set();
        this._slideshowTagSet = new Set();
        // Reset slideshow button in detail panel
        const ssBtn = document.getElementById('detail-slideshow');
        if (ssBtn) { ssBtn.textContent = 'Slideshow'; ssBtn.disabled = false; }
        // Stop any playing media
        m.content.querySelectorAll('video, audio').forEach(el => { el.pause(); el.src = ''; });
        m.content.innerHTML = '';

        // Show triage dialogs if any images were marked
        if ((deleteItems.length > 0 || consolidateItems.length > 0 || tagItems.length > 0) && this.slideshowTriage) {
            this.slideshowTriage.show(deleteItems, consolidateItems, tagItems);
        }
    },

    async startSlideshow(params) {
        // Reset all slideshow state for a clean start
        this._slideshowParams = params;
        this._slideshowCache = {};
        this._slideshowOffset = 0;
        this._slideshowWindow = [];
        this._slideshowWindowStart = 0;
        this._slideshowTotal = 0;
        this._slideshowNavGen = 0;
        this._slideshowDeleteSet = new Set();
        this._slideshowConsolidateSet = new Set();
        this._slideshowTagSet = new Set();

        // Fetch all IDs in one call — no per-window queries during navigation
        const p = params;
        let url;
        if (p.type === 'folder') {
            url = `/api/slideshow-ids?folder_id=${encodeURIComponent(p.folderId)}`;
        } else {
            url = `/api/slideshow-ids?${new URLSearchParams(p.searchParams).toString()}`;
        }
        const res = await API.get(url);
        if (!res.ok) return;
        this._slideshowWindow = res.data.ids;
        this._slideshowWindowStart = 0;
        this._slideshowTotal = res.data.total;

        if (this._slideshowTotal === 0) return;
        await this._slideshowShow(0);
        this._slideshowBuffer(0);
        this._startAutoplay();
    },

    async _slideshowNav(delta) {
        if (this._slideshowTotal === 0) return;
        let target = this._slideshowOffset + delta;
        target = ((target % this._slideshowTotal) + this._slideshowTotal) % this._slideshowTotal;
        this._slideshowOffset = target;
        const gen = ++this._slideshowNavGen;
        await this._slideshowTransition(target, gen);
    },

    async _slideshowTransition(globalOffset, gen) {
        const winIdx = globalOffset - this._slideshowWindowStart;
        const fileId = this._slideshowWindow[winIdx];
        if (!fileId) return;
        const m = this._previewModal;

        // Fetch detail (from cache or API)
        let detail = this._slideshowCache[fileId];
        if (!detail) {
            try {
                const res = await API.get(`/api/files/${fileId}`);
                if (res.ok) {
                    detail = res.data;
                    this._slideshowCache[fileId] = detail;
                }
            } catch { /* skip */ }
        }
        if (!detail) return;
        if (gen !== undefined && this._slideshowNavGen !== gen) return;

        const url = _authUrl(`/api/files/${fileId}/content`);
        m.title.textContent = detail.name;
        m._fileId = fileId;
        m._fileName = detail.name;

        // Build new image starting invisible
        const bufferedImg = this._slideshowCache[`_img_${fileId}`];
        let newImg;
        if (bufferedImg && bufferedImg.complete) {
            newImg = bufferedImg;
        } else {
            newImg = new Image();
            newImg.src = url;
            newImg.alt = detail.name;
        }
        newImg.className = 'slideshow-clickable slideshow-stacked slideshow-fade-out';
        newImg.style.visibility = '';
        newImg.addEventListener('click', () => {
            this._closePreviewModal();
            if (this.onNavigateToFile) this.onNavigateToFile(fileId);
        });

        // The current visible image crossfades out while new fades in
        const oldImg = m.content.querySelector('img:not(.slideshow-fade-out)') || m.content.querySelector('img');
        if (oldImg) {
            oldImg.classList.add('slideshow-stacked');
            oldImg.style.visibility = '';
            oldImg.classList.remove('slideshow-fade-out');
        }

        // Remove any images beyond prev/current/next window (cap at 3)
        const imgs = m.content.querySelectorAll('img');
        if (imgs.length > 2) {
            for (let i = 0; i < imgs.length - 2; i++) {
                if (imgs[i] !== oldImg) imgs[i].remove();
            }
        }

        m.content.appendChild(newImg);

        // Simultaneous crossfade: old out, new in
        requestAnimationFrame(() => {
            requestAnimationFrame(() => {
                if (oldImg) oldImg.classList.add('slideshow-fade-out');
                newImg.classList.remove('slideshow-fade-out');
            });
        });

        // When new image finishes fading in, hide all others —
        // but only if this image is still the current navigation
        const myGen = this._slideshowNavGen;
        newImg.addEventListener('transitionend', () => {
            if (this._slideshowNavGen !== myGen) return;
            for (const child of [...m.content.querySelectorAll('img')]) {
                if (child !== newImg) {
                    child.classList.add('slideshow-fade-out');
                    child.style.visibility = 'hidden';
                }
            }
        }, { once: true });

        m.counter.textContent = `${globalOffset + 1} / ${this._slideshowTotal}`;
        this._slideshowBuffer(globalOffset);
        this._slideshowUpdateMark();
    },

    async _slideshowShow(globalOffset) {
        const winIdx = globalOffset - this._slideshowWindowStart;
        const fileId = this._slideshowWindow[winIdx];
        if (!fileId) return;
        const m = this._previewModal;

        let detail = this._slideshowCache[fileId];
        if (!detail) {
            try {
                const res = await API.get(`/api/files/${fileId}`);
                if (res.ok) {
                    detail = res.data;
                    this._slideshowCache[fileId] = detail;
                }
            } catch { /* skip */ }
        }
        if (!detail) return;

        const url = _authUrl(`/api/files/${fileId}/content`);
        m.title.textContent = detail.name;
        m._fileId = fileId;
        m._fileName = detail.name;

        const bufferedImg = this._slideshowCache[`_img_${fileId}`];
        let img;
        if (bufferedImg && bufferedImg.complete) {
            img = bufferedImg;
        } else {
            img = new Image();
            img.src = url;
            img.alt = detail.name;
        }
        img.className = 'slideshow-clickable';
        m.content.innerHTML = '';
        m.content.appendChild(img);

        img.addEventListener('click', () => {
            this._closePreviewModal();
            if (this.onNavigateToFile) this.onNavigateToFile(fileId);
        });

        m.downloadBtn.classList.remove('hidden');
        m.fullscreenBtn.classList.remove('hidden');
        m.overlay.classList.remove('hidden');
        m.prevBtn.classList.remove('hidden');
        m.nextBtn.classList.remove('hidden');
        m.counter.classList.remove('hidden');
        m.autoplayWrap.classList.remove('hidden');
        m.counter.textContent = `${globalOffset + 1} / ${this._slideshowTotal}`;
        this._slideshowUpdateMark();
    },

    _slideshowBuffer(globalOffset) {
        if (this._slideshowTotal <= 1) return;
        const prevOffset = globalOffset === 0 ? this._slideshowTotal - 1 : globalOffset - 1;
        const nextOffset = globalOffset === this._slideshowTotal - 1 ? 0 : globalOffset + 1;
        this._bufferIfInWindow(prevOffset);
        this._bufferIfInWindow(nextOffset);
    },

    _bufferIfInWindow(globalOffset) {
        const winEnd = this._slideshowWindowStart + this._slideshowWindow.length;
        if (globalOffset < this._slideshowWindowStart || globalOffset >= winEnd) return;
        const fileId = this._slideshowWindow[globalOffset - this._slideshowWindowStart];
        if (!fileId || this._slideshowCache[`_img_${fileId}`]) return;
        const img = new Image();
        img.src = _authUrl(`/api/files/${fileId}/content`);
        this._slideshowCache[`_img_${fileId}`] = img;
        if (!this._slideshowCache[fileId]) {
            API.get(`/api/files/${fileId}`).then(res => {
                if (res.ok) this._slideshowCache[fileId] = res.data;
            }).catch(() => {});
        }
    },

    _toggleAutoplay() {
        if (this._slideshowPlaying) {
            this._stopAutoplay();
        } else {
            this._startAutoplay();
        }
    },

    _startAutoplay() {
        const m = this._previewModal;
        this._slideshowPlaying = true;
        m.playBtn.classList.add('active');
        m.playBtn.innerHTML = '&#9646;&#9646;';
        const interval = parseInt(m.speedSelect.value, 10) || 5000;
        this._slideshowTimer = setInterval(() => {
            this._slideshowNav(1);
        }, interval);
    },

    _stopAutoplay() {
        const m = this._previewModal;
        this._slideshowPlaying = false;
        if (m) {
            m.playBtn.classList.remove('active');
            m.playBtn.innerHTML = '&#9654;';
        }
        if (this._slideshowTimer) {
            clearInterval(this._slideshowTimer);
            this._slideshowTimer = null;
        }
    },

    _slideshowCurrentFileId() {
        const winIdx = this._slideshowOffset - this._slideshowWindowStart;
        return this._slideshowWindow[winIdx] || null;
    },

    _slideshowToggleMark(kind) {
        const fileId = this._slideshowCurrentFileId();
        if (!fileId) return;
        const setMap = {
            delete: this._slideshowDeleteSet,
            consolidate: this._slideshowConsolidateSet,
            tag: this._slideshowTagSet,
        };
        const s = setMap[kind];
        if (!s) return;
        if (s.has(fileId)) {
            s.delete(fileId);
        } else {
            s.add(fileId);
        }
        this._slideshowUpdateMark();
    },

    _slideshowUpdateMark() {
        const m = this._previewModal;
        const fileId = this._slideshowCurrentFileId();
        const badge = m.markBadge;
        const triageEl = m.triageCounter;

        // Badge on current image — show all active marks
        badge.classList.remove('mark-delete', 'mark-consolidate', 'mark-tag');
        const marks = [];
        if (fileId && this._slideshowDeleteSet.has(fileId)) marks.push('D');
        if (fileId && this._slideshowConsolidateSet.has(fileId)) marks.push('C');
        if (fileId && this._slideshowTagSet.has(fileId)) marks.push('T');

        if (marks.length > 0) {
            badge.textContent = marks.join(' ');
            // Colour by highest priority mark
            if (marks.includes('D')) badge.classList.add('mark-delete');
            else if (marks.includes('C')) badge.classList.add('mark-consolidate');
            else badge.classList.add('mark-tag');
            badge.classList.remove('hidden');
        } else {
            badge.classList.add('hidden');
        }

        // Triage counter in header
        const dc = this._slideshowDeleteSet.size;
        const cc = this._slideshowConsolidateSet.size;
        const tc = this._slideshowTagSet.size;
        if (dc === 0 && cc === 0 && tc === 0) {
            triageEl.classList.add('hidden');
        } else {
            const parts = [];
            if (dc > 0) parts.push(`${dc} to delete`);
            if (cc > 0) parts.push(`${cc} to consolidate`);
            if (tc > 0) parts.push(`${tc} to tag`);
            triageEl.textContent = parts.join(', ');
            triageEl.classList.remove('hidden');
        }
    },

    async showLoading() {
        this.el.innerHTML = `<div class="detail-loading"><div class="detail-spinner"></div><span>Loading\u2026</span></div>`;
        await new Promise(r => requestAnimationFrame(r));
    },

    _buildBreadcrumb(breadcrumb) {
        if (!breadcrumb || breadcrumb.length === 0) return '';
        const segments = breadcrumb.map(entry =>
            `<span class="breadcrumb-segment" data-node-id="${entry.nodeId}">${entry.name}</span>`
        );
        return `<div class="detail-breadcrumb">${segments.join('<span class="breadcrumb-sep">/</span>')}</div>`;
    },

    _wireBreadcrumbs() {
        this.el.querySelectorAll('.breadcrumb-segment').forEach(el => {
            el.addEventListener('click', () => {
                const nodeId = el.dataset.nodeId;
                if (nodeId && this.onNavigateToFolder) this.onNavigateToFolder(nodeId);
            });
        });
    },

    async renderDashboard() {
        const gen = ++this._renderGen;
        const signal = this._abortPrevious();
        await this.showLoading();
        let res;
        try {
            res = await API.get('/api/stats', { signal });
        } catch (e) {
            if (e.name === 'AbortError') return;
            throw e;
        }
        if (!res.ok || gen !== this._renderGen) return;
        const s = res.data;

        let scansHtml = '';
        if (s.recentScans && s.recentScans.length > 0) {
            scansHtml = s.recentScans.map(scan => {
                const ago = _timeAgo(scan.completedAt || scan.startedAt);
                const suffix = scan.status && scan.status !== 'completed' ? ` (${scan.status})` : '';
                return `<div class="detail-field">
                    <span class="label">${scan.location}</span>
                    <span class="value">${ago}${suffix}</span>
                </div>`;
            }).join('');
        }

        let typeHtml = '';
        if (s.typeBreakdown && s.typeBreakdown.length > 0) {
            typeHtml = s.typeBreakdown.map(t =>
                `<div class="detail-field">
                    <span class="label">${t.type || 'other'}</span>
                    <span class="value">${t.count.toLocaleString()}</span>
                </div>`
            ).join('');
        }

        this.el.innerHTML = `
            <div class="dashboard-stats">
                <div class="stat-card">
                    <div class="stat-value" data-stat="globalFileCount">${s.totalFiles.toLocaleString()}</div>
                    <div class="stat-label">Files Cataloged</div>
                </div>
                <div class="stat-card">
                    <div class="stat-value" data-stat="globalLocations">${s.totalLocations}</div>
                    <div class="stat-label">Locations</div>
                </div>
                <div class="stat-card">
                    <div class="stat-value" data-stat="globalDuplicates">${s.duplicateFiles.toLocaleString()}</div>
                    <div class="stat-label">Duplicates Found</div>
                </div>
                <div class="stat-card">
                    <div class="stat-value" data-stat="globalTotalSize">${s.totalSizeFormatted}</div>
                    <div class="stat-label">Total Cataloged</div>
                </div>
            </div>
            ${typeHtml ? `<div class="detail-section" data-stat="globalTypeBreakdown"><h3>Files by Type</h3>${typeHtml}</div>` : ''}
            <div class="detail-section">
                <h3>Recent Scans</h3>
                ${scansHtml || '<div class="detail-field"><span class="value">No scans yet.</span></div>'}
            </div>
            <div class="detail-section">
                <div class="detail-btn-group"><button class="btn" id="detail-recalc-stats">Recalculate Stats</button></div>
            </div>
        `;
        const recalcBtn = this.el.querySelector('#detail-recalc-stats');
        if (recalcBtn) {
            recalcBtn.addEventListener('click', () => {
                recalcBtn.textContent = 'Recalculating...';
                recalcBtn.disabled = true;
                API.post('/api/stats/recalculate');
            });
        }
    },

    async renderFile(file) {
        this._currentLocationNode = null;
        const gen = ++this._renderGen;
        const signal = this._abortPrevious();
        await this.showLoading();
        if (gen !== this._renderGen) return;
        if (file.type === 'folder') {
            await this.renderFolder(file);
            return;
        }

        // Fetch full detail from API if file has an id
        let detail = file;
        if (file.id && typeof file.id === 'number') {
            let res;
            try {
                res = await API.get(`/api/files/${file.id}`, { signal });
            } catch (e) {
                if (e.name === 'AbortError') return;
                throw e;
            }
            if (gen !== this._renderGen) return;
            if (res.ok) {
                detail = res.data;
            }
        }
        this._lastDetail = detail;

        const tags = detail.tags || [];
        const dups = detail.duplicates || [];

        const previewHtml = (detail.online && !detail.stale) ? this._buildPreview(detail) : '';

        const hasPendingOp = !!detail.pendingOp;
        const pendingOpLabel = detail.pendingOp === 'delete' ? 'deleted' : detail.pendingOp === 'move' ? 'moved' : detail.pendingOp === 'verify' ? 'verified' : detail.pendingOp;

        const staleBanner = detail.stale
            ? '<div class="detail-stale-banner">This file was not found during the last scan of this location.</div>'
            : '';
        const pendingBanner = hasPendingOp
            ? `<div class="detail-pending-banner"><span>This file will be ${pendingOpLabel} when the location comes online.</span><button class="btn btn-sm" id="detail-cancel-pending">Cancel</button></div>`
            : '';
        const ignoredBannerPlaceholder = detail.id ? '<div id="detail-ignored-banner"></div>' : '';
        const showInFolderBtn = detail.folderId ? `<button class="btn btn-sm" id="detail-show-folder" style="margin-top:0.4rem">Show in Folder</button>` : '';
        const downloadBtn = detail.id && detail.online && !detail.stale && !hasPendingOp ? `<button class="btn btn-sm" id="detail-download" style="margin-top:0.4rem">Download</button>` : '';
        const fileMissing = detail.locationOnline && !detail.online;
        const fileDisabled = detail.stale || fileMissing;
        const disabledReason = detail.stale ? 'File is stale' : 'File is missing from disk';
        const renameFileBtn = detail.id && detail.locationOnline && !detail.stale && !hasPendingOp ? `<button class="btn btn-sm" id="detail-rename-file" style="margin-top:0.4rem"${fileMissing ? ` disabled title="${disabledReason}"` : ''}>Rename</button>` : '';
        const moveFileBtn = detail.id && !detail.stale && !hasPendingOp ? `<button class="btn btn-sm" id="detail-move-file" style="margin-top:0.4rem"${fileMissing ? ` disabled title="${disabledReason}"` : ''}>Move</button>` : '';
        const deleteFileBtn = detail.id && !hasPendingOp ? `<button class="btn btn-danger btn-sm" id="detail-delete-file" style="margin-top:0.4rem">Delete</button>` : '';
        const ignoreFileBtn = detail.id && !hasPendingOp ? `<button class="btn btn-sm" id="detail-ignore-file" style="margin-top:0.4rem">Ignore files like this\u2026</button>` : '';
        const btnRow = (downloadBtn || showInFolderBtn || renameFileBtn || moveFileBtn || deleteFileBtn || ignoreFileBtn) ? `<div style="display:flex;gap:0.4rem;flex-wrap:wrap">${downloadBtn}${showInFolderBtn}${renameFileBtn}${moveFileBtn}${ignoreFileBtn}${deleteFileBtn}</div>` : '';

        let html = `
            <div class="detail-section">
                <div class="detail-filename">${detail.name}${detail.locationOnline === false ? '<span class="detail-offline-badge">offline</span>' : ''}${hasPendingOp ? `<span class="pending-indicator">pending ${detail.pendingOp}</span>` : ''}</div>
                ${staleBanner}
                ${pendingBanner}
                ${ignoredBannerPlaceholder}
                ${detail.breadcrumb ? this._buildBreadcrumb(detail.breadcrumb) : `<div class="detail-path">${detail.path || ''}</div>`}
                ${btnRow}
            </div>
            ${previewHtml}
            <div class="detail-section">
                <h3>Metadata</h3>
                <div class="detail-field">
                    <span class="label">Type</span>
                    <span class="value">${detail.typeHigh || ''} / ${detail.typeLow || ''}</span>
                </div>
                <div class="detail-field">
                    <span class="label">Size</span>
                    <span class="value">${formatSize(detail.size)}</span>
                </div>
                <div class="detail-field">
                    <span class="label">Modified</span>
                    <span class="value">${formatDate(detail.date)}</span>
                </div>
                <div class="detail-field">
                    <span class="label">Cataloged</span>
                    <span class="value">${formatDate(detail.cataloged)}</span>
                </div>
                <div class="detail-field">
                    <span class="label">Last Seen</span>
                    <span class="value">${formatDate(detail.lastSeen)}</span>
                </div>
            </div>
            <div class="detail-section">
                <h3>Hashes${detail.verified ? '<span class="detail-verified-badge">verified</span>' : ''}</h3>
                <div class="detail-field">
                    <span class="label">Partial</span>
                    <span class="value" style="font-family: monospace; font-size: 0.8em;">${detail.hashPartial || ''}</span>
                </div>
                <div class="detail-field">
                    <span class="label">Fast</span>
                    <span class="value" style="font-family: monospace; font-size: 0.8em;">${detail.hashFast || ''}</span>
                </div>
                <div class="detail-field">
                    <span class="label">Strong</span>
                    <span class="value" style="font-family: monospace; font-size: 0.8em; word-break: break-all;">${detail.hashStrong || ''}</span>
                </div>
                ${detail.id && !detail.verified && !detail.stale && !hasPendingOp ? `<button class="btn btn-sm" id="detail-verify" style="margin-top:0.4rem">Verify (SHA-256)${detail.locationOnline === false ? ' \u2014 queued' : ''}</button>` : ''}
            </div>
            <div class="detail-section">
                <h3>Description</h3>
                <textarea class="detail-description-edit" id="detail-desc"
                    placeholder="Add a description...">${detail.description || ''}</textarea>
            </div>
            <div class="detail-section">
                <h3>Tags</h3>
                <div class="tag-list" id="detail-tags">
                    ${tags.map(t => `<span class="tag">${t} <span class="tag-remove" data-tag="${t}">&times;</span></span>`).join('')}
                </div>
                <div class="tag-add-row">
                    <input type="text" class="tag-input" id="detail-tag-input" placeholder="Add tag...">
                    <button class="btn btn-sm" id="detail-tag-add">+</button>
                </div>
            </div>
        `;

        const dupTotal = detail.dupTotal || dups.length;
        if (dupTotal > 0 && detail.size > 0) {
            const moreBtn = dupTotal > dups.length
                ? `<button class="btn btn-sm" id="detail-show-all-dups" style="margin-top:0.4rem">and ${(dupTotal - dups.length).toLocaleString()} more\u2026</button>`
                : '';
            html += `
                <div class="detail-section" id="detail-dups-section">
                    <h3>Duplicates (${dupTotal.toLocaleString()})</h3>
                    ${dups.map(d => `
                        <div class="dup-list-item dup-link" data-file-id="${d.fileId}">
                            <span class="dup-location">${d.location}</span><br>
                            ${d.path}
                        </div>
                    `).join('')}
                    ${moreBtn}
                </div>
            `;
        }

        this.el.innerHTML = html;
        this._wireEditing(detail);
        this._wireDupLinks();
        this._wireShowAllDups(detail);
        this._wireBreadcrumbs();
        this._wireShowInFolder(detail);
        this._loadTextPreview(detail);
        this._wirePreviewZoom(detail);
        const textPreviewBtn = document.getElementById('detail-preview-text');
        if (textPreviewBtn) textPreviewBtn.addEventListener('click', () => this._openPreviewModal(detail));
        const hexPreviewBtn = document.getElementById('detail-preview-hex');
        if (hexPreviewBtn) hexPreviewBtn.addEventListener('click', () => this._openHexPreview(detail));
        this._checkIgnored(detail, gen);

        const cancelPendingBtn = document.getElementById('detail-cancel-pending');
        if (cancelPendingBtn && detail.id) {
            cancelPendingBtn.addEventListener('click', async () => {
                cancelPendingBtn.disabled = true;
                cancelPendingBtn.textContent = 'Cancelling\u2026';
                await API.post(`/api/files/${detail.id}/cancel-pending`);
            });
        }

        const verifyBtn = document.getElementById('detail-verify');
        if (verifyBtn && detail.id) {
            verifyBtn.addEventListener('click', async () => {
                verifyBtn.disabled = true;
                verifyBtn.textContent = 'Verifying\u2026';
                const res = await API.post(`/api/files/${detail.id}/verify`);
                if (res.ok) {
                    if (res.data.deferred) {
                        // Queued for offline — detail will refresh via WS
                        verifyBtn.textContent = 'Queued';
                    } else {
                        this._lastDetail = res.data;
                        this.renderFile({ id: detail.id, type: 'file' });
                    }
                } else {
                    verifyBtn.textContent = res.error || 'Failed';
                }
            });
        }

        return {
            folderId: detail.folderId || null,
            locationId: detail.locationId || null,
            locationOnline: detail.locationOnline,
        };
    },

    _buildPreview(detail) {
        if (!detail.id) return '';
        const type = (detail.typeHigh || '').toLowerCase();
        const url = _authUrl(`/api/files/${detail.id}/content`);
        const zoom = `<button class="preview-zoom-btn" id="preview-zoom-btn" title="Enlarge">${zoomIcon}</button>`;

        const hexBtn = `<button class="btn btn-sm" id="detail-preview-hex">Preview as Hex</button>`;
        if (type === 'image') {
            return `<div class="detail-preview">${zoom}<img src="${url}" alt="${detail.name}"></div><div class="detail-preview-btns">${hexBtn}</div>`;
        }
        if (type === 'video') {
            return `<div class="detail-preview">${zoom}<video src="${url}" controls></video></div><div class="detail-preview-btns">${hexBtn}</div>`;
        }
        if (type === 'audio') {
            return `<div class="detail-preview">${zoom}<audio src="${url}" controls></audio></div><div class="detail-preview-btns">${hexBtn}</div>`;
        }
        if (type === 'document' && (detail.typeLow || '').toLowerCase() === 'pdf') {
            return `<div class="detail-preview detail-preview-pdf">${zoom}<iframe src="${url}" title="${detail.name}"></iframe></div><div class="detail-preview-btns">${hexBtn}</div>`;
        }
        if (type === 'text') {
            return `<div class="detail-preview">${zoom}<pre id="detail-text-preview">Loading...</pre></div><div class="detail-preview-btns">${hexBtn}</div>`;
        }
        return `<div class="detail-preview"><button class="btn btn-sm" id="detail-preview-text">Preview as text</button> ${hexBtn}</div>`;
    },

    async _loadTextPreview(detail) {
        const pre = document.getElementById('detail-text-preview');
        if (!pre || !detail.id) return;
        try {
            const resp = await fetch(`/api/files/${detail.id}/content`, { headers: _authHeaders() });
            if (!resp.ok) {
                pre.textContent = '(Preview not available)';
                return;
            }
            const text = await resp.text();
            if (text.length > 2048) {
                pre.textContent = text.slice(0, 2048) + '\n\n— Truncated (2 KB preview limit) —';
            } else {
                pre.textContent = text;
            }
        } catch {
            pre.textContent = '(Preview not available)';
        }
    },

    // ── Hex viewer with paging and search ──

    _hexPageSize: 4096,
    _hexState: null,

    _openHexPreview(detail) {
        const m = this._previewModal;
        m.title.textContent = `${detail.name} — Hex`;
        m._fileId = detail.id;
        m._fileName = detail.name;

        m.content.innerHTML = `
            <div class="hex-viewer">
                <div class="hex-toolbar">
                    <button class="btn btn-sm" id="hex-top" title="Top">Top</button>
                    <button class="btn btn-sm" id="hex-prev" title="Previous page">Prev</button>
                    <button class="btn btn-sm" id="hex-next" title="Next page">Next</button>
                    <button class="btn btn-sm" id="hex-end" title="End">End</button>
                    <span class="hex-separator"></span>
                    <input type="text" class="hex-find-input" id="hex-find" placeholder="Find text or hex (0x...)">
                    <button class="btn btn-sm btn-primary" id="hex-find-go">Find</button>
                    <button class="btn btn-sm" id="hex-find-next">Next match</button>
                    <span class="hex-status" id="hex-status"></span>
                </div>
                <pre class="hex-dump" id="hex-output">Loading...</pre>
            </div>`;

        m.downloadBtn.classList.remove('hidden');
        m.fullscreenBtn.classList.remove('hidden');
        m.overlay.classList.remove('hidden');

        this._hexState = {
            fileId: detail.id,
            offset: 0,
            fileSize: 0,
            searchBytes: null,
            searchOffset: 0,
        };

        this._hexWireButtons();
        this._hexLoadPage(0);
    },

    _hexWireButtons() {
        document.getElementById('hex-top').addEventListener('click', () => this._hexLoadPage(0));
        document.getElementById('hex-prev').addEventListener('click', () => {
            const s = this._hexState;
            this._hexLoadPage(Math.max(0, s.offset - this._hexPageSize));
        });
        document.getElementById('hex-next').addEventListener('click', () => {
            const s = this._hexState;
            const next = s.offset + this._hexPageSize;
            if (next < s.fileSize) this._hexLoadPage(next);
        });
        document.getElementById('hex-end').addEventListener('click', () => {
            const s = this._hexState;
            const last = Math.max(0, Math.floor((s.fileSize - 1) / this._hexPageSize) * this._hexPageSize);
            this._hexLoadPage(last);
        });

        const findInput = document.getElementById('hex-find');
        const findGo = document.getElementById('hex-find-go');
        const findNext = document.getElementById('hex-find-next');

        const doFind = () => this._hexFind(findInput.value.trim(), 0);
        findGo.addEventListener('click', doFind);
        findInput.addEventListener('keydown', (e) => { if (e.key === 'Enter') doFind(); });
        findNext.addEventListener('click', () => {
            const s = this._hexState;
            if (s.searchBytes) this._hexFind(null, s.searchOffset + 1);
        });
    },

    async _hexLoadPage(offset) {
        const s = this._hexState;
        const pre = document.getElementById('hex-output');
        const status = document.getElementById('hex-status');
        if (!pre) return;

        pre.textContent = 'Loading...';

        try {
            const resp = await fetch(
                `/api/files/${s.fileId}/bytes?offset=${offset}&limit=${this._hexPageSize}`,
                { headers: _authHeaders() }
            );
            if (!resp.ok) { pre.textContent = '(Preview not available)'; return; }

            s.fileSize = parseInt(resp.headers.get('X-File-Size') || '0', 10);
            s.offset = offset;

            const buf = await resp.arrayBuffer();
            const bytes = new Uint8Array(buf);
            pre.textContent = this._formatHexDump(bytes, offset);

            const endByte = Math.min(offset + bytes.length, s.fileSize);
            status.textContent = `${offset.toLocaleString()}–${endByte.toLocaleString()} of ${s.fileSize.toLocaleString()} bytes`;

            // Update button states
            document.getElementById('hex-prev').disabled = offset === 0;
            document.getElementById('hex-top').disabled = offset === 0;
            document.getElementById('hex-next').disabled = endByte >= s.fileSize;
            document.getElementById('hex-end').disabled = endByte >= s.fileSize;
        } catch {
            pre.textContent = '(Preview not available)';
        }
    },

    async _hexFind(query, startFrom) {
        const s = this._hexState;
        const status = document.getElementById('hex-status');

        // Parse query into bytes to search for
        if (query !== null) {
            if (query.startsWith('0x') || query.startsWith('0X')) {
                // Hex string: "0x48656c6c6f"
                const hexStr = query.slice(2).replace(/\s/g, '');
                if (!/^[0-9a-fA-F]*$/.test(hexStr) || hexStr.length % 2 !== 0) {
                    status.textContent = 'Invalid hex string';
                    return;
                }
                const bytes = [];
                for (let i = 0; i < hexStr.length; i += 2) {
                    bytes.push(parseInt(hexStr.slice(i, i + 2), 16));
                }
                s.searchBytes = new Uint8Array(bytes);
            } else {
                // Text string — encode as UTF-8
                s.searchBytes = new TextEncoder().encode(query);
            }
            s.searchOffset = startFrom;
        } else {
            s.searchOffset = startFrom;
        }

        if (!s.searchBytes || s.searchBytes.length === 0) return;

        status.textContent = 'Searching...';

        // Search in chunks from the server
        const needle = s.searchBytes;
        const chunkSize = 65536;
        let pos = s.searchOffset;

        while (pos < s.fileSize) {
            try {
                // Fetch enough to find needle spanning chunk boundary
                const fetchSize = chunkSize + needle.length - 1;
                const resp = await fetch(
                    `/api/files/${s.fileId}/bytes?offset=${pos}&limit=${fetchSize}`,
                    { headers: _authHeaders() }
                );
                if (!resp.ok) { status.textContent = 'Search failed'; return; }

                const buf = await resp.arrayBuffer();
                const haystack = new Uint8Array(buf);
                if (haystack.length === 0) break;

                // Search within this chunk
                const idx = this._findBytes(haystack, needle);
                if (idx !== -1) {
                    const foundAt = pos + idx;
                    s.searchOffset = foundAt;
                    // Load the page containing the match
                    const pageStart = Math.floor(foundAt / this._hexPageSize) * this._hexPageSize;
                    await this._hexLoadPage(pageStart);
                    status.textContent = `Found at offset 0x${foundAt.toString(16)} (${foundAt.toLocaleString()})`;
                    return;
                }

                // Advance past the chunk (minus needle overlap)
                pos += chunkSize;
            } catch {
                status.textContent = 'Search failed';
                return;
            }
        }

        status.textContent = s.searchOffset > 0 ? 'No more matches' : 'Not found';
    },

    _findBytes(haystack, needle) {
        outer: for (let i = 0; i <= haystack.length - needle.length; i++) {
            for (let j = 0; j < needle.length; j++) {
                if (haystack[i + j] !== needle[j]) continue outer;
            }
            return i;
        }
        return -1;
    },

    _formatHexDump(bytes, baseOffset) {
        baseOffset = baseOffset || 0;
        const lines = [];
        for (let i = 0; i < bytes.length; i += 16) {
            const row = bytes.slice(i, i + 16);
            const hex = [];
            const chars = [];
            for (let j = 0; j < 16; j++) {
                if (j < row.length) {
                    hex.push(row[j].toString(16).padStart(2, '0'));
                    chars.push(row[j] >= 0x20 && row[j] <= 0x7e ? String.fromCharCode(row[j]) : '.');
                } else {
                    hex.push('  ');
                    chars.push(' ');
                }
            }
            const hexStr = hex.slice(0, 8).join(' ') + '  ' + hex.slice(8).join(' ');
            lines.push((baseOffset + i).toString(16).padStart(8, '0') + '  ' + hexStr + '  |' + chars.join('') + '|');
        }
        return lines.join('\n');
    },

    _wirePreviewZoom(detail) {
        const btn = document.getElementById('preview-zoom-btn');
        if (btn && detail.id) {
            btn.addEventListener('click', () => this._openPreviewModal(detail));
        }
    },

    _wireEditing(detail) {
        const descEl = document.getElementById('detail-desc');
        if (descEl && detail.id) {
            descEl.addEventListener('blur', async () => {
                const newDesc = descEl.value;
                if (newDesc !== (detail.description || '')) {
                    await API.patch(`/api/files/${detail.id}`, { description: newDesc });
                }
            });
        }

        // Tag removal
        this.el.querySelectorAll('.tag-remove').forEach(btn => {
            btn.addEventListener('click', async () => {
                const tag = btn.dataset.tag;
                const newTags = (detail.tags || []).filter(t => t !== tag);
                const res = await API.patch(`/api/files/${detail.id}`, { tags: newTags });
                if (res.ok) {
                    detail.tags = res.data.tags;
                    this._lastDetail = res.data;
                    this.renderFile({ id: detail.id, type: 'file' });
                }
            });
        });

        // Tag addition
        const addBtn = document.getElementById('detail-tag-add');
        const tagInput = document.getElementById('detail-tag-input');
        if (addBtn && tagInput && detail.id) {
            const addTag = async () => {
                const newTag = tagInput.value.trim();
                if (!newTag) return;
                const newTags = [...(detail.tags || []), newTag];
                const res = await API.patch(`/api/files/${detail.id}`, { tags: newTags });
                if (res.ok) {
                    detail.tags = res.data.tags;
                    this._lastDetail = res.data;
                    this.renderFile({ id: detail.id, type: 'file' });
                }
            };
            addBtn.addEventListener('click', addTag);
            tagInput.addEventListener('keydown', (e) => {
                if (e.key === 'Enter') addTag();
            });
        }

        // Download
        const dlBtn = document.getElementById('detail-download');
        if (dlBtn && detail.id) {
            dlBtn.addEventListener('click', async () => {
                dlBtn.disabled = true;
                try {
                    const resp = await fetch(_authUrl(`/api/files/${detail.id}/content`), { headers: _authHeaders() });
                    if (resp.ok) {
                        const blob = await resp.blob();
                        const blobUrl = URL.createObjectURL(blob);
                        const a = document.createElement('a');
                        a.href = blobUrl;
                        a.download = detail.name || '';
                        document.body.appendChild(a);
                        a.click();
                        a.remove();
                        URL.revokeObjectURL(blobUrl);
                    }
                } catch { /* ignore */ }
                dlBtn.disabled = false;
            });
        }
    },

    _wireDupLinks() {
        this.el.querySelectorAll('.dup-link').forEach(el => {
            el.addEventListener('click', () => {
                const fileId = parseInt(el.dataset.fileId, 10);
                if (fileId && this.onNavigateToFile) this.onNavigateToFile(fileId);
            });
        });
    },

    _wireShowAllDups(detail) {
        const btn = document.getElementById('detail-show-all-dups');
        const effectiveHash = detail.hashStrong || detail.hashFast;
        if (btn && effectiveHash && this.onShowDuplicates) {
            btn.addEventListener('click', () => {
                this.onShowDuplicates(effectiveHash);
            });
        }
    },

    _wireShowInFolder(detail) {
        const btn = document.getElementById('detail-show-folder');
        if (btn && detail.folderId && this.onNavigateToFolder) {
            btn.addEventListener('click', () => {
                this.onNavigateToFolder(detail.folderId, detail.id);
            });
        }
    },

    async _checkIgnored(detail, gen) {
        if (!detail.id || !detail.name) return;
        const locId = (detail.locationId || '').toString().replace('loc-', '');
        const params = new URLSearchParams({
            filename: detail.name,
            file_size: String(detail.size != null ? detail.size : 0),
        });
        if (locId) params.set('location_id', locId);
        let res;
        try {
            res = await API.get(`/api/ignore/check?${params}`);
        } catch { return; }
        if (!res.ok || gen !== this._renderGen) return;
        const banner = document.getElementById('detail-ignored-banner');
        if (!banner) return;
        if (res.data.ignored) {
            const rule = res.data.rule;
            const scope = rule.location_id ? 'this location' : 'all locations';
            banner.innerHTML = `<div class="detail-stale-banner" style="background:var(--color-warning-bg, #fff3cd);color:var(--color-warning-text, #856404);border-color:var(--color-warning-border, #ffc107);">Files like this are ignored (${scope}). <a href="#" id="detail-remove-ignore">Remove rule</a></div>`;
            const dupsSection = document.getElementById('detail-dups-section');
            if (dupsSection) dupsSection.remove();
            const removeLink = document.getElementById('detail-remove-ignore');
            if (removeLink) {
                removeLink.addEventListener('click', async (e) => {
                    e.preventDefault();
                    const delRes = await API.delete(`/api/ignore/${rule.id}`);
                    if (delRes.ok) {
                        banner.innerHTML = '';
                    }
                });
            }
        }
    },

    updateActivity() {
        const node = this._currentLocationNode;
        if (!node) return;
        const el = this.el && this.el.querySelector('.detail-agent-status');
        const activity = locationActivityLabel(node.id, this._locationActivityFn);
        if (activity) {
            if (el) {
                el.textContent = activity;
                el.className = `value detail-agent-status ${activity.toLowerCase()}`;
            } else {
                // Activity field doesn't exist yet — inject it after Status
                const statusField = this.el && this.el.querySelector('.detail-field');
                if (statusField && statusField.parentNode) {
                    const div = document.createElement('div');
                    div.className = 'detail-field';
                    div.innerHTML = `<span class="label">Activity</span>
                        <span class="value detail-agent-status ${activity.toLowerCase()}">${activity}</span>`;
                    statusField.after(div);
                }
            }
        } else if (el) {
            el.parentNode.remove();
        }
    },

    async renderLocation(node) {
        this._currentLocationNode = node;
        const gen = ++this._renderGen;
        const signal = this._abortPrevious();
        await this.showLoading();
        this._lastDetail = null;
        const locId = node.id.replace('loc-', '');
        let res;
        try {
            res = await API.get(`/api/locations/${locId}/stats`, { signal });
        } catch (e) {
            if (e.name === 'AbortError') return {};
            throw e;
        }
        if (!res.ok || gen !== this._renderGen) return {};
        const s = res.data;
        this._lastLocationOnline = s.online;

        const statusClass = s.online ? '' : ' offline';
        const statusLabel = s.online ? 'Online' : 'Offline';

        let typeHtml = '';
        if (s.typeBreakdown && s.typeBreakdown.length > 0) {
            typeHtml = s.typeBreakdown.map(t =>
                `<div class="detail-field">
                    <span class="label">${t.type || 'other'}</span>
                    <span class="value">${t.count.toLocaleString()}</span>
                </div>`
            ).join('');
        }

        const dayNames = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'];
        const schedDays = s.scheduleDays || [];
        const schedTime = s.scheduleTime || '03:00';
        const schedEnabled = !!s.scheduleEnabled;
        const schedLastRun = s.scheduleLastRun;

        const dayCheckboxes = dayNames.map((name, i) => {
            const checked = schedDays.includes(i) ? ' checked' : '';
            return `<label class="detail-schedule-day"><input type="checkbox" value="${i}"${checked}> ${name}</label>`;
        }).join('');

        this.el.innerHTML = `
            <div class="detail-section">
                <div class="detail-filename">${s.name}</div>
                <div class="detail-path">${s.rootPath}</div>
            </div>
            <div class="detail-section">
                <h3>Location Info</h3>
                <div class="detail-field">
                    <span class="label">Status</span>
                    <span class="value${statusClass}">${statusLabel}</span>
                </div>
                ${(() => {
                    const activity = locationActivityLabel(node.id, this._locationActivityFn);
                    return activity ? `<div class="detail-field">
                    <span class="label">Activity</span>
                    <span class="value detail-agent-status ${activity.toLowerCase()}">${activity}</span>
                </div>` : '';
                })()}
                ${s.diskStats && s.diskStats.mount ? `
                <div class="detail-field">
                    <span class="label">Capacity</span>
                    <span class="value">${formatSize(s.diskStats.total)}</span>
                </div>
                <div class="detail-field">
                    <span class="label">Free Space</span>
                    <span class="value">${formatSize(s.diskStats.free)} (${(s.diskStats.free / s.diskStats.total * 100).toFixed(1)}%)</span>
                </div>
                ${s.diskStats.readonly ? `<div class="detail-field">
                    <span class="label">Read Only</span>
                    <span class="value">Yes</span>
                </div>` : ''}` : ''}
                <div class="detail-field">
                    <span class="label">Added</span>
                    <span class="value">${formatDate(s.dateAdded)}</span>
                </div>
                <div class="detail-field">
                    <span class="label">Last Scanned</span>
                    <span class="value" data-stat="lastScanned">${s.dateLastScanned ? _timeAgo(s.dateLastScanned) + (s.lastScanStatus && s.lastScanStatus !== 'completed' ? ' (' + s.lastScanStatus + ')' : '') : 'Never'}</span>
                </div>
            </div>
            ${s.online ? `<div class="detail-section">
                <h3>Scheduled Scan</h3>
                <div class="detail-schedule">
                    <label class="detail-schedule-toggle">
                        <input type="checkbox" id="schedule-enabled"${schedEnabled ? ' checked' : ''}> Enable scheduled scan
                    </label>
                    <div class="detail-schedule-config" id="schedule-config"${schedEnabled ? '' : ' style="display:none"'}>
                        <div class="detail-schedule-days">${dayCheckboxes}</div>
                        <div class="detail-schedule-time">
                            <label>Time</label>
                            <input type="time" id="schedule-time" value="${schedTime}">
                        </div>
                        <button class="btn btn-sm" id="schedule-save">Save Schedule</button>
                    </div>
                    <div class="detail-field" id="schedule-last-run-row"${schedEnabled ? '' : ' style="display:none"'}>
                        <span class="label">Last scheduled run</span>
                        <span class="value">${schedLastRun ? _timeAgo(schedLastRun) : 'Never'}</span>
                    </div>
                </div>
            </div>` : ''}
            <div class="detail-section">
                <h3>Contents</h3>
                <div class="detail-field">
                    <span class="label">Files</span>
                    <span class="value" data-stat="fileCount">${s.fileCount.toLocaleString()}</span>
                </div>
                <div class="detail-field">
                    <span class="label">Folders</span>
                    <span class="value" data-stat="folderCount">${s.folderCount.toLocaleString()}</span>
                </div>
                <div class="detail-field">
                    <span class="label">Total Size</span>
                    <span class="value" data-stat="totalSize">${s.totalSizeFormatted}</span>
                </div>
                <div class="detail-field">
                    <span class="label">Duplicates</span>
                    <span class="value" data-stat="duplicates">${s.duplicateFiles.toLocaleString()}</span>
                </div>
                <div class="detail-field">
                    <span class="label">Hidden</span>
                    <span class="value" data-stat="hiddenFiles">${(s.hiddenFiles || 0).toLocaleString()}</span>
                </div>
            </div>
            ${typeHtml ? `<div class="detail-section" data-stat="typeBreakdown"><h3>File Types</h3>${typeHtml}</div>` : ''}
            <div class="detail-section">
                <div class="detail-btn-group">
                    <span id="detail-slideshow-slot"></span>
                    <button class="btn" id="detail-new-folder"${s.online ? '' : ' disabled title="Location is offline"'}>New Folder</button>
                    <button class="btn" id="detail-download-zip"${s.online ? '' : ' disabled title="Location is offline"'}>Download ZIP</button>
                    <button class="btn" id="detail-merge-btn"${s.online ? '' : ' disabled title="Location is offline"'}>Merge</button>
                    <button class="btn" id="detail-treemap-btn"${s.online ? '' : ' disabled title="Location is offline"'}>Storage Map</button>
                    <button class="btn" id="detail-rename-location">Rename</button>
                    <button class="btn btn-danger" id="detail-delete-location">Delete Location</button>
                </div>
            </div>
        `;

        if (s.online) this._wireSchedule(locId);

        return { online: s.online };
    },

    async renderFolder(folder) {
        this._currentLocationNode = null;
        const gen = ++this._renderGen;
        const signal = this._abortPrevious();
        await this.showLoading();
        this._lastDetail = null;
        const folderId = String(folder.id).replace('fld-', '');
        let res;
        try {
            res = await API.get(`/api/folders/${folderId}/stats`, { signal });
        } catch (e) {
            if (e.name === 'AbortError') return {};
            throw e;
        }
        if (gen !== this._renderGen) return {};
        if (!res.ok) {
            this.el.innerHTML = `
                <div class="detail-section">
                    <div class="detail-filename">${folder.name}</div>
                    <div class="detail-path">Folder</div>
                </div>
            `;
            return {};
        }
        const s = res.data;

        this.el.innerHTML = `
            <div class="detail-section">
                <div class="detail-filename">${s.name}</div>
                ${s.breadcrumb ? this._buildBreadcrumb(s.breadcrumb) : `<div class="detail-path">${s.location} / ${s.relPath}</div>`}
                <label class="detail-dup-exclude">
                    <input type="checkbox" id="detail-dup-exclude-cb" ${s.dupExcluded ? 'checked' : ''}>
                    Exclude from duplicates
                </label>
            </div>
            <div class="detail-section">
                <h3>Contents</h3>
                <div class="detail-field">
                    <span class="label">Files</span>
                    <span class="value" data-stat="fileCount">${s.fileCount.toLocaleString()}</span>
                </div>
                <div class="detail-field">
                    <span class="label">Subfolders</span>
                    <span class="value" data-stat="folderCount">${s.subfolderCount.toLocaleString()}</span>
                </div>
                <div class="detail-field">
                    <span class="label">Total Size</span>
                    <span class="value" data-stat="totalSize">${s.totalSizeFormatted}</span>
                </div>
                <div class="detail-field">
                    <span class="label">Duplicates</span>
                    <span class="value" data-stat="duplicates">${s.duplicateFiles.toLocaleString()}</span>
                </div>
                <div class="detail-field">
                    <span class="label">Hidden</span>
                    <span class="value" data-stat="hiddenFiles">${(s.hiddenFiles || 0).toLocaleString()}</span>
                </div>
            </div>
            <div class="detail-section">
                <div class="detail-btn-group">
                    <span id="detail-slideshow-slot"></span>
                    <button class="btn" id="detail-new-folder"${s.locationOnline === false ? ' disabled title="Location is offline"' : s.online === false ? ' disabled title="Folder is missing from disk"' : ''}>New Folder</button>
                    <button class="btn" id="detail-download-zip"${s.locationOnline === false ? ' disabled title="Location is offline"' : s.online === false ? ' disabled title="Folder is missing from disk"' : ''}>Download ZIP</button>
                    <button class="btn" id="detail-merge-btn"${s.locationOnline === false ? ' disabled title="Location is offline"' : s.online === false ? ' disabled title="Folder is missing from disk"' : ''}>Merge</button>
                    <button class="btn" id="detail-move-folder"${s.locationOnline === false ? ' disabled title="Location is offline"' : s.online === false ? ' disabled title="Folder is missing from disk"' : ''}>Move</button>
                    <button class="btn btn-danger" id="detail-delete-folder"${s.locationOnline === false ? ' disabled title="Location is offline"' : ''}>Delete Folder</button>
                </div>
            </div>
        `;
        this._wireBreadcrumbs();

        const dupExcludeCb = document.getElementById('detail-dup-exclude-cb');
        if (dupExcludeCb) {
            dupExcludeCb.addEventListener('change', async () => {
                const exclude = dupExcludeCb.checked;
                // Phase 1: get counts for confirmation
                const res = await API.post(`/api/folders/${folderId}/dup-exclude`, { exclude });
                if (!res.ok) {
                    dupExcludeCb.checked = !exclude; // revert
                    return;
                }
                const d = res.data;
                if (d.confirm) {
                    const verb = d.direction === 'exclude' ? 'Exclude' : 'Include';
                    const prep = d.direction === 'exclude' ? 'from' : 'in';
                    const ok = await ConfirmModal.open({
                        title: `${verb} ${prep} duplicates`,
                        message: `${verb} ${d.fileCount.toLocaleString()} files in ${d.folderCount.toLocaleString()} folders ${prep} duplicate detection? This will pause queued operations.`,
                        confirmLabel: verb,
                    });
                    if (!ok) {
                        dupExcludeCb.checked = !exclude; // revert
                        return;
                    }
                    // Phase 2: confirmed — start the operation
                    const startRes = await API.post(`/api/folders/${folderId}/dup-exclude`, { exclude, confirmed: true });
                    if (!startRes.ok) {
                        dupExcludeCb.checked = !exclude;
                    }
                }
            });
        }

        return {
            locationId: s.locationId || null,
            locationOnline: s.locationOnline,
        };
    },

    _wireSchedule(locId) {
        const enabledCb = document.getElementById('schedule-enabled');
        const configDiv = document.getElementById('schedule-config');
        const lastRunRow = document.getElementById('schedule-last-run-row');
        const saveBtn = document.getElementById('schedule-save');

        if (enabledCb) {
            enabledCb.addEventListener('change', () => {
                const show = enabledCb.checked;
                if (configDiv) configDiv.style.display = show ? '' : 'none';
                if (lastRunRow) lastRunRow.style.display = show ? '' : 'none';
                // If disabling, save immediately
                if (!show) {
                    API.patch(`/api/locations/${locId}`, {
                        scheduleEnabled: false,
                        scheduleDays: [],
                        scheduleTime: '03:00',
                    });
                }
            });
        }

        if (saveBtn) {
            saveBtn.addEventListener('click', async () => {
                const days = [];
                document.querySelectorAll('.detail-schedule-days input:checked').forEach(cb => {
                    days.push(parseInt(cb.value, 10));
                });
                const timeInput = document.getElementById('schedule-time');
                const time = timeInput ? timeInput.value : '03:00';
                saveBtn.disabled = true;
                saveBtn.textContent = 'Saving...';
                const res = await API.patch(`/api/locations/${locId}`, {
                    scheduleEnabled: true,
                    scheduleDays: days,
                    scheduleTime: time,
                });
                saveBtn.disabled = false;
                saveBtn.textContent = res.ok ? 'Saved' : 'Save Schedule';
                if (res.ok) {
                    setTimeout(() => { saveBtn.textContent = 'Save Schedule'; }, 1500);
                }
            });
        }
    },

    renderSearchResults(data, searchParams) {
        this._renderGen++;
        this._lastDetail = null;
        const folderCount = (data.folders || []).length;
        const folderField = folderCount > 0 ? `
                <div class="detail-field">
                    <span class="label">Folders</span>
                    <span class="value">${folderCount.toLocaleString()}</span>
                </div>` : '';
        // Check current page for any images to decide whether to show button
        const hasImages = (data.items || []).some(f => (f.typeHigh || '').toLowerCase() === 'image');
        const slideshowBtn = hasImages && searchParams
            ? `<div class="detail-section"><div class="detail-btn-group"><button class="btn" id="detail-slideshow">Slideshow</button></div></div>`
            : '';
        this.el.innerHTML = `
            <div class="detail-section">
                <div class="detail-filename">Search Results</div>
            </div>
            <div class="detail-section">
                <h3>Summary</h3>
                <div class="detail-field">
                    <span class="label">Files</span>
                    <span class="value">${data.total.toLocaleString()}</span>
                </div>${folderField}
            </div>
            ${slideshowBtn}
        `;
        if (hasImages && searchParams) {
            document.getElementById('detail-slideshow').addEventListener('click', async () => {
                const btn = document.getElementById('detail-slideshow');
                btn.disabled = true;
                btn.textContent = 'Loading\u2026';
                await this.startSlideshow({ type: 'search', searchParams: Object.fromEntries(new URLSearchParams(searchParams)) });
                if (this._slideshowTotal === 0) {
                    btn.textContent = 'No images available';
                    setTimeout(() => { btn.textContent = 'Slideshow'; btn.disabled = false; }, 2000);
                }
            });
        }
    },

    getFileDups() {
        if (this._lastDetail && this._lastDetail.duplicates) {
            return this._lastDetail.duplicates;
        }
        return [];
    },

    async renderMultiSelect(items) {
        const gen = ++this._renderGen;
        const signal = this._abortPrevious();
        this._lastDetail = null;

        const files = items.filter(i => i.type !== 'folder');
        const folders = items.filter(i => i.type === 'folder');

        // Fetch folder sizes in parallel
        const folderSizes = {};
        if (folders.length > 0) {
            let results;
            try {
                results = await Promise.all(
                    folders.map(f => {
                        const numId = String(f.id).replace('fld-', '');
                        return API.get(`/api/folders/${numId}/stats`, { signal });
                    })
                );
            } catch (e) {
                if (e.name === 'AbortError') return;
                throw e;
            }
            if (gen !== this._renderGen) return;
            results.forEach((res, i) => {
                if (res.ok) {
                    folderSizes[folders[i].id] = res.data.totalSize || 0;
                }
            });
        }

        const totalSize = files.reduce((sum, i) => sum + (i.size || 0), 0)
            + folders.reduce((sum, f) => sum + (folderSizes[f.id] || 0), 0);

        const fileCount = files.length;
        const folderCount = folders.length;
        const parts = [];
        if (fileCount > 0) parts.push(`${fileCount} file${fileCount !== 1 ? 's' : ''}`);
        if (folderCount > 0) parts.push(`${folderCount} folder${folderCount !== 1 ? 's' : ''}`);

        let itemListHtml = items.map(item => {
            const icon = item.type === 'folder' ? '\uD83D\uDCC1' : '\uD83D\uDCC4';
            const size = item.type === 'folder'
                ? (folderSizes[item.id] ? formatSize(folderSizes[item.id]) : '')
                : (item.size ? formatSize(item.size) : '');
            return `<div class="detail-field">
                <span class="label">${icon} ${item.name}</span>
                <span class="value">${size}</span>
            </div>`;
        }).join('');

        this.el.innerHTML = `
            <div class="detail-section">
                <div class="detail-filename">${items.length} Items Selected</div>
                <div class="detail-path">${parts.join(' \u00B7 ')}</div>
                <div class="detail-path">Total size: ${formatSize(totalSize)}</div>
            </div>
            <div class="detail-section">
                <div class="detail-btn-group">
                    <button class="btn btn-danger btn-sm" id="batch-delete-btn">Delete</button>
                    <button class="btn btn-sm" id="batch-move-btn">Move</button>
                    <button class="btn btn-sm" id="batch-download-btn">Download ZIP</button>
                </div>
            </div>
            <div class="detail-section">
                <h3>Tags</h3>
                <div class="tag-add-row">
                    <input type="text" class="tag-input" id="batch-tag-input" placeholder="Add tag to ${fileCount} file${fileCount !== 1 ? 's' : ''}...">
                    <button class="btn btn-sm" id="batch-tag-add">+</button>
                </div>
            </div>
            <div class="detail-section">
                <h3>Items</h3>
                ${itemListHtml}
            </div>
        `;
    },
};

function locationActivityLabel(nodeId, activityFn) {
    if (!activityFn) return null;
    const activity = activityFn(nodeId);
    if (!activity) return null;
    return activity;
}

function _timeAgo(isoStr) {
    if (!isoStr) return '';
    const then = new Date(isoStr);
    const now = new Date();
    const diffMs = now - then;
    const diffMin = Math.floor(diffMs / 60000);
    if (diffMin < 60) return `${diffMin} minutes ago`;
    const diffHr = Math.floor(diffMin / 60);
    if (diffHr < 24) return `${diffHr} hours ago`;
    const diffDay = Math.floor(diffHr / 24);
    if (diffDay < 30) return `${diffDay} days ago`;
    const diffMonth = Math.floor(diffDay / 30);
    return `${diffMonth} months ago`;
}

export default Detail;
