import API from '../api.js';

function formatSize(bytes) {
    if (bytes === null || bytes === undefined || bytes === 0) return '0 B';
    if (bytes < 1024) return bytes + ' B';
    if (bytes < 1048576) return (bytes / 1024).toFixed(1) + ' KB';
    if (bytes < 1073741824) return (bytes / 1048576).toFixed(1) + ' MB';
    if (bytes < 1099511627776) return (bytes / 1073741824).toFixed(1) + ' GB';
    return (bytes / 1099511627776).toFixed(1) + ' TB';
}

const Treemap = {
    _modal: null,
    _closeBtn: null,
    _breadcrumbEl: null,
    _container: null,
    _locationId: null,
    _parentId: null,
    _onFileClick: null,

    init(opts) {
        if (opts && opts.onFileClick) this._onFileClick = opts.onFileClick;
        this._modal = document.getElementById('treemap-modal');
        this._closeBtn = document.getElementById('treemap-close');
        this._breadcrumbEl = document.getElementById('treemap-breadcrumb');
        this._container = document.getElementById('treemap-container');

        this._closeBtn.addEventListener('click', () => this.close());
        this._modal.addEventListener('click', (e) => {
            if (e.target === this._modal) this.close();
        });
        document.addEventListener('keydown', (e) => {
            if (e.key === 'Escape' && !this._modal.classList.contains('hidden')) {
                this.close();
            }
        });
    },

    open(nodeId, locationId) {
        const idStr = String(nodeId);
        if (idStr.startsWith('fld-')) {
            // Folder — need the location ID and start at this folder
            this._locationId = parseInt(String(locationId).replace('loc-', ''), 10);
            this._parentId = parseInt(idStr.replace('fld-', ''), 10);
        } else {
            // Location root
            this._locationId = parseInt(idStr.replace('loc-', ''), 10);
            this._parentId = null;
        }
        this._modal.classList.remove('hidden');
        this._load();
    },

    close() {
        this._modal.classList.add('hidden');
        this._container.innerHTML = '';
        this._breadcrumbEl.innerHTML = '';
    },

    async _load() {
        this._container.innerHTML = '<div class="treemap-loading">Loading...</div>';
        let url = `/api/treemap/${this._locationId}`;
        if (this._parentId !== null) url += `?parent_id=${this._parentId}`;
        const res = await API.get(url);
        if (!res.ok) {
            this._container.innerHTML = '<div class="treemap-loading">Failed to load data.</div>';
            return;
        }
        const data = res.data;
        this._renderBreadcrumb(data.breadcrumb);
        this._renderTreemap(data);
    },

    _renderBreadcrumb(breadcrumb) {
        if (!breadcrumb || breadcrumb.length === 0) {
            this._breadcrumbEl.innerHTML = '';
            return;
        }
        const segments = breadcrumb.map((entry, i) => {
            const isLast = i === breadcrumb.length - 1;
            if (isLast) {
                return `<span class="treemap-bc-segment treemap-bc-current">${entry.name}</span>`;
            }
            const dataId = typeof entry.id === 'string' && entry.id.startsWith('loc-')
                ? 'null' : entry.id;
            return `<span class="treemap-bc-segment treemap-bc-link" data-parent-id="${dataId}">${entry.name}</span>`;
        });
        this._breadcrumbEl.innerHTML = segments.join('<span class="treemap-bc-sep">/</span>');

        this._breadcrumbEl.querySelectorAll('.treemap-bc-link').forEach(el => {
            el.addEventListener('click', () => {
                const pid = el.dataset.parentId;
                this._parentId = pid === 'null' ? null : parseInt(pid, 10);
                this._load();
            });
        });
    },

    _renderTreemap(data) {
        this._container.innerHTML = '';
        const items = [];

        for (const child of data.children) {
            if (child.totalSize > 0) {
                items.push({
                    id: child.id,
                    name: child.name,
                    size: child.totalSize,
                    drillable: child.hasChildren || child.fileCount > 0,
                    fileCount: child.fileCount,
                    isFiles: false,
                });
            }
        }

        // Show individual files that are >= 1% of total, group the rest
        if (data.directFilesSize > 0) {
            const threshold = data.totalSize * 0.01;
            const topFiles = data.directFiles || [];
            let shownSize = 0;
            let shownCount = 0;

            for (const f of topFiles) {
                if (f.size >= threshold) {
                    items.push({
                        id: f.id,
                        name: f.name,
                        size: f.size,
                        drillable: false,
                        isFile: true,
                    });
                    shownSize += f.size;
                    shownCount++;
                }
            }

            const remainingSize = data.directFilesSize - shownSize;
            const remainingCount = data.directFilesCount - shownCount;
            if (remainingSize > 0 && remainingCount > 0) {
                items.push({
                    id: null,
                    name: `[${remainingCount} other file${remainingCount !== 1 ? 's' : ''}]`,
                    size: remainingSize,
                    drillable: false,
                    isFiles: true,
                });
            }
        }

        if (items.length === 0) {
            this._container.innerHTML = '<div class="treemap-loading">No data to display.</div>';
            return;
        }

        // Sort descending by size
        items.sort((a, b) => b.size - a.size);

        const totalSize = items.reduce((s, i) => s + i.size, 0);
        const rect = {
            x: 0,
            y: 0,
            w: this._container.clientWidth,
            h: this._container.clientHeight,
        };

        if (rect.w === 0 || rect.h === 0) return;

        const rects = this._squarify(items, rect, totalSize);
        const colors = this._generateColors(items.length);

        rects.forEach((r, i) => {
            const item = items[i];
            const div = document.createElement('div');
            const classes = ['treemap-rect'];
            if (item.drillable) classes.push('treemap-rect-drillable');
            if (item.isFile) classes.push('treemap-rect-file');
            if (item.isFiles) classes.push('treemap-rect-files');
            div.className = classes.join(' ');
            div.style.left = r.x + 'px';
            div.style.top = r.y + 'px';
            div.style.width = r.w + 'px';
            div.style.height = r.h + 'px';
            div.style.backgroundColor = colors[i];
            div.title = `${item.name}\n${formatSize(item.size)}`;

            const showLabel = r.w > 60 && r.h > 30;
            if (showLabel) {
                const label = document.createElement('span');
                label.className = 'treemap-label';
                label.textContent = item.name;
                div.appendChild(label);

                const sizeLabel = document.createElement('span');
                sizeLabel.className = 'treemap-size';
                sizeLabel.textContent = formatSize(item.size);
                div.appendChild(sizeLabel);
            }

            if (item.drillable) {
                div.addEventListener('click', () => {
                    this._parentId = item.id;
                    this._load();
                });
            } else if (item.isFile && item.id && this._onFileClick) {
                div.addEventListener('click', () => {
                    this.close();
                    this._onFileClick(item.id);
                });
            }

            this._container.appendChild(div);
        });
    },

    _squarify(items, rect, totalSize) {
        const result = [];
        if (items.length === 0 || totalSize === 0) return result;

        const totalArea = rect.w * rect.h;
        const areas = items.map(item => (item.size / totalSize) * totalArea);

        let remaining = { x: rect.x, y: rect.y, w: rect.w, h: rect.h };
        let row = [];
        let rowAreaSum = 0;
        let idx = 0;

        while (idx < areas.length) {
            const area = areas[idx];
            const testRow = [...row, area];
            const testSum = rowAreaSum + area;

            if (row.length === 0) {
                row = [area];
                rowAreaSum = area;
                idx++;
                continue;
            }

            const currentWorst = this._worstRatio(row, rowAreaSum, remaining);
            const testWorst = this._worstRatio(testRow, testSum, remaining);

            if (testWorst <= currentWorst) {
                row.push(area);
                rowAreaSum += area;
                idx++;
            } else {
                const laid = this._layoutRow(row, rowAreaSum, remaining);
                result.push(...laid.rects);
                remaining = laid.remaining;
                row = [];
                rowAreaSum = 0;
            }
        }

        if (row.length > 0) {
            const laid = this._layoutRow(row, rowAreaSum, remaining);
            result.push(...laid.rects);
        }

        return result;
    },

    _worstRatio(row, rowAreaSum, rect) {
        const shorter = Math.min(rect.w, rect.h);
        if (shorter === 0 || rowAreaSum === 0) return Infinity;
        const s2 = shorter * shorter;
        let worst = 0;
        for (const area of row) {
            const r1 = (s2 * area) / (rowAreaSum * rowAreaSum);
            const r2 = (rowAreaSum * rowAreaSum) / (s2 * area);
            worst = Math.max(worst, Math.max(r1, r2));
        }
        return worst;
    },

    _layoutRow(row, rowAreaSum, rect) {
        const rects = [];
        const horizontal = rect.w >= rect.h;
        const shorter = horizontal ? rect.h : rect.w;

        if (shorter === 0 || rowAreaSum === 0) {
            row.forEach(() => rects.push({ x: rect.x, y: rect.y, w: 0, h: 0 }));
            return { rects, remaining: rect };
        }

        const rowThickness = rowAreaSum / shorter;
        let offset = 0;

        for (const area of row) {
            const length = area / rowThickness;
            if (horizontal) {
                rects.push({
                    x: rect.x,
                    y: rect.y + offset,
                    w: rowThickness,
                    h: length,
                });
            } else {
                rects.push({
                    x: rect.x + offset,
                    y: rect.y,
                    w: length,
                    h: rowThickness,
                });
            }
            offset += length;
        }

        let remaining;
        if (horizontal) {
            remaining = {
                x: rect.x + rowThickness,
                y: rect.y,
                w: rect.w - rowThickness,
                h: rect.h,
            };
        } else {
            remaining = {
                x: rect.x,
                y: rect.y + rowThickness,
                w: rect.w,
                h: rect.h - rowThickness,
            };
        }

        return { rects, remaining };
    },

    _generateColors(count) {
        const style = getComputedStyle(document.documentElement);
        const palette = [];
        for (let i = 1; i <= 8; i++) {
            const c = style.getPropertyValue(`--treemap-color-${i}`).trim();
            if (c) palette.push(c);
        }
        if (palette.length === 0) palette.push('#4a7aba');
        const colors = [];
        for (let i = 0; i < count; i++) {
            colors.push(palette[i % palette.length]);
        }
        return colors;
    },
};

export default Treemap;
