const STORAGE_KEY = 'fh-activity-log-open';

function ts() {
    const d = new Date();
    const hh = String(d.getHours()).padStart(2, '0');
    const mm = String(d.getMinutes()).padStart(2, '0');
    const ss = String(d.getSeconds()).padStart(2, '0');
    return `${hh}:${mm}:${ss}`;
}

const ActivityLog = {
    el: null,
    listEl: null,
    toggleEl: null,
    indicatorEl: null,
    open: false,
    autoScroll: true,

    init() {
        this.el = document.getElementById('activity-log');
        this.listEl = document.getElementById('activity-log-list');
        this.toggleEl = document.getElementById('activity-log-toggle');

        // "New activity" indicator
        const ind = document.createElement('div');
        ind.className = 'activity-new-indicator hidden';
        ind.textContent = 'New activity \u25BE';
        ind.addEventListener('click', () => this.scrollToBottom());
        this.el.appendChild(ind);
        this.indicatorEl = ind;

        this.open = localStorage.getItem(STORAGE_KEY) !== 'false';
        this.apply();

        this.toggleEl.addEventListener('click', () => {
            this.open = !this.open;
            localStorage.setItem(STORAGE_KEY, this.open);
            this.apply();
        });

        this.listEl.addEventListener('scroll', () => this.onScroll());
    },

    apply() {
        this.el.classList.toggle('collapsed', !this.open);
        this.toggleEl.textContent = this.open ? 'Activity \u25BE' : 'Activity \u25B8';
    },

    onScroll() {
        if (this.ignoreScroll) return;
        const el = this.listEl;
        if (el.scrollHeight <= el.clientHeight) {
            this.autoScroll = true;
            return;
        }
        const atBottom = el.scrollHeight - el.scrollTop - el.clientHeight < 8;
        this.autoScroll = atBottom;
        if (atBottom) {
            this.indicatorEl.classList.add('hidden');
        } else {
            this.indicatorEl.classList.remove('hidden');
        }
    },

    scrollToBottom() {
        this.autoScroll = true;
        this.ignoreScroll = true;
        this.listEl.scrollTop = this.listEl.scrollHeight;
        this.indicatorEl.classList.add('hidden');
        requestAnimationFrame(() => { this.ignoreScroll = false; });
    },

    add(text) {
        const row = document.createElement('div');
        row.className = 'activity-entry';
        row.innerHTML = `<span class="activity-ts">${ts()}</span> ${text}`;
        this.listEl.appendChild(row);
        while (this.listEl.childElementCount > 100) {
            this.listEl.removeChild(this.listEl.firstElementChild);
        }
        if (this.autoScroll) {
            this.ignoreScroll = true;
            this.listEl.scrollTop = this.listEl.scrollHeight;
            requestAnimationFrame(() => { this.ignoreScroll = false; });
        } else if (this.listEl.scrollHeight > this.listEl.clientHeight) {
            this.indicatorEl.classList.remove('hidden');
        }
    },
};

export default ActivityLog;
