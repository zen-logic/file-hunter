const WS = {
    socket: null,
    listeners: {},
    reconnectScheduled: false,

    connect() {
        const protocol = location.protocol === 'https:' ? 'wss:' : 'ws:';
        const token = localStorage.getItem('fh-token') || '';
        this.socket = new WebSocket(`${protocol}//${location.host}/ws?token=${encodeURIComponent(token)}`);
        this.reconnectScheduled = false;

        this.socket.onopen = () => {
            const handlers = this.listeners['__open'] || [];
            handlers.forEach(fn => fn());
        };

        this.socket.onmessage = (event) => {
            const msg = JSON.parse(event.data);
            const handlers = this.listeners[msg.type] || [];
            handlers.forEach(fn => fn(msg));
        };

        this.socket.onclose = () => {
            const handlers = this.listeners['__close'] || [];
            handlers.forEach(fn => fn());
            this.scheduleReconnect();
        };

        this.socket.onerror = () => {
            this.scheduleReconnect();
        };
    },

    scheduleReconnect() {
        if (this.reconnectScheduled) return;
        this.reconnectScheduled = true;
        setTimeout(() => this.connect(), 3000);
    },

    on(type, fn) {
        if (!this.listeners[type]) this.listeners[type] = [];
        this.listeners[type].push(fn);
    },
};

export default WS;
