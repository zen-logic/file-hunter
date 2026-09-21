const API = {
    baseUrl: '',

    headers() {
        const h = { 'Content-Type': 'application/json' };
        const token = localStorage.getItem('fh-token');
        if (token) h['Authorization'] = `Bearer ${token}`;
        return h;
    },

    checkAuth(res, path) {
        if (res.status === 401 && !path.startsWith('/api/auth/')) {
            localStorage.removeItem('fh-token');
            location.reload();
        }
    },

    async get(path, { signal } = {}) {
        const res = await fetch(`${this.baseUrl}${path}`, {
            headers: this.headers(),
            signal,
        });
        this.checkAuth(res, path);
        return res.json();
    },

    async post(path, data) {
        const res = await fetch(`${this.baseUrl}${path}`, {
            method: 'POST',
            headers: this.headers(),
            body: JSON.stringify(data),
        });
        this.checkAuth(res, path);
        return res.json();
    },

    async patch(path, data) {
        const res = await fetch(`${this.baseUrl}${path}`, {
            method: 'PATCH',
            headers: this.headers(),
            body: JSON.stringify(data),
        });
        this.checkAuth(res, path);
        return res.json();
    },

    async delete(path) {
        const res = await fetch(`${this.baseUrl}${path}`, {
            method: 'DELETE',
            headers: this.headers(),
        });
        this.checkAuth(res, path);
        return res.json();
    },

    upload(formData, onProgress) {
        return new Promise((resolve) => {
            const xhr = new XMLHttpRequest();
            xhr.open('POST', `${this.baseUrl}/api/upload`);
            const token = localStorage.getItem('fh-token');
            if (token) xhr.setRequestHeader('Authorization', `Bearer ${token}`);
            if (onProgress) {
                xhr.upload.addEventListener('progress', (e) => {
                    if (e.lengthComputable) onProgress(e.loaded, e.total);
                });
            }
            xhr.addEventListener('load', () => {
                if (xhr.status === 401) {
                    localStorage.removeItem('fh-token');
                    location.reload();
                    return;
                }
                try { resolve(JSON.parse(xhr.responseText)); }
                catch { resolve({ ok: false, error: 'Invalid response' }); }
            });
            xhr.addEventListener('error', () => resolve({ ok: false, error: 'Network error' }));
            xhr.send(formData);
        });
    },
};

export default API;
