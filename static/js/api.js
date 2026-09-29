const API = {
    baseUrl: '',

    /** The Authorization header for the signed-in user, or none. */
    authHeaders() {
        const token = localStorage.getItem('fh-token');
        return token ? { 'Authorization': `Bearer ${token}` } : {};
    },

    /** The URL with the token as a query parameter, for requests the browser
     *  makes itself (img src, download links), which can't carry a header. */
    authUrl(url) {
        const token = localStorage.getItem('fh-token');
        return token ? `${url}${url.includes('?') ? '&' : '?'}token=${encodeURIComponent(token)}` : url;
    },

    /** Download a URL as filename through a temporary link. */
    download(url, filename) {
        const a = document.createElement('a');
        a.href = this.authUrl(url);
        a.download = filename || '';
        document.body.appendChild(a);
        a.click();
        a.remove();
    },

    headers() {
        return { 'Content-Type': 'application/json', ...this.authHeaders() };
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
            for (const [k, v] of Object.entries(this.authHeaders())) xhr.setRequestHeader(k, v);
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
