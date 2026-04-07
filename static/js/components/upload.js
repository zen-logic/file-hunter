import API from '../api.js';
import StatusBar from './statusbar.js';
import Toast from './toast.js';

const Upload = {
    _getTarget: null,
    _fileInput: null,

    init(getTarget) {
        this._getTarget = getTarget;

        // Hidden file input
        this._fileInput = document.createElement('input');
        this._fileInput.type = 'file';
        this._fileInput.multiple = true;
        this._fileInput.style.display = 'none';
        document.body.appendChild(this._fileInput);

        this._fileInput.addEventListener('change', () => {
            if (this._fileInput.files.length > 0) {
                this._doUpload(this._fileInput.files);
            }
            this._fileInput.value = '';
        });

        // Upload button
        const btn = document.getElementById('btn-upload');
        if (btn) {
            btn.addEventListener('click', () => {
                const target = this._getTarget();
                if (target && target.online !== false) {
                    this._fileInput.click();
                }
            });
        }

        // Drag-and-drop on file panel
        const filePanel = document.getElementById('file-panel');
        if (filePanel) {
            filePanel.addEventListener('dragover', (e) => {
                e.preventDefault();
                const target = this._getTarget();
                if (target && target.online !== false) {
                    e.dataTransfer.dropEffect = 'copy';
                    filePanel.classList.add('drop-active');
                } else {
                    e.dataTransfer.dropEffect = 'none';
                }
            });

            filePanel.addEventListener('dragleave', (e) => {
                // Only remove if leaving the panel (not entering a child)
                if (!filePanel.contains(e.relatedTarget)) {
                    filePanel.classList.remove('drop-active');
                }
            });

            filePanel.addEventListener('drop', (e) => {
                e.preventDefault();
                filePanel.classList.remove('drop-active');
                const target = this._getTarget();
                if (!target || target.online === false) {
                    Toast.error('Select an online location or folder first.');
                    return;
                }
                if (e.dataTransfer.files.length > 0) {
                    this._doUpload(e.dataTransfer.files);
                }
            });
        }

        // Block browser default drag behavior on #app
        const app = document.getElementById('app');
        if (app) {
            app.addEventListener('dragover', (e) => e.preventDefault());
            app.addEventListener('drop', (e) => e.preventDefault());
        }
    },

    updateState(node) {
        const btn = document.getElementById('btn-upload');
        if (btn) {
            btn.disabled = !node || node.online === false;
        }
    },

    async _doUpload(fileList) {
        const target = this._getTarget();
        if (!target) {
            Toast.error('No location selected.');
            return;
        }

        const formData = new FormData();
        formData.append('target_id', target.id);
        const mtimes = [];
        let totalSize = 0;
        for (const file of fileList) {
            formData.append('files', file);
            mtimes.push(file.lastModified);
            totalSize += file.size;
        }
        formData.append('mtimes', JSON.stringify(mtimes));

        const fileCount = fileList.length;
        const sizeMB = (totalSize / 1048576).toFixed(1);
        StatusBar.renderActivity('uploading', `sending ${fileCount} file(s) (${sizeMB} MB)...`);

        const res = await API.upload(formData, (loaded, total) => {
            const pct = Math.round((loaded / total) * 100);
            const sentMB = (loaded / 1048576).toFixed(1);
            StatusBar.renderActivity('uploading', `sending ${fileCount} file(s) — ${sentMB}/${sizeMB} MB (${pct}%)`);
        });

        if (!res.ok) {
            StatusBar.renderActivity('idle');
            Toast.error(res.error || 'Upload failed.');
        }
        // On success, the WS upload_started/upload_progress/upload_completed handlers take over
    },
};

export default Upload;
