import API from '../api.js';
import StatusBar from './statusbar.js';
import Toast from './toast.js';

const Upload = {
    getTarget: null,
    fileInput: null,

    init(getTarget) {
        this.getTarget = getTarget;

        // Hidden file input
        this.fileInput = document.createElement('input');
        this.fileInput.type = 'file';
        this.fileInput.multiple = true;
        this.fileInput.style.display = 'none';
        document.body.appendChild(this.fileInput);

        this.fileInput.addEventListener('change', () => {
            if (this.fileInput.files.length > 0) {
                this.doUpload(this.fileInput.files);
            }
            this.fileInput.value = '';
        });

        // Upload button
        const btn = document.getElementById('btn-upload');
        if (btn) {
            btn.addEventListener('click', () => {
                const target = this.getTarget();
                if (target && target.online !== false) {
                    this.fileInput.click();
                }
            });
        }

        // Drag-and-drop on file panel
        const filePanel = document.getElementById('file-panel');
        if (filePanel) {
            filePanel.addEventListener('dragover', (e) => {
                e.preventDefault();
                if (e.dataTransfer.types.includes('application/x-filehunter-move')) return;
                const target = this.getTarget();
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
                // Ignore internal file-move drags
                if (e.dataTransfer.types.includes('application/x-filehunter-move')) return;
                const target = this.getTarget();
                if (!target || target.online === false) {
                    Toast.error('Select an online location or folder first.');
                    return;
                }
                if (e.dataTransfer.files.length > 0) {
                    this.doUpload(e.dataTransfer.files);
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

    async doUpload(fileList) {
        const target = this.getTarget();
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
