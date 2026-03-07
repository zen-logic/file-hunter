import API from './api.js';
import Tree from './components/tree.js';
import FileList from './components/filelist.js';
import Detail from './components/detail.js';
import StatusBar from './components/statusbar.js';
import Search from './components/search.js';
import AddLocationModal from './components/addlocation.js';
import ScanConfirm from './components/scanconfirm.js';
import ConfirmModal from './components/confirm.js';
import Consolidate from './components/consolidate.js';
import Merge from './components/merge.js';
import DeleteLocationModal from './components/deletelocation.js';
import DeleteFileModal from './components/deletefile.js';
import RenameLocationModal from './components/renamelocation.js';
import NewFolderModal from './components/newfolder.js';
import RenameFileModal from './components/renamefile.js';
import MoveFileModal from './components/movefile.js';
import IgnoreFileModal from './components/ignorefile.js';
import Treemap from './components/treemap.js';
import Login from './components/login.js';
import Settings from './components/settings.js';
import About from './components/about.js';
import ActivityLog from './components/activitylog.js';
import Toast from './components/toast.js';
import Upload from './components/upload.js';
import SlideshowTriage from './components/slideshow-triage.js';
import Keyboard from './keyboard.js';
import WS from './ws.js';

let currentUser = null;
let selectedNode = null;
let selectedFile = null;
let selectedFileDups = [];
const scanBtn = document.getElementById('btn-scan');
const consolidateBtn = document.getElementById('btn-consolidate');
const uploadBtn = document.getElementById('btn-upload');

async function refreshDetailPanel() {
    if (selectedFile) {
        const result = await Detail.renderFile(selectedFile);
        selectedFileDups = Detail.getFileDups();
        consolidateBtn.disabled = selectedFileDups.length === 0;
        if (selectedFile.type === 'folder') {
            wireMergeBtn(selectedFile);
            wireNewFolderBtn(selectedFile);
            wireDownloadZipBtn(selectedFile);
            wireMoveFolder(selectedFile);
            wireDeleteFolderBtn(selectedFile);
        } else {
            wireDeleteFileBtn();
            wireRenameFileBtn();
            wireMoveFileBtn();
            wireIgnoreFileBtn();
        }
        if (result) updateLocationOnline(result.locationId, result.locationOnline);
    } else if (selectedNode) {
        if (selectedNode.type === 'location') {
            const result = await Detail.renderLocation(selectedNode);
            wireDeleteLocationBtn();
            wireRenameLocationBtn();
            wireNewFolderBtn();
            wireDownloadZipBtn();
            wireMergeBtn();
            wireTreemapBtn();
            if (result && result.online !== undefined && result.online !== selectedNode.online) {
                selectedNode.online = result.online;
                Tree.render();
            }
        } else {
            const result = await Detail.renderFolder(selectedNode);
            wireNewFolderBtn();
            wireDownloadZipBtn();
            wireMergeBtn();
            wireMoveFolder();
            wireDeleteFolderBtn();
            if (result) updateLocationOnline(result.locationId, result.locationOnline);
        }
    } else {
        await Detail.renderDashboard();
    }
}

function updateLocationOnline(locationId, online) {
    if (!locationId || online === undefined) return;
    const locNode = Tree.getLocation(locationId);
    if (locNode && locNode.online !== online) {
        locNode.online = online;
        Tree.render();
    }
}

function wireDeleteLocationBtn() {
    const btn = document.getElementById('detail-delete-location');
    if (btn && selectedNode) {
        btn.addEventListener('click', () => DeleteLocationModal.open(selectedNode));
    }
}

function wireRenameLocationBtn() {
    const btn = document.getElementById('detail-rename-location');
    if (btn && selectedNode) {
        btn.addEventListener('click', () => RenameLocationModal.open(selectedNode));
    }
}

function wireMergeBtn(node) {
    const btn = document.getElementById('detail-merge-btn');
    const target = node || selectedNode;
    if (btn && target) {
        btn.addEventListener('click', () => Merge.open(target));
    }
}

function wireNewFolderBtn(node) {
    const btn = document.getElementById('detail-new-folder');
    const target = node || selectedNode;
    if (btn && target) {
        btn.addEventListener('click', () => NewFolderModal.open(target));
    }
}

function wireDownloadZipBtn(node) {
    const btn = document.getElementById('detail-download-zip');
    const target = node || selectedNode;
    if (btn && target) {
        btn.addEventListener('click', async () => {
            const isLoc = String(target.id).startsWith('loc-');
            const numId = String(target.id).replace(/^(loc-|fld-)/, '');
            const url = isLoc ? `/api/locations/${numId}/download` : `/api/folders/${numId}/download`;
            const dlHeaders = {};
            const dlToken = localStorage.getItem('fh-token');
            if (dlToken) dlHeaders['Authorization'] = `Bearer ${dlToken}`;
            btn.disabled = true;
            const origText = btn.textContent;
            btn.textContent = 'Downloading\u2026';
            try {
                const resp = await fetch(url, { headers: dlHeaders });
                if (resp.ok) {
                    const blob = await resp.blob();
                    const blobUrl = URL.createObjectURL(blob);
                    const a = document.createElement('a');
                    a.href = blobUrl;
                    a.download = '';
                    document.body.appendChild(a);
                    a.click();
                    a.remove();
                    URL.revokeObjectURL(blobUrl);
                }
            } catch { /* ignore */ }
            btn.disabled = false;
            btn.textContent = origText;
        });
    }
}

function wireRenameFileBtn() {
    const btn = document.getElementById('detail-rename-file');
    if (btn && selectedFile) {
        btn.addEventListener('click', () => {
            RenameFileModal.open(selectedFile);
        });
    }
}

function wireMoveFileBtn() {
    const btn = document.getElementById('detail-move-file');
    if (btn && selectedFile) {
        btn.addEventListener('click', () => {
            MoveFileModal.open(selectedFile);
        });
    }
}

function wireDeleteFileBtn() {
    const btn = document.getElementById('detail-delete-file');
    if (btn && selectedFile) {
        btn.addEventListener('click', () => {
            const lastDetail = Detail._lastDetail || {};
            DeleteFileModal.open({
                type: 'file',
                id: selectedFile.id,
                name: selectedFile.name,
                offline: lastDetail.locationOnline === false,
                dupCount: selectedFileDups.length,
            });
        });
    }
}

function wireIgnoreFileBtn() {
    const btn = document.getElementById('detail-ignore-file');
    if (btn && selectedFile) {
        btn.addEventListener('click', () => {
            const lastDetail = Detail._lastDetail || {};
            const locIdStr = lastDetail.locationId || '';
            const locId = locIdStr.replace('loc-', '');
            IgnoreFileModal.open({
                filename: lastDetail.name || selectedFile.name,
                file_size: lastDetail.size != null ? lastDetail.size : (selectedFile.size || 0),
                locationId: locId ? parseInt(locId, 10) : null,
                locationName: lastDetail.locationName || null,
            });
        });
    }
}

function wireDeleteFolderBtn(node) {
    const btn = document.getElementById('detail-delete-folder');
    const target = node || selectedNode;
    if (btn && target) {
        btn.addEventListener('click', () => {
            DeleteFileModal.open({
                type: 'folder',
                id: target.id,
                name: target.name || target.label,
            });
        });
    }
}

function wireMoveFolder(node) {
    const btn = document.getElementById('detail-move-folder');
    const target = node || selectedNode;
    if (btn && target) {
        btn.addEventListener('click', () => {
            MoveFileModal.open(
                { id: target.id, name: target.name || target.label },
                target.id
            );
        });
    }
}

function wireTreemapBtn() {
    const btn = document.getElementById('detail-treemap-btn');
    if (btn && selectedNode) {
        btn.addEventListener('click', () => Treemap.open(selectedNode.id));
    }
}

function wireBatchActions(items) {
    const fileIds = items.filter(i => i.type !== 'folder').map(i => i.id);
    const folderIds = items.filter(i => i.type === 'folder').map(i => {
        // Folder ids come as "fld-N" — extract numeric part
        return parseInt(String(i.id).replace('fld-', ''), 10);
    });

    // Batch delete
    const deleteBtn = document.getElementById('batch-delete-btn');
    if (deleteBtn) {
        deleteBtn.addEventListener('click', () => {
            DeleteFileModal.open({
                type: 'batch',
                name: `${items.length} items`,
                batchFileIds: fileIds,
                batchFolderIds: folderIds,
            });
        });
    }

    // Batch move
    const moveBtn = document.getElementById('batch-move-btn');
    if (moveBtn) {
        moveBtn.addEventListener('click', () => {
            MoveFileModal.open({
                id: 'batch',
                name: `${items.length} items`,
                batchFileIds: fileIds,
                batchFolderIds: folderIds,
            });
        });
    }

    // Batch download ZIP
    const downloadBtn = document.getElementById('batch-download-btn');
    if (downloadBtn) {
        downloadBtn.addEventListener('click', async () => {
            downloadBtn.disabled = true;
            downloadBtn.textContent = 'Downloading\u2026';
            try {
                const batchHeaders = { 'Content-Type': 'application/json' };
                const batchToken = localStorage.getItem('fh-token');
                if (batchToken) batchHeaders['Authorization'] = `Bearer ${batchToken}`;
                const resp = await fetch('/api/batch/download', {
                    method: 'POST',
                    headers: batchHeaders,
                    body: JSON.stringify({ file_ids: fileIds, folder_ids: folderIds }),
                });
                if (resp.ok) {
                    const blob = await resp.blob();
                    const url = URL.createObjectURL(blob);
                    const a = document.createElement('a');
                    a.href = url;
                    a.download = 'selection.zip';
                    document.body.appendChild(a);
                    a.click();
                    a.remove();
                    URL.revokeObjectURL(url);
                } else {
                    Toast.error('Download failed.');
                }
            } catch {
                Toast.error('Download failed.');
            }
            downloadBtn.disabled = false;
            downloadBtn.textContent = 'Download ZIP';
        });
    }

    // Batch tag
    const tagAddBtn = document.getElementById('batch-tag-add');
    const tagInput = document.getElementById('batch-tag-input');
    if (tagAddBtn && tagInput) {
        const addTag = async () => {
            const tag = tagInput.value.trim();
            if (!tag || fileIds.length === 0) return;
            const res = await API.post('/api/batch/tag', {
                file_ids: fileIds,
                add_tags: [tag],
                remove_tags: [],
            });
            if (res.ok) {
                Toast.success(`Tag "${tag}" added to ${res.data.updated} file${res.data.updated !== 1 ? 's' : ''}`);
                tagInput.value = '';
            } else {
                Toast.error(res.error || 'Failed to add tag.');
            }
        };
        tagAddBtn.addEventListener('click', addTag);
        tagInput.addEventListener('keydown', (e) => {
            if (e.key === 'Enter') addTag();
        });
    }
}

function startApp(user) {
    currentUser = user;
    StatusBar.init();
    ActivityLog.init();
    Upload.init(() => selectedNode || null);
    Keyboard.init();
    Keyboard.setSearchToggle(() => Search.toggle());
    Treemap.init({
        async onFileClick(fileId) {
            Search.close();
            const res = await API.get(`/api/files/${fileId}`);
            if (!res.ok) return;
            const detail = res.data;
            const folderId = detail.folderId || detail.locationId;
            if (folderId) {
                const node = await Tree.revealNode(folderId);
                if (node) {
                    selectedNode = node;
                    scanBtn.disabled = false;
                    Upload.updateState(node);
                }
            }
            const fileItem = {
                id: detail.id,
                name: detail.name,
                typeHigh: detail.typeHigh,
                typeLow: detail.typeLow,
                size: detail.size,
                date: detail.date,
                dups: (detail.duplicates || []).length,
            };
            FileList.showSingleFile(fileItem);
            selectedFile = fileItem;
            await Detail.renderFile(fileItem);
            selectedFileDups = Detail.getFileDups();
            consolidateBtn.disabled = selectedFileDups.length === 0;
            wireDeleteFileBtn();
            wireRenameFileBtn();
            wireMoveFileBtn();
            wireIgnoreFileBtn();
            if (detail.locationId) updateLocationOnline(detail.locationId, detail.locationOnline);
        },
    });
    Keyboard.setSelectAllHandler(() => FileList._selectAll());

    // About dialog
    About.init();
    document.getElementById('app-title').addEventListener('click', () => About.open());

    // Settings gear button
    document.getElementById('btn-settings').addEventListener('click', () => Settings.open(currentUser));
    document.getElementById('settings-close').addEventListener('click', () => Settings.close());
    // Close settings on overlay click
    document.getElementById('settings-modal').addEventListener('click', (e) => {
        if (e.target.id === 'settings-modal') Settings.close();
    });

    // Apply server name to title
    API.get('/api/settings').then(res => {
        if (res.ok && res.data.serverName) {
            document.title = `File Hunter \u2014 ${res.data.serverName}`;
            document.getElementById('app-title').textContent = `File Hunter \u2014 ${res.data.serverName}`;
        }
    });

ConfirmModal.init();

ScanConfirm.init(async (locationNode, folderNode) => {
    const payload = { location_id: locationNode.id };
    if (folderNode) payload.folder_id = folderNode.id;
    const res = await API.post('/api/scan', payload);
    if (!res.ok) Toast.error(res.error || 'Failed to start scan.');
});

scanBtn.addEventListener('click', () => {
    if (!selectedNode) return;
    const loc = Tree.getLocation(selectedNode.id);
    if (!loc) return;
    const isFolder = String(selectedNode.id).startsWith('fld-');
    ScanConfirm.open(loc, isFolder ? selectedNode : null);
});

Consolidate.init(async ({ file_id, mode, destination_folder_id }) => {
    const payload = { file_id, mode };
    if (destination_folder_id) payload.destination_folder_id = destination_folder_id;
    await API.post('/api/consolidate', payload);
});

consolidateBtn.addEventListener('click', async () => {
    if (selectedFile && selectedFileDups.length > 0) {
        await Consolidate.open(selectedFile, selectedFileDups);
    }
});

SlideshowTriage.init();
Detail.slideshowTriage = SlideshowTriage;

Merge.init(async ({ source_id, destination_id }) => {
    await API.post('/api/merge', { source_id, destination_id });
});

DeleteLocationModal.init(async (node) => {
    const locId = node.id.replace('loc-', '');
    const res = await API.delete(`/api/locations/${locId}`);
    if (res.ok) {
        ActivityLog.add(`Location deleted: <b>${node.label}</b>`);
        Toast.success(`Location deleted: ${node.label}`);
        selectedNode = null;
        selectedFile = null;
        selectedFileDups = [];
        scanBtn.disabled = true;
        consolidateBtn.disabled = true;
        Upload.updateState(null);
        FileList.renderEmpty();
        await Detail.renderDashboard();
        await Tree.reload();
        await StatusBar.loadStats();
    }
});

DeleteFileModal.init(async (item) => {
    if (item.type === 'batch') {
        const res = await API.post('/api/batch/delete', {
            file_ids: item.batchFileIds,
            folder_ids: item.batchFolderIds,
        });
        if (res.ok) {
            const d = res.data;
            ActivityLog.add(`Batch deleted: <b>${d.deleted_files} files, ${d.deleted_folders} folders</b>`);
            Toast.success(`Deleted ${d.deleted_files} files, ${d.deleted_folders} folders`);
            selectedFile = null;
            selectedFileDups = [];
            consolidateBtn.disabled = true;
            await Tree.reload();
            if (selectedNode) {
                await FileList.showFolder(selectedNode.id);
            }
            await StatusBar.loadStats();
            await refreshDetailPanel();
        }
    } else if (item.type === 'folder') {
        const folderId = String(item.id).replace('fld-', '');
        const res = await API.delete(`/api/folders/${folderId}`);
        if (res.ok) {
            ActivityLog.add(`Folder deleted: <b>${item.name || item.label}</b> (${res.data.file_count} files)`);
            Toast.success(`Deleted folder: ${item.name || item.label}`);
            selectedFile = null;
            selectedFileDups = [];
            consolidateBtn.disabled = true;
            if (selectedNode && (selectedNode.id === item.id || selectedNode.id === `fld-${folderId}`)) {
                selectedNode = null;
                scanBtn.disabled = true;
                Upload.updateState(null);
                FileList.renderEmpty();
            } else if (selectedNode) {
                await FileList.showFolder(selectedNode.id);
            }
            await Tree.reload();
            await StatusBar.loadStats();
            await refreshDetailPanel();
        }
    } else {
        const url = item.deleteAllDuplicates
            ? `/api/files/${item.id}?all_duplicates=true`
            : `/api/files/${item.id}`;
        const res = await API.delete(url);
        if (res.ok) {
            if (item.deleteAllDuplicates && res.data.deleted_count > 1) {
                ActivityLog.add(`File deleted: <b>${item.name}</b> and ${res.data.deleted_count - 1} duplicate(s)`);
                Toast.success(`Deleted ${item.name} and ${res.data.deleted_count - 1} duplicate(s)`);
            } else {
                ActivityLog.add(`File deleted: <b>${item.name}</b>`);
                Toast.success(`Deleted: ${item.name}`);
            }
            selectedFile = null;
            selectedFileDups = [];
            consolidateBtn.disabled = true;
            if (selectedNode) {
                await FileList.showFolder(selectedNode.id);
            }
            await StatusBar.loadStats();
            await refreshDetailPanel();
        }
    }
});

IgnoreFileModal.init(async ({ filename, file_size, location_id }) => {
    const res = await API.post('/api/ignore', { filename, file_size, location_id });
    if (res.ok) {
        const scope = location_id ? 'this location' : 'all locations';
        Toast.success(`Ignore rule created for "${filename}" (${scope})`);
        await refreshDetailPanel();
    } else {
        Toast.error(res.error || 'Failed to create ignore rule.');
    }
});

RenameLocationModal.init(async (node, newName) => {
    const locId = node.id.replace('loc-', '');
    const res = await API.patch(`/api/locations/${locId}`, { name: newName });
    if (!res.ok) {
        return { error: res.error || 'Rename failed.' };
    }
    ActivityLog.add(`Location renamed: <b>${node.label}</b> &rarr; <b>${newName}</b>`);
    Toast.success(`Location renamed to ${newName}`);
    if (selectedNode && selectedNode.id === node.id) {
        selectedNode.label = newName;
    }
    await Tree.reload();
    if (selectedNode) {
        await Detail.renderLocation(selectedNode);
        wireDeleteLocationBtn();
        wireRenameLocationBtn();
        wireNewFolderBtn();
        wireDownloadZipBtn();
        wireMergeBtn();
        wireTreemapBtn();
    }
    return { ok: true };
});

NewFolderModal.init(async (parentNode, name) => {
    const res = await API.post('/api/folders', { parent_id: parentNode.id, name });
    if (!res.ok) {
        return { error: res.error || 'Failed to create folder.' };
    }
    ActivityLog.add(`Folder created: <b>${name}</b>`);
    Toast.success(`Folder created: ${name}`);
    await Tree.reload();
    if (selectedNode) {
        await FileList.showFolder(selectedNode.id);
    }
    await StatusBar.loadStats();
    return { ok: true };
});

RenameFileModal.init(async (file, newName) => {
    const res = await API.post(`/api/files/${file.id}/move`, { name: newName });
    if (!res.ok) {
        return { error: res.error || 'Rename failed.' };
    }
    ActivityLog.add(`File renamed: <b>${file.name}</b> &rarr; <b>${newName}</b>`);
    Toast.success(`File renamed to ${newName}`);
    if (selectedNode) {
        await FileList.showFolder(selectedNode.id);
    }
    if (selectedFile && selectedFile.id === file.id) {
        selectedFile.name = newName;
        await refreshDetailPanel();
    }
    return { ok: true };
});

MoveFileModal.init(async (item, destinationFolderId) => {
    if (item.id === 'batch') {
        const res = await API.post('/api/batch/move', {
            file_ids: item.batchFileIds,
            folder_ids: item.batchFolderIds,
            destination_folder_id: destinationFolderId,
        });
        if (!res.ok) {
            return { error: res.error || 'Batch move failed.' };
        }
        const d = res.data;
        ActivityLog.add(`Batch moved: <b>${d.moved_files} files, ${d.moved_folders} folders</b>`);
        Toast.success(`Moved ${d.moved_files} files, ${d.moved_folders} folders`);
        selectedFile = null;
        selectedFileDups = [];
        consolidateBtn.disabled = true;
        await Tree.reload();
        if (selectedNode) {
            await FileList.showFolder(selectedNode.id);
        }
        await StatusBar.loadStats();
        await refreshDetailPanel();
        return { ok: true };
    }

    const itemId = String(item.id);
    if (itemId.startsWith('fld-')) {
        const numId = itemId.replace('fld-', '');
        const res = await API.post(`/api/folders/${numId}/move`, { destination_parent_id: destinationFolderId });
        if (!res.ok) {
            return { error: res.error || 'Move failed.' };
        }
        ActivityLog.add(`Folder moved: <b>${item.name || item.label}</b>`);
        Toast.success(`Folder moved: ${item.name || item.label}`);
        selectedFile = null;
        selectedFileDups = [];
        consolidateBtn.disabled = true;
        await Tree.reload();
        if (selectedNode) {
            await FileList.showFolder(selectedNode.id);
        }
        await StatusBar.loadStats();
        await refreshDetailPanel();
        return { ok: true };
    }

    const res = await API.post(`/api/files/${item.id}/move`, { destination_folder_id: destinationFolderId });
    if (!res.ok) {
        return { error: res.error || 'Move failed.' };
    }
    ActivityLog.add(`File moved: <b>${item.name}</b>`);
    Toast.success(`File moved: ${item.name}`);
    selectedFile = null;
    selectedFileDups = [];
    consolidateBtn.disabled = true;
    await Tree.reload();
    if (selectedNode) {
        await FileList.showFolder(selectedNode.id);
    }
    await StatusBar.loadStats();
    await refreshDetailPanel();
    return { ok: true };
});

function wireSlideshowBtn() {
    const slot = document.getElementById('detail-slideshow-slot');
    if (!slot) return;
    // Check current page for any images to decide whether to show button
    const hasImages = (FileList.currentItems || []).some(f => (f.typeHigh || '').toLowerCase() === 'image');
    if (!hasImages) return;
    const folderId = FileList.currentFolder;
    if (!folderId) return;
    slot.innerHTML = `<button class="btn" id="detail-slideshow">Slideshow</button>`;
    document.getElementById('detail-slideshow').addEventListener('click', async () => {
        const btn = document.getElementById('detail-slideshow');
        btn.disabled = true;
        btn.textContent = 'Loading\u2026';
        await Detail.startSlideshow({ type: 'folder', folderId });
        if (Detail._slideshowTotal === 0) {
            btn.textContent = 'No images available';
            setTimeout(() => { btn.textContent = 'Slideshow'; btn.disabled = false; }, 2000);
        }
    });
}

Tree.init(async (node) => {
    selectedNode = node;
    selectedFile = null;
    selectedFileDups = [];
    scanBtn.disabled = false;
    consolidateBtn.disabled = true;
    Upload.updateState(node);
    Search.setScopeContext(node);
    Search.close();
    const detailPromise = node.type === 'location'
        ? Detail.renderLocation(node)
        : Detail.renderFolder(node);
    const [, result] = await Promise.all([
        FileList.showFolder(node.id),
        detailPromise,
    ]);
    if (node.type === 'location') {
        wireDeleteLocationBtn();
        wireRenameLocationBtn();
        wireNewFolderBtn();
        wireDownloadZipBtn();
        wireMergeBtn();
        wireTreemapBtn();
        if (result && result.online !== undefined && result.online !== node.online) {
            node.online = result.online;
            Tree.render();
        }
    } else {
        wireNewFolderBtn();
        wireDownloadZipBtn();
        wireMergeBtn();
        wireMoveFolder();
        wireDeleteFolderBtn();
        if (result) updateLocationOnline(result.locationId, result.locationOnline);
    }
    wireSlideshowBtn();
}, () => {
    selectedNode = null;
    selectedFile = null;
    selectedFileDups = [];
    scanBtn.disabled = true;
    consolidateBtn.disabled = true;
    Upload.updateState(null);
    Search.setScopeContext(null);
    FileList.renderEmpty();
    Detail.renderDashboard();
});

document.getElementById('tree-header-label').addEventListener('click', () => {
    Search.close();
    Tree.collapseAll();
    if (Tree.onDeselect) Tree.onDeselect();
});

FileList.init(async (file) => {
    selectedFile = file;
    const result = await Detail.renderFile(file);
    selectedFileDups = Detail.getFileDups();
    consolidateBtn.disabled = selectedFileDups.length === 0;
    if (file.type === 'folder') {
        wireMergeBtn(file);
        wireNewFolderBtn(file);
        wireDownloadZipBtn(file);
        wireMoveFolder(file);
        wireDeleteFolderBtn(file);
    } else {
        wireDeleteFileBtn();
        wireRenameFileBtn();
        wireMoveFileBtn();
        wireIgnoreFileBtn();
    }
    if (result) {
        updateLocationOnline(result.locationId, result.locationOnline);
        if (result.folderId) {
            const node = await Tree.revealNode(result.folderId);
            if (node) {
                selectedNode = node;
                scanBtn.disabled = false;
                Upload.updateState(node);
            }
        }
    }
}, async (folder) => {
    selectedFile = null;
    selectedFileDups = [];
    consolidateBtn.disabled = true;
    const node = await Tree.revealNode(folder.id);
    if (node) {
        selectedNode = node;
        scanBtn.disabled = false;
        Upload.updateState(node);
    }
    const [, result] = await Promise.all([
        FileList.showFolder(folder.id),
        Detail.renderFolder(folder),
    ]);
    wireNewFolderBtn();
    wireDownloadZipBtn();
    wireMergeBtn();
    wireMoveFolder();
    wireDeleteFolderBtn();
    if (result) updateLocationOnline(result.locationId, result.locationOnline);
    wireSlideshowBtn();
}, async () => {
    selectedFile = null;
    selectedFileDups = [];
    consolidateBtn.disabled = true;
    if (selectedNode) {
        if (selectedNode.type === 'location') {
            const result = await Detail.renderLocation(selectedNode);
            wireDeleteLocationBtn();
            wireRenameLocationBtn();
            wireNewFolderBtn();
            wireDownloadZipBtn();
            wireMergeBtn();
            wireTreemapBtn();
            if (result && result.online !== undefined && result.online !== selectedNode.online) {
                selectedNode.online = result.online;
                Tree.render();
            }
        } else {
            const result = await Detail.renderFolder(selectedNode);
            wireNewFolderBtn();
            wireDownloadZipBtn();
            wireMergeBtn();
            wireMoveFolder();
            wireDeleteFolderBtn();
            if (result) updateLocationOnline(result.locationId, result.locationOnline);
        }
    } else {
        await Detail.renderDashboard();
    }
}, async (items) => {
    selectedFile = null;
    selectedFileDups = [];
    consolidateBtn.disabled = true;
    await Detail.renderMultiSelect(items);
    wireBatchActions(items);
});

FileList.onBreadcrumbNav = (nodeId) => Tree.navigateTo(nodeId);

Detail.init({
    async onNavigateToFolder(nodeId, fileId) {
        if (fileId) FileList.pendingFocusFile = fileId;
        await Tree.navigateTo(nodeId);
    },
    onShowDuplicates(hashStrong) {
        FileList.showDuplicateGroup(hashStrong);
    },
    async onNavigateToFile(fileId) {
        Search.close();
        const res = await API.get(`/api/files/${fileId}`);
        if (!res.ok) return;
        const detail = res.data;
        const folderId = detail.folderId || detail.locationId;
        if (folderId) {
            const node = await Tree.revealNode(folderId);
            if (node) {
                selectedNode = node;
                scanBtn.disabled = false;
                Upload.updateState(node);
            }
        }
        const fileItem = {
            id: detail.id,
            name: detail.name,
            typeHigh: detail.typeHigh,
            typeLow: detail.typeLow,
            size: detail.size,
            date: detail.date,
            dups: (detail.duplicates || []).length,
            hashStrong: detail.hashStrong,
        };
        FileList.showSingleFile(fileItem);
        selectedFile = fileItem;
        await Detail.renderFile(fileItem);
        selectedFileDups = Detail.getFileDups();
        consolidateBtn.disabled = selectedFileDups.length === 0;
        wireDeleteFileBtn();
        wireRenameFileBtn();
        wireMoveFileBtn();
        wireIgnoreFileBtn();
        if (detail.locationId) updateLocationOnline(detail.locationId, detail.locationOnline);
    },
});

AddLocationModal.init(async ({ name, path }) => {
    const res = await API.post('/api/locations', { name, path });
    if (!res.ok) {
        return { error: res.error || 'Failed to add location.' };
    }
    ActivityLog.add(`Location added: <b>${name}</b>`);
    Toast.success(`Location added: ${name}`);
    await Tree.reload();
    API.post('/api/scan', { location_id: res.data.id });
    return { ok: true };
});

Search.init({
    async onSearch(values) {
        const params = new URLSearchParams();
        if (values.mode === 'advanced') {
            params.set('mode', 'advanced');
            values.conditions.forEach((c, i) => {
                params.set(`c${i}_field`, c.field);
                params.set(`c${i}_op`, c.op);
                switch (c.field) {
                    case 'size':
                        if (c.min) params.set(`c${i}_min`, c.min);
                        if (c.max) params.set(`c${i}_max`, c.max);
                        break;
                    case 'date':
                        if (c.from) params.set(`c${i}_from`, c.from);
                        if (c.to) params.set(`c${i}_to`, c.to);
                        break;
                    default:
                        if (c.value) params.set(`c${i}_value`, c.value);
                        if (c.match) params.set(`c${i}_match`, c.match);
                        break;
                }
            });
            if (!values.files) params.set('files', 'false');
            if (values.folders) params.set('folders', 'true');
        } else {
            if (values.name) params.set('name', values.name);
            if (values.name && values.nameMatch && values.nameMatch !== 'anywhere') params.set('nameMatch', values.nameMatch);
            if (values.type) params.set('type', values.type);
            if (values.description) params.set('description', values.description);
            if (values.tags) params.set('tags', values.tags);
            if (values.sizeMin) params.set('sizeMin', values.sizeMin);
            if (values.sizeMax) params.set('sizeMax', values.sizeMax);
            if (values.minDups) params.set('minDups', values.minDups);
            if (values.dateFrom) params.set('dateFrom', values.dateFrom);
            if (values.dateTo) params.set('dateTo', values.dateTo);
            if (!values.files) params.set('files', 'false');
            if (values.folders) params.set('folders', 'true');
            if (values.dupes) params.set('dupes', '1');
        }
        if (values.scopeType && values.scopeId) {
            params.set('scopeType', values.scopeType);
            params.set('scopeId', values.scopeId);
        }
        params.set('page', '0');
        FileList.showLoading();
        const res = await API.get(`/api/search?${params.toString()}`);
        if (res.ok) {
            // Pass search params so FileList can re-fetch for paging/sorting
            const searchParams = {};
            for (const [k, v] of params.entries()) {
                if (k !== 'page' && k !== 'sort' && k !== 'sortDir') searchParams[k] = v;
            }
            FileList.showSearchResults(res.data, searchParams);
            Detail.renderSearchResults(res.data, searchParams);
        } else {
            FileList.renderEmpty();
            Toast.error(res.error || 'Search failed.');
        }
    },
    onClear() {
        if (selectedNode) {
            FileList.showFolder(selectedNode.id);
        } else {
            FileList.renderEmpty();
        }
    },
});

// WebSocket event handlers
WS.on('__open', () => {
    StatusBar.renderConnection(true);
    ActivityLog.add('WebSocket connected');
    // Sync scan queue state (badges, status bar) on connect/reconnect
    API.get('/api/scan/queue').then(res => {
        if (res.ok) syncQueuedLocations(res.data);
    });
});

WS.on('__close', () => {
    StatusBar.renderConnection(false);
    ActivityLog.add('WebSocket disconnected');
});

WS.on('scan_started', (msg) => {
    StatusBar.renderActivity('scanning', `${msg.location} — starting...`, msg.locationId);
    ActivityLog.add(`Scan started: <b>${msg.location}</b>`);
    updateLocationOnline(msg.locationId, true);
    Tree.setScanningLocation(msg.locationId);
});

WS.on('scan_progress', (msg) => {
    const skippedSuffix = msg.filesSkipped ? `, ${msg.filesSkipped.toLocaleString()} skipped` : '';
    const matchesSuffix = msg.potentialMatches ? `, ${msg.potentialMatches.toLocaleString()} matches` : '';
    StatusBar.renderActivity('scanning', `${msg.location} — ${msg.filesHashed.toLocaleString()} hashed${skippedSuffix}${matchesSuffix}`, msg.locationId);
    StatusBar.updateStatsFromProgress(msg);
    const skippedLog = msg.filesSkipped ? `, ${msg.filesSkipped.toLocaleString()} skipped` : '';
    const matchesPart = msg.potentialMatches ? `, ${msg.potentialMatches.toLocaleString()} matches` : '';
    ActivityLog.add(`${msg.location} — ${msg.filesFound.toLocaleString()} found, ${msg.filesHashed.toLocaleString()} hashed${skippedLog}${matchesPart}`);
    updateLocationOnline(msg.locationId, true);
    Tree.setScanningLocation(msg.locationId);
});

WS.on('scan_completed', async (msg) => {
    const queuePending = StatusBar.getQueue().length > 0;
    if (queuePending) {
        StatusBar.renderActivity('scanning', 'Starting next scan...', null);
    } else {
        StatusBar.renderActivity('idle');
    }
    const skippedPart = msg.filesSkipped ? `, ${msg.filesSkipped.toLocaleString()} skipped` : '';
    const stalePart = msg.staleFiles ? `, ${msg.staleFiles.toLocaleString()} stale` : '';
    ActivityLog.add(`Scan completed: <b>${msg.location}</b> — ${msg.filesHashed.toLocaleString()} hashed${skippedPart}, ${msg.duplicatesFound.toLocaleString()} duplicates${stalePart}`);
    Toast.success(`Scan completed: ${msg.location}`);
    Tree.clearScanningLocation(msg.locationId);
    await StatusBar.loadStats();
    await Tree.reload();
    if (selectedNode) {
        selectedNode = Tree._findNode(selectedNode.id);
        if (selectedFile) FileList.pendingFocusFile = selectedFile.id;
        if (selectedNode) await FileList.showFolder(selectedNode.id);
    }
    await refreshDetailPanel();
});

WS.on('scan_cancelled', async (msg) => {
    const queuePending = StatusBar.getQueue().length > 0;
    if (queuePending) {
        StatusBar.renderActivity('scanning', 'Starting next scan...', null);
    } else {
        StatusBar.renderActivity('idle');
    }
    const skippedPart = msg.filesSkipped ? `, ${msg.filesSkipped.toLocaleString()} skipped` : '';
    ActivityLog.add(`Scan cancelled: <b>${msg.location}</b> — ${msg.filesHashed.toLocaleString()} hashed${skippedPart} before cancel`);
    Toast.info(`Scan cancelled: ${msg.location}`);
    Tree.clearScanningLocation(msg.locationId);
    await StatusBar.loadStats();
    await Tree.reload();
    if (selectedNode) {
        selectedNode = Tree._findNode(selectedNode.id);
        if (selectedFile) FileList.pendingFocusFile = selectedFile.id;
        if (selectedNode) await FileList.showFolder(selectedNode.id);
    }
    await refreshDetailPanel();
});

WS.on('scan_interrupted', async (msg) => {
    ActivityLog.add(`Scan interrupted: <b>${msg.location}</b> — agent disconnected, will resume automatically`);
    Toast.info(`Scan interrupted: ${msg.location} — will resume when agent reconnects`);
    Tree.clearScanningLocation(msg.locationId);
});

WS.on('scan_error', (msg) => {
    const queuePending = StatusBar.getQueue().length > 0;
    if (queuePending) {
        StatusBar.renderActivity('scanning', 'Starting next scan...', null);
    } else {
        StatusBar.renderActivity('idle');
    }
    Tree.clearScanningLocation(msg.locationId);
    ActivityLog.add(`Scan error: <b>${msg.location}</b> — ${msg.error}`);
    Toast.error(`Scan error: ${msg.error}`);
});

function syncQueuedLocations(queue) {
    StatusBar.updateQueue(queue);
    // Rebuild both sets from queue state (single source of truth)
    Tree._queuedLocations.clear();
    Tree._scanningLocations.clear();
    if (queue) {
        if (queue.running_location_ids) {
            for (const locId of queue.running_location_ids) {
                Tree._scanningLocations.add('loc-' + locId);
            }
        } else if (queue.running_location_id) {
            Tree._scanningLocations.add('loc-' + queue.running_location_id);
        }
        if (queue.pending) {
            for (const entry of queue.pending) {
                Tree._queuedLocations.set('loc-' + entry.location_id, entry.queue_id);
            }
        }
    }
    Tree.render();
}

WS.on('scan_queued', (msg) => {
    syncQueuedLocations(msg.queue);
    ActivityLog.add(`Scan queued: <b>${msg.entry.name}</b>`);
});

WS.on('scan_dequeued', (msg) => {
    syncQueuedLocations(msg.queue);
    ActivityLog.add(`Scan dequeued: <b>${msg.entry.name}</b>`);
    Toast.info(`Scan dequeued: ${msg.entry.name}`);
});

WS.on('scan_queue_updated', (msg) => {
    syncQueuedLocations(msg.queue);
});

WS.on('scan_queue_skipped', (msg) => {
    syncQueuedLocations(msg.queue);
    ActivityLog.add(`Scan skipped (${msg.reason}): <b>${msg.entry.name}</b>`);
    Toast.info(`Scan skipped: ${msg.entry.name} — ${msg.reason}`);
});

WS.on('backfill_started', (msg) => {
    StatusBar.renderActivity('scanning', `${msg.location} — backfilling hashes (0/${msg.totalFiles.toLocaleString()})`);
    ActivityLog.add(`Hash backfill started: <b>${msg.location}</b> — ${msg.totalFiles.toLocaleString()} files`);
    Tree.setBackfillingLocation(msg.locationId);
});

WS.on('backfill_progress', (msg) => {
    StatusBar.renderActivity('scanning', `${msg.location} — backfilling hashes (${msg.filesHashed.toLocaleString()}/${msg.totalFiles.toLocaleString()})`);
    ActivityLog.add(`Hash backfill: <b>${msg.location}</b> — ${msg.filesHashed.toLocaleString()} / ${msg.totalFiles.toLocaleString()} files`);
    Tree.setBackfillingLocation(msg.locationId);
    FileList.refreshDupCounts();
    StatusBar.loadStats();
});

WS.on('backfill_completed', async (msg) => {
    Tree.clearBackfillingLocation(msg.locationId);
    StatusBar.renderActivity('idle');
    const parts = [];
    if (msg.agentFilesHashed) parts.push(`${msg.agentFilesHashed.toLocaleString()} agent files hashed`);
    if (msg.localFilesHashed) parts.push(`${msg.localFilesHashed.toLocaleString()} local files hashed`);
    if (msg.duplicatesFound) parts.push(`${msg.duplicatesFound.toLocaleString()} duplicates`);
    const summary = parts.length ? parts.join(', ') : 'no matches found';
    const label = msg.cancelled ? 'Hash backfill cancelled' : 'Hash backfill completed';
    ActivityLog.add(`${label}: <b>${msg.location}</b> — ${summary}`);
    if (msg.duplicatesFound && !msg.cancelled) {
        Toast.info(`Backfill found ${msg.duplicatesFound.toLocaleString()} duplicates in ${msg.location}`);
    }
    await StatusBar.loadStats();
    await Tree.reload();
    if (selectedNode) {
        selectedNode = Tree._findNode(selectedNode.id);
        if (selectedFile) FileList.pendingFocusFile = selectedFile.id;
        if (selectedNode) await FileList.showFolder(selectedNode.id);
    }
    await refreshDetailPanel();
});

WS.on('dup_backfill_started', msg => {
    const locs = msg.locations && msg.locations.length ? msg.locations.join(', ') : 'all locations';
    ActivityLog.add(`Fixing duplicate counts for <b>${locs}</b>...`);
});
WS.on('dup_backfill_progress', msg => {
    const pct = Math.round((msg.processed / msg.totalHashes) * 100);
    ActivityLog.add(`Fixing duplicate counts... ${pct}%`);
});
WS.on('dup_backfill_completed', msg => {
    if (msg.skipped) {
        ActivityLog.add('Duplicate counts up to date');
    } else {
        ActivityLog.add(`Duplicate counts fixed (${(msg.updated || 0).toLocaleString()} hashes)`);
    }
});
WS.on('size_recalc_completed', msg => {
    ActivityLog.add('Location sizes recalculated');
});
WS.on('dup_recalc_completed', msg => {
    const src = msg.source ? ` (${msg.source})` : '';
    ActivityLog.add(`Duplicate counts updated${src}`);
});

WS.on('dup_exclude_started', msg => {
    const verb = msg.direction === 'exclude' ? 'Excluding' : 'Including';
    const prep = msg.direction === 'exclude' ? 'from' : 'in';
    ActivityLog.add(`${verb} folder ${prep} duplicates: <b>${msg.folder}</b>...`);
});
WS.on('dup_exclude_progress', msg => {
    ActivityLog.add(`Updating duplicate exclusion... ${msg.pct}%`);
});
WS.on('dup_exclude_completed', async msg => {
    const verb = msg.direction === 'exclude' ? 'excluded' : 'included';
    ActivityLog.add(`Duplicate exclusion updated: <b>${msg.folder}</b> ${verb} — ${(msg.fileCount || 0).toLocaleString()} files, ${(msg.hashCount || 0).toLocaleString()} hashes recalculated`);
    if (selectedNode) await FileList.showFolder(selectedNode.id);
    await refreshDetailPanel();
    await StatusBar.loadStats();
});

WS.on('location_changed', async (msg) => {
    ActivityLog.add(`Location ${msg.action}: <b>${msg.location?.label || msg.locationId || ''}</b>`);
    await Tree.reload();
    if (selectedNode) selectedNode = Tree._findNode(selectedNode.id);
    await StatusBar.loadStats();
    await refreshDetailPanel();
});

WS.on('stats_updated', async () => {
    await StatusBar.loadStats();
    await refreshDetailPanel();
});

WS.on('file_deleted', async (msg) => {
    if (selectedFile && selectedFile.id === msg.fileId) {
        selectedFile = null;
        selectedFileDups = [];
        consolidateBtn.disabled = true;
    }
    await StatusBar.loadStats();
    if (selectedNode) await FileList.showFolder(selectedNode.id);
    await refreshDetailPanel();
});

WS.on('folder_created', async (msg) => {
    await Tree.reload();
    if (selectedNode) {
        selectedNode = Tree._findNode(selectedNode.id);
        if (selectedFile) FileList.pendingFocusFile = selectedFile.id;
        if (selectedNode) await FileList.showFolder(selectedNode.id);
    }
    await StatusBar.loadStats();
    await refreshDetailPanel();
});

WS.on('folder_moved', async (msg) => {
    await Tree.reload();
    if (selectedNode) {
        selectedNode = Tree._findNode(selectedNode.id);
        if (selectedFile) FileList.pendingFocusFile = selectedFile.id;
        if (selectedNode) await FileList.showFolder(selectedNode.id);
    }
    await StatusBar.loadStats();
    await refreshDetailPanel();
});

WS.on('file_moved', async (msg) => {
    if (selectedFile && selectedFile.id === msg.fileId) {
        selectedFile = null;
        selectedFileDups = [];
        consolidateBtn.disabled = true;
    }
    await Tree.reload();
    if (selectedNode) {
        selectedNode = Tree._findNode(selectedNode.id);
        if (selectedFile) FileList.pendingFocusFile = selectedFile.id;
        if (selectedNode) await FileList.showFolder(selectedNode.id);
    }
    await StatusBar.loadStats();
    await refreshDetailPanel();
});

WS.on('batch_deleted', async (msg) => {
    await Tree.reload();
    if (selectedNode) {
        selectedNode = Tree._findNode(selectedNode.id);
        if (selectedFile) FileList.pendingFocusFile = selectedFile.id;
        if (selectedNode) await FileList.showFolder(selectedNode.id);
    }
    await StatusBar.loadStats();
    await refreshDetailPanel();
});

WS.on('batch_moved', async (msg) => {
    await Tree.reload();
    if (selectedNode) {
        selectedNode = Tree._findNode(selectedNode.id);
        if (selectedFile) FileList.pendingFocusFile = selectedFile.id;
        if (selectedNode) await FileList.showFolder(selectedNode.id);
    }
    await StatusBar.loadStats();
    await refreshDetailPanel();
});

WS.on('folder_deleted', async (msg) => {
    await Tree.reload();
    if (selectedNode) {
        selectedNode = Tree._findNode(selectedNode.id);
        if (selectedFile) FileList.pendingFocusFile = selectedFile.id;
        if (selectedNode) await FileList.showFolder(selectedNode.id);
    }
    await StatusBar.loadStats();
    await refreshDetailPanel();
});

WS.on('consolidate_started', (msg) => {
    StatusBar.renderActivity('consolidating', msg.filename);
    ActivityLog.add(`Consolidation started: <b>${msg.filename}</b>`);
});

WS.on('consolidate_completed', async (msg) => {
    StatusBar.renderActivity('idle');
    ActivityLog.add(`Consolidation completed: <b>${msg.filename}</b> — ${msg.stubsWritten} stubs written, ${msg.stubsQueued} queued`);
    Toast.success(`Consolidation completed: ${msg.filename}`);
    // Clear selected file — it may have been deleted or replaced
    selectedFile = null;
    selectedFileDups = [];
    consolidateBtn.disabled = true;
    await StatusBar.loadStats();
    await Tree.reload();
    // Reload file list for current folder
    if (selectedNode) {
        selectedNode = Tree._findNode(selectedNode.id);
        if (selectedNode) await FileList.showFolder(selectedNode.id);
    }
    await refreshDetailPanel();
});

WS.on('consolidate_error', (msg) => {
    StatusBar.renderActivity('idle');
    ActivityLog.add(`Consolidation error: <b>${msg.filename}</b> — ${msg.error}`);
    Toast.error(`Consolidation error: ${msg.error}`);
});

WS.on('consolidate_queue_drained', (msg) => {
    ActivityLog.add(`Queue drained: ${msg.jobsCompleted} pending consolidation jobs completed`);
    Toast.info(`${msg.jobsCompleted} deferred consolidation job${msg.jobsCompleted === 1 ? '' : 's'} completed`);
});

WS.on('batch_consolidate_completed', (msg) => {
    ActivityLog.add(`Batch consolidation completed: ${msg.completed} of ${msg.total} files`);
    Toast.success(`Batch consolidation completed: ${msg.completed} of ${msg.total} files`);
});

WS.on('upload_transfer', (msg) => {
    const filePart = `${msg.current}/${msg.total} ${msg.filename}`;
    const sizePart = msg.totalMB > 1 ? ` — ${msg.sentMB}/${msg.totalMB} MB (${msg.pct}%)` : '';
    StatusBar.renderActivity('uploading', `${msg.location} — sending to agent ${filePart}${sizePart}`);
});

WS.on('upload_started', (msg) => {
    StatusBar.renderActivity('uploading', `${msg.location} — ${msg.fileCount} file(s)...`);
    ActivityLog.add(`Upload started: <b>${msg.location}</b> — ${msg.fileCount} file(s)`);
});

WS.on('upload_progress', (msg) => {
    StatusBar.renderActivity('uploading', `${msg.location} — ${msg.processed}/${msg.total} (${msg.currentFile})`);
});

WS.on('upload_duplicate', (msg) => {
    Toast.info(`Duplicate: ${msg.filename} — already in ${msg.existingLocation}`);
    ActivityLog.add(`Upload duplicate: <b>${msg.filename}</b> — exists in ${msg.existingLocation}`);
});

WS.on('upload_file_error', (msg) => {
    Toast.error(`Upload error: ${msg.filename} — ${msg.error}`);
    ActivityLog.add(`Upload error: <b>${msg.filename}</b> — ${msg.error}`);
});

WS.on('upload_completed', async (msg) => {
    StatusBar.renderActivity('idle');
    ActivityLog.add(`Upload completed: <b>${msg.location}</b> — ${msg.cataloged} cataloged, ${msg.duplicates} duplicates`);
    Toast.success(`Upload completed: ${msg.cataloged} cataloged, ${msg.duplicates} duplicates`);
    await StatusBar.loadStats();
    await Tree.reload();
    if (selectedNode) {
        selectedNode = Tree._findNode(selectedNode.id);
        if (selectedFile) FileList.pendingFocusFile = selectedFile.id;
        if (selectedNode) await FileList.showFolder(selectedNode.id);
    }
    await refreshDetailPanel();
});

WS.on('merge_started', (msg) => {
    StatusBar.renderActivity('merging', `${msg.source} → ${msg.destination} — starting...`);
    ActivityLog.add(`Merge started: <b>${msg.source}</b> → <b>${msg.destination}</b>`);
});

WS.on('merge_progress', (msg) => {
    StatusBar.renderActivity('merging', `${msg.source} → ${msg.destination} — ${msg.processed}/${msg.total} files (${msg.copied} copied, ${msg.stubbed} stubbed)`);
    ActivityLog.add(`Merging: ${msg.source} → ${msg.destination} — ${msg.processed}/${msg.total} files`);
});

WS.on('merge_completed', async (msg) => {
    StatusBar.renderActivity('idle');
    ActivityLog.add(`Merge completed: <b>${msg.source}</b> → <b>${msg.destination}</b> — ${msg.filesCopied} copied, ${msg.filesStubbed} stubbed, ${msg.filesSkipped} skipped`);
    Toast.success(`Merge completed: ${msg.filesCopied} copied, ${msg.filesStubbed} stubbed`);
    await StatusBar.loadStats();
    await Tree.reload();
    if (selectedNode) {
        selectedNode = Tree._findNode(selectedNode.id);
        if (selectedFile) FileList.pendingFocusFile = selectedFile.id;
        if (selectedNode) await FileList.showFolder(selectedNode.id);
    }
    await refreshDetailPanel();
});

WS.on('merge_cancelled', async (msg) => {
    StatusBar.renderActivity('idle');
    ActivityLog.add(`Merge cancelled: <b>${msg.source}</b> → <b>${msg.destination}</b> — ${msg.copied} copied, ${msg.stubbed} stubbed before cancel`);
    Toast.info(`Merge cancelled: ${msg.source} → ${msg.destination}`);
    await StatusBar.loadStats();
    await Tree.reload();
    if (selectedNode) {
        selectedNode = Tree._findNode(selectedNode.id);
        if (selectedFile) FileList.pendingFocusFile = selectedFile.id;
        if (selectedNode) await FileList.showFolder(selectedNode.id);
    }
    await refreshDetailPanel();
});

WS.on('merge_error', (msg) => {
    StatusBar.renderActivity('idle');
    ActivityLog.add(`Merge error: <b>${msg.source}</b> → <b>${msg.destination}</b> — ${msg.error}`);
    Toast.error(`Merge error: ${msg.error}`);
});

WS.on('settings_changed', async (msg) => {
    const serverName = msg.settings?.serverName || '';
    if (serverName) {
        document.title = `File Hunter \u2014 ${serverName}`;
        document.getElementById('app-title').textContent = `File Hunter \u2014 ${serverName}`;
    } else {
        document.title = 'File Hunter';
        document.getElementById('app-title').textContent = 'File Hunter';
    }
    // Reload tree and file list when showHiddenFiles changes
    await Tree.reload();
    const selectedNode = Tree.data?.find(n => n.id === Tree.selected)
        || Tree.data?.flatMap(n => n.children || []).find(n => n.id === Tree.selected);
    if (selectedNode) {
        await FileList.showFolder(selectedNode.id);
    }
});

WS.on('agent_status', async (msg) => {
    const label = msg.agentId ? `Agent #${msg.agentId}` : 'Agent';
    ActivityLog.add(`${label} ${msg.status}`);
    await Tree.reload();
    await StatusBar.loadStats();
});

WS.connect();

// Load pro extension if active
API.get('/api/pro/status').then(res => {
    if (res.ok && res.data.active) {
        import('/pro/js/pro.js').then(mod => {
            if (mod.default?.init) mod.default.init();
        });
    }
});

// ── Panel resize handles ──
function initPanelResize(handleId, leftPanel, rightPanel, storageKey) {
    const handle = document.getElementById(handleId);
    if (!handle) return;

    // Restore saved width
    const saved = localStorage.getItem(storageKey);
    if (saved) leftPanel.style.width = saved + 'px';

    let startX, startWidth;

    handle.addEventListener('mousedown', (e) => {
        e.preventDefault();
        startX = e.clientX;
        startWidth = leftPanel.getBoundingClientRect().width;
        handle.classList.add('dragging');
        document.body.style.cursor = 'col-resize';
        document.body.style.userSelect = 'none';

        const onMove = (e) => {
            const delta = e.clientX - startX;
            const newWidth = Math.max(
                parseInt(getComputedStyle(leftPanel).minWidth) || 120,
                startWidth + delta
            );
            leftPanel.style.width = newWidth + 'px';
        };

        const onUp = () => {
            handle.classList.remove('dragging');
            document.body.style.cursor = '';
            document.body.style.userSelect = '';
            localStorage.setItem(storageKey, leftPanel.getBoundingClientRect().width);
            document.removeEventListener('mousemove', onMove);
            document.removeEventListener('mouseup', onUp);
        };

        document.addEventListener('mousemove', onMove);
        document.addEventListener('mouseup', onUp);
    });
}

const treePanel = document.getElementById('tree-panel');
const detailPanel = document.getElementById('detail-panel');
initPanelResize('resize-left', treePanel, null, 'fh-tree-width');

// Right handle resizes detail panel — drag right = smaller, drag left = larger
const rightHandle = document.getElementById('resize-right');
if (rightHandle) {
    const savedDetail = localStorage.getItem('fh-detail-width');
    if (savedDetail) detailPanel.style.width = savedDetail + 'px';

    let startX, startWidth;
    rightHandle.addEventListener('mousedown', (e) => {
        e.preventDefault();
        startX = e.clientX;
        startWidth = detailPanel.getBoundingClientRect().width;
        rightHandle.classList.add('dragging');
        document.body.style.cursor = 'col-resize';
        document.body.style.userSelect = 'none';

        const onMove = (e) => {
            const delta = startX - e.clientX;
            const newWidth = Math.max(
                parseInt(getComputedStyle(detailPanel).minWidth) || 180,
                startWidth + delta
            );
            detailPanel.style.width = newWidth + 'px';
        };

        const onUp = () => {
            rightHandle.classList.remove('dragging');
            document.body.style.cursor = '';
            document.body.style.userSelect = '';
            localStorage.setItem('fh-detail-width', detailPanel.getBoundingClientRect().width);
            document.removeEventListener('mousemove', onMove);
            document.removeEventListener('mouseup', onUp);
        };

        document.addEventListener('mousemove', onMove);
        document.addEventListener('mouseup', onUp);
    });
}

} // end startApp

// ── Auth gate ──
Login.init((user) => startApp(user));

(async () => {
    const res = await API.get('/api/auth/status');
    if (!res.ok) return;

    if (res.data.needsSetup) {
        Login.showSetup();
        return;
    }

    // Check existing token
    const token = localStorage.getItem('fh-token');
    if (!token) {
        // Fetch server name for login screen (settings is a protected endpoint,
        // so use the status endpoint's serverName if available, or show default)
        Login.showLogin(res.data.serverName || '');
        return;
    }

    // Validate token
    const meRes = await API.get('/api/auth/me');
    if (meRes.ok) {
        startApp(meRes.data);
    } else {
        localStorage.removeItem('fh-token');
        Login.showLogin(res.data.serverName || '');
    }
})();
