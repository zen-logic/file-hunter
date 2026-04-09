import API from './api.js';
import icons from './icons.js';
import Tree from './components/tree.js';
import FileList from './components/filelist.js';
import Detail from './components/detail.js';
import StatusBar from './components/statusbar.js';
import Search from './components/search.js';
import AddLocationModal from './components/addlocation.js';
import ConfirmModal from './components/confirm.js';
import Consolidate from './components/consolidate.js';
import Merge from './components/merge.js';
import DeleteLocationModal from './components/deletelocation.js';
import DeleteFileModal from './components/deletefile.js';
import RenameLocationModal from './components/renamelocation.js';
import NewFolderModal from './components/newfolder.js';
import RenameFileModal from './components/renamefile.js';
import RenameFolderModal from './components/renamefolder.js';
import MoveFileModal from './components/movefile.js';
import IgnoreFileModal from './components/ignorefile.js';
import Treemap from './components/treemap.js';
import Login from './components/login.js';
import Settings from './components/settings.js';
import About from './components/about.js';
import Activity from './components/activity.js';
import ActivityLog from './components/activitylog.js';
import Toast from './components/toast.js';
import Upload from './components/upload.js';
import SlideshowTriage from './components/slideshow-triage.js';
import Triage from './components/triage.js';
import ImportCatalog from './components/importcatalog.js';
import RepairCatalog from './components/repaircatalog.js';
import ScanConfirm from './components/scanconfirm.js';
import Keyboard from './keyboard.js';
import WS from './ws.js';

let currentUser = null;
let selectedNode = null;
let selectedFile = null;
let selectedFileDups = [];
const scanBtn = document.getElementById('btn-scan');
const consolidateBtn = document.getElementById('btn-consolidate');
const uploadBtn = document.getElementById('btn-upload');

async function reloadTreeAndFileList(focusFileId) {
    // Only numeric IDs are focusable files — ignore folder IDs (fld-xxx)
    if (focusFileId && String(focusFileId).startsWith('fld-')) focusFileId = null;
    const folderId = selectedNode ? selectedNode.id : null;
    await Tree.reload();
    if (folderId) {
        selectedNode = Tree._findNode(folderId);
        // Don't blow away search results or in-flight select-all
        if (!FileList._searchMode) {
            if (focusFileId) FileList.pendingFocusFile = focusFileId;
            if (folderId === FileList.currentFolder) {
                await FileList.refreshFolder();
            } else {
                await FileList.showFolder(folderId);
            }
        }
    }
}

async function refreshDetailPanel() {
    // Don't overwrite search results detail panel with folder/location view
    if (FileList._searchMode && !selectedFile) return;
    if (selectedFile) {
        const result = await Detail.renderFile(selectedFile);
        selectedFileDups = Detail.getFileDups();
        consolidateBtn.disabled = selectedFileDups.length === 0;
        if (selectedFile.type === 'folder') {
            wireMergeBtn(selectedFile);
            wireNewFolderBtn(selectedFile);
            wireDownloadZipBtn(selectedFile);
            wireRenameFolderBtn(selectedFile);
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
            wireFavouriteBtn();
            if (result && result.online !== undefined && result.online !== selectedNode.online) {
                Tree.updateOnlineStatus([selectedNode.id], result.online);
            }
        } else {
            const result = await Detail.renderFolder(selectedNode);
            wireNewFolderBtn();
            wireDownloadZipBtn();
            wireMergeBtn();
            wireRenameFolderBtn();
            wireMoveFolder();
            wireDeleteFolderBtn();
            wireFavouriteBtn();
            if (result) updateLocationOnline(result.locationId, result.locationOnline);
        }
    } else {
        await Detail.renderDashboard();
    }
}

function updateLocationOnline(locationId, online) {
    if (!locationId || online === undefined) return;
    const locId = String(locationId).startsWith('loc-') ? locationId : 'loc-' + locationId;
    Tree.updateOnlineStatus([locId], online);
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
            btn.disabled = true;
            btn.textContent = 'Building ZIP\u2026';
            const resp = await API.post(url);
            if (resp.ok) {
                _pendingZipBtn = btn;
                _pendingZipOrigText = 'Download ZIP';
                const zipLabel = target.label || 'download';
                Activity.started('zip-' + resp.data.jobId, {
                    label: `Building ZIP: ${zipLabel}`,
                    detail: `0/${resp.data.total} files`,
                    log: `ZIP build started: <b>${zipLabel}</b>`,
                });
            } else {
                btn.disabled = false;
                btn.textContent = 'Download ZIP';
                Toast.error(resp.data?.detail || 'Download failed.');
            }
        });
    }
}

let _pendingZipBtn = null;
let _pendingZipOrigText = 'Download ZIP';

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

function wireRenameFolderBtn(node) {
    const btn = document.getElementById('detail-rename-folder');
    const target = node || selectedNode;
    if (btn && target) {
        btn.addEventListener('click', () => RenameFolderModal.open(target));
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

function wireFavouriteBtn() {
    const btn = document.getElementById('detail-favourite');
    if (btn && selectedNode) {
        btn.addEventListener('click', async () => {
            const res = await API.post('/api/favourite/toggle', { id: selectedNode.id });
            if (res.ok) {
                const icon = btn.querySelector('.fav-icon');
                if (icon) icon.innerHTML = res.data.favourite ? icons.heart : icons.heartOutline;
                btn.classList.toggle('btn-active', res.data.favourite);
                Tree.setFavourite(selectedNode.id, res.data.favourite);
                FileList.setFolderFavourite(selectedNode.id, res.data.favourite);
            }
        });
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
            downloadBtn.textContent = 'Building ZIP\u2026';
            const resp = await API.post('/api/batch/download', { file_ids: fileIds, folder_ids: folderIds });
            if (resp.ok) {
                _pendingZipBtn = downloadBtn;
                _pendingZipOrigText = 'Download ZIP';
                Toast.info('Building ZIP...');
            } else {
                downloadBtn.disabled = false;
                downloadBtn.textContent = 'Download ZIP';
                Toast.error(resp.data?.detail || 'Download failed.');
            }
        });
    }

    // Batch rehash
    const rehashBtn = document.getElementById('batch-rehash-btn');
    if (rehashBtn && fileIds.length > 0) {
        rehashBtn.addEventListener('click', async () => {
            rehashBtn.disabled = true;
            rehashBtn.textContent = 'Hashing\u2026';
            const res = await API.post('/api/files/rehash', { fileIds });
            if (res.ok) {
                Toast.success(`Re-hashing ${fileIds.length} file(s) in background`);
            } else {
                Toast.error(res.error || 'Re-hash failed');
            }
            rehashBtn.disabled = false;
            rehashBtn.textContent = 'Re-hash';
        });
    }

    // Batch tag
    const tagAddBtn = document.getElementById('batch-tag-add');
    const tagInput = document.getElementById('batch-tag-input');
    if (tagAddBtn && tagInput) {
        const addTag = async () => {
            const tags = tagInput.value.split(',').map(t => t.trim()).filter(Boolean);
            if (tags.length === 0 || fileIds.length === 0) return;
            const n = fileIds.length;
            const label = tags.length === 1 ? `"${tags[0]}"` : `${tags.length} tags`;
            API.post('/api/batch/tag', {
                file_ids: fileIds,
                add_tags: tags,
                remove_tags: [],
            });
            Toast.info(`Writing ${label} to ${n} file${n !== 1 ? 's' : ''}...`);
            tagInput.value = '';
        };
        tagAddBtn.addEventListener('click', addTag);
        tagInput.addEventListener('keydown', (e) => {
            if (e.key === 'Enter') addTag();
        });
    }
}

function startApp(user) {
    currentUser = user;
    Detail._locationActivityFn = (nodeId) => {
        if (Tree._scanningLocations.has(nodeId)) return 'Scanning';
        if (Tree._queuedLocations.has(nodeId)) return 'Queued';
        if (Tree._backfillingLocations.has(nodeId)) return 'Backfilling';
        return null;
    };
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

ScanConfirm.init(async (loc, folder, scanType) => {
    const payload = { location_id: loc.id };
    if (folder) payload.folder_id = folder.id;
    const endpoint = scanType === 'quick' ? '/api/scan/quick' : '/api/scan';
    const res = await API.post(endpoint, payload);
    if (!res.ok) Toast.error(res.error || 'Failed to start scan.');
});

scanBtn.addEventListener('click', async () => {
    if (!selectedNode) return;
    const loc = Tree.getLocation(selectedNode.id);
    if (!loc) return;
    const isFolder = String(selectedNode.id).startsWith('fld-');
    const folderNode = isFolder ? selectedNode : null;

    const res = await API.get(`/api/scan/capabilities?location_id=${loc.id}`);
    const hasQuickScan = res.ok && res.data.quick_scan;
    ScanConfirm.open(loc, folderNode, hasQuickScan);
});

Consolidate.init(async (params) => {
    if (params.batch) {
        const payload = { file_ids: params.file_ids, mode: params.mode, consolidateMode: params.consolidateMode };
        if (params.destination_folder_id) payload.destination_folder_id = params.destination_folder_id;
        if (params.filename_match_only) payload.filename_match_only = true;
        if (params.stub_file_ids) payload.stub_file_ids = params.stub_file_ids;
        await API.post('/api/batch/consolidate', payload);
    } else {
        const payload = { file_id: params.file_id, mode: params.mode, consolidateMode: params.consolidateMode };
        if (params.destination_folder_id) payload.destination_folder_id = params.destination_folder_id;
        if (params.filename_match_only) payload.filename_match_only = true;
        if (params.stub_file_ids) payload.stub_file_ids = params.stub_file_ids;
        await API.post('/api/consolidate', payload);
    }
});

consolidateBtn.addEventListener('click', async () => {
    const selection = FileList.getSelection();
    if (selection.length > 1) {
        const files = selection.filter(i => i.type !== 'folder');
        if (files.length > 0) {
            await Consolidate.open({ files });
        }
    } else if (selectedFile) {
        await Consolidate.open({ file: selectedFile });
    }
});

SlideshowTriage.init();
SlideshowTriage._consolidateOpen = (files, onDone) => {
    Consolidate.open({ files, onDone });
};
Detail.slideshowTriage = SlideshowTriage;
Triage.init(
    (del, con, tag, mov) => SlideshowTriage.show(del, con, tag, mov),
    () => FileList.render(),
);
ImportCatalog.init();
RepairCatalog.init();
Merge.init(async ({ source_id, destination_id, mode }) => {
    await API.post('/api/merge', { source_id, destination_id, mode });
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
            ActivityLog.add(`Batch delete started: <b>${res.data.total} items</b>`);
            Activity.progress('batch-delete', { label: 'Deleting', detail: `0/${res.data.total}` });
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
        wireFavouriteBtn();
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

RenameFolderModal.init(async (folder, newName) => {
    const folderId = String(folder.id).replace('fld-', '');
    const res = await API.post(`/api/folders/${folderId}/move`, { name: newName });
    if (!res.ok) {
        return { error: res.error || 'Rename failed.' };
    }
    ActivityLog.add(`Folder renamed: <b>${folder.label || folder.name}</b> &rarr; <b>${newName}</b>`);
    Toast.success(`Folder renamed to ${newName}`);
    await reloadTreeAndFileList();
    await refreshDetailPanel();
    return { ok: true };
});

MoveFileModal.init(async (item, destinationFolderId, copy) => {
    const verb = copy ? 'copied' : 'moved';
    const Verb = copy ? 'Copied' : 'Moved';
    const noun = copy ? 'Copy' : 'Move';

    if (item.id === 'batch') {
        const res = await API.post('/api/batch/move', {
            file_ids: item.batchFileIds,
            folder_ids: item.batchFolderIds,
            destination_folder_id: destinationFolderId,
            copy: !!copy,
        });
        if (!res.ok) {
            return { error: res.error || `Batch ${noun.toLowerCase()} failed.` };
        }
        const d = res.data;
        ActivityLog.add(`Batch ${verb}: <b>${d.moved_files} files, ${d.moved_folders} folders</b>`);
        Toast.success(`${Verb} ${d.moved_files} files, ${d.moved_folders} folders`);
        selectedFile = null;
        selectedFileDups = [];
        consolidateBtn.disabled = true;
        const currentFolderId = selectedNode ? selectedNode.id : null;
        await Tree.reload();
        if (currentFolderId) {
            selectedNode = Tree._findNode(currentFolderId);
            await FileList.showFolder(currentFolderId);
        }
        await StatusBar.loadStats();
        await refreshDetailPanel();
        return { ok: true };
    }

    const itemId = String(item.id);
    if (itemId.startsWith('fld-')) {
        const numId = itemId.replace('fld-', '');
        const res = await API.post(`/api/folders/${numId}/move`, {
            destination_parent_id: destinationFolderId,
            copy: !!copy,
        });
        if (!res.ok) {
            return { error: res.error || `${noun} failed.` };
        }
        ActivityLog.add(`Folder ${verb}: <b>${item.name || item.label}</b>`);
        Toast.success(`Folder ${verb}: ${item.name || item.label}`);
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

    const res = await API.post(`/api/files/${item.id}/move`, {
        destination_folder_id: destinationFolderId,
        copy: !!copy,
    });
    if (!res.ok) {
        return { error: res.error || `${noun} failed.` };
    }
    ActivityLog.add(`File ${verb}: <b>${item.name}</b>`);
    Toast.success(`File ${verb}: ${item.name}`);
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
    const items = FileList.currentItems || [];
    const hasImages = items.some(f => (f.typeHigh || '').toLowerCase() === 'image');
    const hasVideo = items.some(f => (f.typeHigh || '').toLowerCase() === 'video');
    if (!hasImages && !hasVideo) return;
    const folderId = FileList.currentFolder;
    if (!folderId) return;
    let html = '';
    if (hasImages) html += `<button class="btn btn-sm" id="detail-slideshow">Slideshow</button>`;
    if (hasVideo) html += `<button class="btn btn-sm" id="detail-playlist">Playlist</button>`;
    slot.innerHTML = html;
    if (hasImages) {
        document.getElementById('detail-slideshow').addEventListener('click', async () => {
            const btn = document.getElementById('detail-slideshow');
            btn.disabled = true;
            btn.textContent = 'Loading\u2026';
            await Detail.startSlideshow({ type: 'folder', folderId, mode: 'slideshow' });
            if (Detail._slideshowTotal === 0) {
                btn.textContent = 'No images available';
                setTimeout(() => { btn.textContent = 'Slideshow'; btn.disabled = false; }, 2000);
            }
        });
    }
    if (hasVideo) {
        document.getElementById('detail-playlist').addEventListener('click', async () => {
            const btn = document.getElementById('detail-playlist');
            btn.disabled = true;
            btn.textContent = 'Loading\u2026';
            await Detail.startSlideshow({ type: 'folder', folderId, mode: 'playlist' });
            if (Detail._slideshowTotal === 0) {
                btn.textContent = 'No videos available';
                setTimeout(() => { btn.textContent = 'Playlist'; btn.disabled = false; }, 2000);
            }
        });
    }
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
        wireFavouriteBtn();
        if (result && result.online !== undefined && result.online !== node.online) {
            Tree.updateOnlineStatus([node.id], result.online);
        }
    } else {
        wireNewFolderBtn();
        wireDownloadZipBtn();
        wireMergeBtn();
        wireRenameFolderBtn();
        wireMoveFolder();
        wireDeleteFolderBtn();
        wireFavouriteBtn();
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
    FileList.renderFavourites();
    Detail.renderDashboard();
});

document.getElementById('tree-header-label').addEventListener('click', () => {
    Search.close();
    Tree.collapseAll();
    if (Tree.onDeselect) Tree.onDeselect();
});

FileList.init(async (file) => {
    // Favourite items are folder/location nodes — select in tree and show detail panel
    if (file.type === 'folder' && (String(file.id).startsWith('fld-') || String(file.id).startsWith('loc-'))) {
        selectedFile = null;
        selectedFileDups = [];
        consolidateBtn.disabled = true;
        const node = await Tree.revealNode(file.id);
        if (node) {
            selectedNode = node;
            scanBtn.disabled = false;
            Upload.updateState(node);
            await refreshDetailPanel();
        }
        return;
    }

    selectedFile = file;
    const result = await Detail.renderFile(file);
    selectedFileDups = Detail.getFileDups();
    consolidateBtn.disabled = selectedFileDups.length === 0;
    if (file.type === 'folder') {
        wireMergeBtn(file);
        wireNewFolderBtn(file);
        wireDownloadZipBtn(file);
        wireRenameFolderBtn(file);
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
    const isLocation = String(folder.id).startsWith('loc-');
    const node = await Tree.revealNode(folder.id);
    if (node) {
        selectedNode = node;
        scanBtn.disabled = false;
        Upload.updateState(node);
        Search.setScopeContext(node);
    }
    if (isLocation) {
        const [, result] = await Promise.all([
            FileList.showFolder(folder.id),
            Detail.renderLocation(node || folder),
        ]);
        wireDeleteLocationBtn();
        wireRenameLocationBtn();
        wireNewFolderBtn();
        wireDownloadZipBtn();
        wireMergeBtn();
        wireTreemapBtn();
        wireFavouriteBtn();
        if (result && result.online !== undefined && node && result.online !== node.online) {
            Tree.updateOnlineStatus([node.id], result.online);
        }
        wireSlideshowBtn();
    } else {
        const [, result] = await Promise.all([
            FileList.showFolder(folder.id),
            Detail.renderFolder(folder),
        ]);
        wireNewFolderBtn();
        wireDownloadZipBtn();
        wireMergeBtn();
        wireRenameFolderBtn();
        wireMoveFolder();
        wireDeleteFolderBtn();
        wireFavouriteBtn();
        if (result) updateLocationOnline(result.locationId, result.locationOnline);
        wireSlideshowBtn();
    }
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
            wireFavouriteBtn();
            if (result && result.online !== undefined && result.online !== selectedNode.online) {
                Tree.updateOnlineStatus([selectedNode.id], result.online);
            }
        } else {
            const result = await Detail.renderFolder(selectedNode);
            wireNewFolderBtn();
            wireDownloadZipBtn();
            wireMergeBtn();
            wireRenameFolderBtn();
            wireMoveFolder();
            wireDeleteFolderBtn();
            wireFavouriteBtn();
            if (result) updateLocationOnline(result.locationId, result.locationOnline);
        }
    } else {
        await Detail.renderDashboard();
    }
}, async (items) => {
    selectedFile = null;
    selectedFileDups = [];
    const hasFiles = items.some(i => i.type !== 'folder');
    consolidateBtn.disabled = !hasFiles;
    await Detail.renderMultiSelect(items);
    wireBatchActions(items);
});

FileList.onSelectingAll = () => {
    Detail.el.innerHTML = `
        <div class="detail-section">
            <div class="detail-filename">Selecting...</div>
        </div>`;
};

FileList.onBreadcrumbNav = (nodeId) => Tree.navigateTo(nodeId);

Detail.init({
    async onNavigateToFolder(nodeId, fileId) {
        if (fileId) FileList.pendingFocusFile = fileId;
        await Tree.navigateTo(nodeId);
    },
    onShowDuplicates(hash, fileId) {
        FileList.showDuplicateGroup(hash, fileId);
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
                    case 'duplicates':
                    case 'files':
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
            if (values.maxDups) params.set('maxDups', values.maxDups);
            if (values.minFiles) params.set('minFiles', values.minFiles);
            if (values.maxFiles) params.set('maxFiles', values.maxFiles);
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
        Detail.el.innerHTML = '';
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
            ConfirmModal.open({
                title: 'Search Error',
                message: res.error || 'Search failed.',
                confirmLabel: 'OK',
                alert: true,
            });
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

WS.on('activity', (msg) => {
    if (msg.message) ActivityLog.add(msg.message);
});

WS.on('scan_started', (msg) => {
    Activity.started('scan-' + msg.locationId, {
        label: `Scanning: ${msg.location}`,
        detail: 'starting...',
        locationId: msg.locationId,
        log: `Scan started: <b>${msg.location}</b>`,
    });
    updateLocationOnline(msg.locationId, true);
    Tree.setScanningLocation(msg.locationId);
});

WS.on('scan_progress', (msg) => {
    let statusDetail;
    let logText;
    if (msg.phase === 'scanning') {
        statusDetail = `${(msg.filesFound || 0).toLocaleString()} files, ${(msg.dirsFound || 0).toLocaleString()} dirs`;
        logText = `${msg.location} — ${(msg.filesFound || 0).toLocaleString()} files found`;
    } else if (msg.phase === 'hashing') {
        const total = msg.hashesTotal || 0;
        const done = msg.hashesDone || 0;
        const pct = total > 0 ? ` (${Math.round(done / total * 100)}%)` : '';
        statusDetail = `Hashing: ${done.toLocaleString()} / ${total.toLocaleString()}${pct}`;
        logText = `${msg.location} — ${done.toLocaleString()} / ${total.toLocaleString()} hashed`;
    } else if (msg.phase === 'comparing') {
        const step = msg.compareStep || '';
        statusDetail = `Comparing: ${step || 'starting...'}`;
        logText = `${msg.location} — comparing: ${step || 'starting'}`;
    } else if (msg.phase === 'cataloging') {
        const total = msg.catalogTotal || 0;
        const done = msg.catalogDone || 0;
        const pct = total > 0 ? ` (${Math.round(done / total * 100)}%)` : '';
        statusDetail = `Cataloging: ${done.toLocaleString()} / ${total.toLocaleString()}${pct}`;
        logText = `${msg.location} — ${done.toLocaleString()} / ${total.toLocaleString()} cataloged`;
    } else if (msg.phase === 'cataloging_hashes') {
        const total = msg.catalogTotal || 0;
        const done = msg.catalogDone || 0;
        const pct = total > 0 ? ` (${Math.round(done / total * 100)}%)` : '';
        statusDetail = `Saving hashes: ${done.toLocaleString()} / ${total.toLocaleString()}${pct}`;
        logText = `${msg.location} — ${done.toLocaleString()} / ${total.toLocaleString()} hashes saved`;
    } else if (msg.phase === 'checking_duplicates') {
        const candidates = msg.candidates || 0;
        if (candidates > 0) {
            statusDetail = `Checking duplicates: ${candidates.toLocaleString()} candidates`;
            logText = `${msg.location} — ${candidates.toLocaleString()} candidates found`;
        } else {
            statusDetail = 'Checking duplicates...';
            logText = `${msg.location} — checking duplicates`;
        }
    } else if (msg.phase === 'confirming_duplicates') {
        const total = msg.dupsTotal || 0;
        const done = msg.dupsProcessed || 0;
        const confirmed = msg.dupsConfirmed || 0;
        const pct = total > 0 ? ` (${Math.round(done / total * 100)}%)` : '';
        statusDetail = `confirming duplicates: ${done.toLocaleString()} / ${total.toLocaleString()}${pct}`;
        logText = null; // no activity log spam — indicator only
    } else if (msg.phase === 'recounting') {
        const total = msg.checksTotal || 0;
        const done = msg.checksDone || 0;
        const progress = total > 0 ? `: ${done.toLocaleString()} / ${total.toLocaleString()}` : '';
        statusDetail = `Counting duplicates${progress}`;
        logText = `${msg.location} — counting duplicates${progress}`;
    } else if (msg.phase === 'rebuilding') {
        statusDetail = 'Rebuilding statistics...';
        logText = `${msg.location} — rebuilding statistics`;
    } else {
        statusDetail = `${(msg.filesFound || 0).toLocaleString()} files`;
        logText = `${msg.location} — ${(msg.filesFound || 0).toLocaleString()} files`;
    }
    Activity.progress('scan-' + msg.locationId, {
        label: `Scanning: ${msg.location}`,
        detail: statusDetail,
        log: logText,
    });
    StatusBar.updateStatsFromProgress(msg);
    Detail.updateFromScanProgress(msg);
    updateLocationOnline(msg.locationId, true);
    if (msg.totalSize !== undefined) Tree.updateLocationSize(msg.locationId, msg.totalSize);
    Tree.setScanningLocation(msg.locationId, msg.phase);
});

WS.on('location_children', async (msg) => {
    Tree.setLocationChildren(msg.locationId, msg.children);
    if (selectedNode && selectedNode.id === 'loc-' + msg.locationId) {
        await FileList.showFolder(selectedNode.id);
        await Detail.refreshStats();
    }
});

WS.on('scan_completed', async (msg) => {
    let logText;
    if (msg.quickScan) {
        const parts = [];
        if (msg.newFiles) parts.push(`${msg.newFiles} new`);
        if (msg.staleFiles) parts.push(`${msg.staleFiles} stale`);
        if (msg.newFolders) parts.push(`${msg.newFolders} new folders`);
        if (msg.recoveredFiles) parts.push(`${msg.recoveredFiles} recovered`);
        const detail = parts.length ? parts.join(', ') : 'no changes';
        logText = `Quick scan completed: <b>${msg.location}</b> — ${detail}`;
        if (parts.length) Toast.success(`Quick scan completed: ${msg.location} — ${detail}`);
    } else {
        const skippedPart = msg.filesSkipped ? `, ${msg.filesSkipped.toLocaleString()} skipped` : '';
        const stalePart = msg.staleFiles ? `, ${msg.staleFiles.toLocaleString()} stale` : '';
        logText = `Scan completed: <b>${msg.location}</b> — ${(msg.filesHashed || 0).toLocaleString()} hashed${skippedPart}, ${(msg.duplicatesFound || 0).toLocaleString()} duplicates${stalePart}`;
        Toast.success(`Scan completed: ${msg.location}`);
    }
    Activity.completed('scan-' + msg.locationId, { log: logText });
    Tree.clearScanningLocation(msg.locationId);
    if (msg.totalSize !== undefined) Tree.updateLocationSize(msg.locationId, msg.totalSize);
    FileList._fresh = true;
    if (selectedNode) await FileList.refreshFolder();
    await StatusBar.loadStats();
    await Detail.refreshStats();
});

WS.on('scan_finalizing', (msg) => {
    Activity.progress('scan-' + msg.locationId, {
        detail: 'finalizing...',
        log: `Scan finalizing: <b>${msg.location}</b> — marking stale files`,
    });
    Tree.clearScanningLocation(msg.locationId);
});

WS.on('scan_cancelled', async (msg) => {
    const skippedPart = msg.filesSkipped ? `, ${msg.filesSkipped.toLocaleString()} skipped` : '';
    Activity.completed('scan-' + msg.locationId, {
        log: `Scan cancelled: <b>${msg.location}</b> — ${msg.filesHashed.toLocaleString()} hashed${skippedPart} before cancel`,
    });
    Toast.info(`Scan cancelled: ${msg.location}`);
    Tree.clearScanningLocation(msg.locationId);
    await StatusBar.loadStats();
    await Detail.refreshStats();
});

WS.on('scan_interrupted', async (msg) => {
    Activity.error('scan-' + msg.locationId, {
        log: `Scan interrupted: <b>${msg.location}</b> — agent disconnected, will resume automatically`,
    });
    Toast.info(`Scan interrupted: ${msg.location} — will resume when agent reconnects`);
    Tree.clearScanningLocation(msg.locationId);
});

WS.on('scan_error', (msg) => {
    Activity.error('scan-' + msg.locationId, {
        log: `Scan error: <b>${msg.location}</b> — ${msg.error}`,
    });
    Tree.clearScanningLocation(msg.locationId);
    Toast.error(`Scan error: ${msg.error}`);
});

function syncQueuedLocations(queue) {
    StatusBar.updateQueue(queue);

    // Collect all locations that currently have badges
    const affected = new Set([
        ...Tree._scanningLocations,
        ...Tree._backfillingLocations,
        ...Tree._deletingLocations,
        ...Tree._queuedLocations.keys(),
    ]);

    // Rebuild all badge sets from queue state (single source of truth)
    Tree._scanningLocations.clear();
    Tree._backfillingLocations.clear();
    Tree._deletingLocations.clear();
    Tree._queuedLocations.clear();
    if (queue) {
        if (queue.scanning_location_ids) {
            for (const locId of queue.scanning_location_ids) {
                const key = 'loc-' + locId;
                Tree._scanningLocations.add(key);
                affected.add(key);
            }
        }
        if (queue.backfilling_location_ids) {
            for (const locId of queue.backfilling_location_ids) {
                const key = 'loc-' + locId;
                Tree._backfillingLocations.add(key);
                affected.add(key);
            }
        }
        if (queue.deleting_location_ids) {
            for (const locId of queue.deleting_location_ids) {
                const key = 'loc-' + locId;
                Tree._deletingLocations.add(key);
                affected.add(key);
            }
        }
        // Fallback for old server without typed ids
        if (!queue.scanning_location_ids && queue.running_location_ids) {
            for (const locId of queue.running_location_ids) {
                const key = 'loc-' + locId;
                Tree._scanningLocations.add(key);
                affected.add(key);
            }
        }
        if (queue.pending) {
            for (const entry of queue.pending) {
                const key = 'loc-' + entry.location_id;
                Tree._queuedLocations.set(key, entry.queue_id);
                affected.add(key);
            }
        }
    }

    // Targeted badge update for each affected location
    for (const nodeId of affected) {
        Tree._updateLocationBadges(nodeId);
    }
    Detail.updateActivity();
}

WS.on('scan_queued', (msg) => {
    syncQueuedLocations(msg.queue);
    const q = msg.queue;
    let reason = '';
    if (q && q.deleting_location_ids && q.deleting_location_ids.length > 0) {
        reason = ' (waiting for location delete to finish)';
    } else if (q && q.running_location_ids && q.running_location_ids.length > 0) {
        reason = ' (waiting for current operation to finish)';
    }
    ActivityLog.add(`Scan queued: <b>${msg.entry.name}</b>${reason}`);
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

WS.on('queue_paused', (msg) => {
    Tree._paused = true;
    for (const loc of Tree.treeData) Tree._updateLocationBadges(loc.id);
    if (msg.reason === 'dup_exclude') {
        const verb = msg.direction === 'exclude' ? 'Excluding' : 'Including';
        const prep = msg.direction === 'exclude' ? 'from' : 'in';
        ActivityLog.add(`Operations paused: ${verb.toLowerCase()} folder ${prep} duplicates`);
        _startDupExcludePoll();
    } else {
        ActivityLog.add(`Operations paused: importing <b>${msg.location}</b>`);
    }
});

WS.on('queue_resumed', () => {
    Tree._paused = false;
    for (const loc of Tree.treeData) Tree._updateLocationBadges(loc.id);
    ActivityLog.add('Operations resumed');
});

WS.on('backfill_started', (msg) => {
    const statusDetail = msg.totalFiles
        ? `0/${msg.totalFiles.toLocaleString()}`
        : 'preparing...';
    const logDetail = msg.totalFiles
        ? `${msg.totalFiles.toLocaleString()} files`
        : 'finding candidates...';
    Activity.started('backfill-' + msg.locationId, {
        label: `Hashing: ${msg.location}`,
        detail: statusDetail,
        locationId: msg.locationId,
        log: `Hash backfill started: <b>${msg.location}</b> — ${logDetail}`,
    });
    Tree.setBackfillingLocation(msg.locationId);
});

WS.on('backfill_progress', (msg) => {
    let detail;
    if (msg.phase === 'cross_location') {
        detail = msg.filesHashed != null && msg.totalFiles
            ? `comparing across locations (${msg.filesHashed.toLocaleString()}/${msg.totalFiles.toLocaleString()})`
            : 'comparing across locations...';
    } else if (msg.phase === 'local') {
        detail = 'hashing local matches';
    } else if (msg.phase === 'dup_counts') {
        detail = 'updating duplicate counts';
    } else {
        detail = `${msg.filesHashed.toLocaleString()}/${msg.totalFiles.toLocaleString()}`;
    }
    const log = !msg.phase
        ? `Hash backfill: <b>${msg.location}</b> — ${msg.filesHashed.toLocaleString()} / ${msg.totalFiles.toLocaleString()} files`
        : undefined;
    Activity.progress('backfill-' + msg.locationId, { detail, log });
    Tree.setBackfillingLocation(msg.locationId);
    FileList.refreshDupCounts();
    StatusBar.loadStats();
});

WS.on('backfill_completed', async (msg) => {
    const summary = msg.agentFilesHashed
        ? `${msg.agentFilesHashed.toLocaleString()} files hashed`
        : 'no matches found';
    const label = msg.cancelled ? 'Hash backfill cancelled' : 'Hash backfill completed';
    Activity.completed('backfill-' + msg.locationId, {
        log: `${label}: <b>${msg.location}</b> — ${summary}`,
    });
    Tree.clearBackfillingLocation(msg.locationId);
    await StatusBar.loadStats();
    await reloadTreeAndFileList(selectedFile ? selectedFile.id : null);
    await Detail.refreshStats();
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
    StatusBar.loadStats();
});
WS.on('recalc_progress', msg => {
    const recalcBtn = document.getElementById('detail-recalc-stats');
    if (recalcBtn) {
        recalcBtn.textContent = `Recalculating: ${msg.location} (${msg.done}/${msg.total})`;
    }
    ActivityLog.add(`Recalculating: ${msg.location} (${msg.done}/${msg.total})`);
});
WS.on('server_activity', (msg) => {
    Activity.sync(msg);
});

WS.on('size_recalc_completed', async msg => {
    ActivityLog.add('Location sizes recalculated');
    await Tree.reload();
    if (selectedNode) selectedNode = Tree._findNode(selectedNode.id);
    StatusBar.loadStats();
    Detail.refreshStats();
});
WS.on('dup_recalc_started', (msg) => {
    if (msg.locationIds) {
        for (const id of msg.locationIds) Detail._dupRecalcLocations.add(id);
        Detail._applyDupRecalcOverride();
    }
    // Don't show separate activity if this is part of an active scan
    const duringActiveScan = (msg.locationIds || []).some(id => Tree._scanningLocations.has('loc-' + id));
    if (!duringActiveScan) {
        Activity.started('dup-recalc', {
            label: 'Recalculating duplicates',
            log: 'Recalculating duplicate counts...',
        });
    }
});
WS.on('dup_recalc_completed', async (msg) => {
    if (msg.locationIds) {
        for (const id of msg.locationIds) Detail._dupRecalcLocations.delete(id);
    }
    const count = msg.hashCount ? msg.hashCount.toLocaleString() : '0';
    const duringActiveScan = (msg.locationIds || []).some(id => Tree._scanningLocations.has('loc-' + id));
    if (!duringActiveScan) {
        Activity.completed('dup-recalc', {
            log: `Duplicate recalculation complete — ${count} hashes processed`,
        });
    } else {
        // Silently dismiss if it was started before we could suppress
        Activity.completed('dup-recalc', {});
    }
    await StatusBar.loadStats();
    await Detail.refreshStats();
    FileList.refreshDupCounts();
});

// --- Dup exclude progress polling ---
let _dupExcludePollTimer = null;
function _startDupExcludePoll() {
    if (_dupExcludePollTimer) return;
    Activity.started('dup-exclude', { label: 'Updating exclusions', log: false });
    _dupExcludePollTimer = setInterval(async () => {
        const res = await API.get('/api/dup-exclude/progress');
        if (!res.ok) return;
        const p = res.data;
        if (p.status === 'flagging') {
            const pct = p.files_total > 0 ? Math.round((p.files_done / p.files_total) * 100) : 0;
            Activity.progress('dup-exclude', { detail: `Flagging files... ${p.files_done.toLocaleString()} / ${p.files_total.toLocaleString()} (${pct}%)` });
        } else if (p.status === 'recounting') {
            const pct = p.hashes_total > 0 ? Math.round((p.hashes_done / p.hashes_total) * 100) : 0;
            Activity.progress('dup-exclude', { detail: `Recounting duplicates... ${p.hashes_done.toLocaleString()} / ${p.hashes_total.toLocaleString()} (${pct}%)` });
        } else if (p.status === 'zeroing') {
            Activity.progress('dup-exclude', { detail: 'Zeroing excluded file counts...' });
        } else if (p.status === 'rebuilding') {
            Activity.progress('dup-exclude', { detail: 'Rebuilding location sizes...' });
        } else if (p.status === 'pausing') {
            Activity.progress('dup-exclude', { detail: 'Pausing operations...' });
        } else if (p.status === 'complete' || p.status === 'error' || p.status === 'idle') {
            _stopDupExcludePoll();
        }
        await StatusBar.loadStats();
    }, 500);
}
function _stopDupExcludePoll() {
    if (_dupExcludePollTimer) {
        clearInterval(_dupExcludePollTimer);
        _dupExcludePollTimer = null;
        Activity.completed('dup-exclude');
    }
}

WS.on('dup_exclude_completed', async msg => {
    _stopDupExcludePoll();
    const verb = msg.direction === 'exclude' ? 'excluded' : 'included';
    ActivityLog.add(`Duplicate exclusion updated: <b>${msg.folder}</b> ${verb} — ${(msg.fileCount || 0).toLocaleString()} files, ${(msg.hashCount || 0).toLocaleString()} hashes recalculated`);
    await reloadTreeAndFileList();
    await Detail.refreshStats();
    await StatusBar.loadStats();
});

WS.on('location_changed', async (msg) => {
    ActivityLog.add(`Location ${msg.action}: <b>${msg.location?.label || msg.locationId || ''}</b>`);
    if (msg.action === 'connected' || msg.action === 'disconnected') {
        // Online status already handled by agent_status event
        return;
    }
    await Tree.reload();
    if (selectedNode) selectedNode = Tree._findNode(selectedNode.id);
    await StatusBar.loadStats();
    await refreshDetailPanel();
});

WS.on('import_completed', async (msg) => {
    Tree.clearScanningLocation(msg.locationId);
    Activity.completed('import-' + msg.locationId, {
        log: `Location imported: <b>${msg.location}</b>`,
    });
    Toast.success(`Import completed: ${msg.location}`);
    await Tree.reload();
    if (selectedNode) selectedNode = Tree._findNode(selectedNode.id);
    await StatusBar.loadStats();
    await Detail.refreshStats();
});

WS.on('location_deleting', (msg) => {
    ActivityLog.add(`Deleting location: <b>${msg.name}</b>...`);
    Tree.setDeletingLocation(msg.locationId);
    // If viewing the deleting location, clear panels
    if (selectedNode && selectedNode.id === msg.locationId) {
        selectedNode = null;
        selectedFile = null;
        const filePanel = document.getElementById('file-content');
        if (filePanel) filePanel.innerHTML = '<div class="panel-body" style="padding: 1rem; color: var(--color-text-placeholder);">Deleting...</div>';
        const detailPanel = document.getElementById('detail-content');
        if (detailPanel) detailPanel.innerHTML = '<div class="panel-body" style="padding: 1rem; color: var(--color-text-placeholder);">Deleting...</div>';
    }
});

WS.on('location_deleted', async (msg) => {
    ActivityLog.add(`Location deleted: <b>${msg.name}</b>`);
    Toast.success(`Location deleted: ${msg.name}`);
    Tree.clearDeletingLocation(msg.locationId);
    if (selectedNode && selectedNode.id === msg.locationId) {
        selectedNode = null;
        selectedFile = null;
    }
    await Tree.reload();
    if (selectedNode) selectedNode = Tree._findNode(selectedNode.id);
    await StatusBar.loadStats();
    await refreshDetailPanel();
});

WS.on('stats_updated', async () => {
    await StatusBar.loadStats();
    await Detail.refreshStats();
});

WS.on('repair_started', () => {
    ActivityLog.add('Catalog repair started...');
});

WS.on('repair_completed', async (msg) => {
    const parts = [];
    if (msg.hashed > 0) parts.push(`${msg.hashed} files hashed`);
    if (msg.skipped > 0) parts.push(`${msg.skipped} skipped`);
    if (msg.dupHashes > 0) parts.push(`${msg.dupHashes} hashes recounted`);
    if (msg.locations > 0) parts.push(`${msg.locations} locations recalculated`);
    ActivityLog.add(`Catalog repair complete: ${parts.join(', ') || 'no changes'}`);
    await StatusBar.loadStats();
    await Detail.refreshStats();
});

WS.on('repair_failed', () => {
    ActivityLog.add('Catalog repair failed — check server logs');
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
    await reloadTreeAndFileList(selectedFile ? selectedFile.id : null);
    await StatusBar.loadStats();
    await refreshDetailPanel();
});

WS.on('folder_moved', async (msg) => {
    await reloadTreeAndFileList(selectedFile ? selectedFile.id : null);
    await StatusBar.loadStats();
    await refreshDetailPanel();
});

WS.on('file_moved', async (msg) => {
    if (selectedFile && selectedFile.id === msg.fileId) {
        selectedFile = null;
        selectedFileDups = [];
        consolidateBtn.disabled = true;
    }
    const currentFolderId = selectedNode ? selectedNode.id : null;
    await Tree.reload();
    if (currentFolderId) {
        selectedNode = Tree._findNode(currentFolderId);
        await FileList.showFolder(currentFolderId);
    }
    await StatusBar.loadStats();
    await refreshDetailPanel();
});

WS.on('deferred_op_created', async (msg) => {
    const label = msg.opType === 'delete' ? 'deletion' : msg.opType === 'move' ? 'move' : msg.opType;
    Toast.info(`${msg.filename}: ${label} queued for when location comes online`);
    ActivityLog.add(`Deferred ${label}: <b>${msg.filename}</b>`);
    await StatusBar.loadStats();
    if (selectedNode) await FileList.showFolder(selectedNode.id);
    await refreshDetailPanel();
});

WS.on('deferred_op_cancelled', async (msg) => {
    await StatusBar.loadStats();
    if (selectedNode) await FileList.showFolder(selectedNode.id);
    await refreshDetailPanel();
});

WS.on('deferred_ops_drained', async (msg) => {
    if (msg.completed > 0) {
        Toast.info(`${msg.completed} deferred file operation${msg.completed === 1 ? '' : 's'} completed`);
        ActivityLog.add(`Drained ${msg.completed} deferred file ops for location #${msg.locationId}`);
    }
    await StatusBar.loadStats();
    if (selectedNode) await FileList.showFolder(selectedNode.id);
    await refreshDetailPanel();
});

WS.on('batch_delete_progress', (msg) => {
    const detail = msg.name
        ? `${msg.name} (${msg.done}/${msg.total})`
        : `${msg.done}/${msg.total}`;
    Activity.progress('batch-delete', { label: 'Deleting', detail });
});

WS.on('batch_deleted', async (msg) => {
    Activity.completed('batch-delete');
    const files = msg.deletedFiles || 0;
    const folders = msg.deletedFolders || 0;
    ActivityLog.add(`Batch deleted: <b>${files} files, ${folders} folders</b>`);
    Toast.success(`Deleted ${files} files, ${folders} folders`);
    selectedFile = null;
    selectedFileDups = [];
    await reloadTreeAndFileList(selectedFile ? selectedFile.id : null);
    await StatusBar.loadStats();
    await refreshDetailPanel();
});

WS.on('batch_move_progress', (msg) => {
    const detail = msg.name
        ? `${msg.name} (${msg.done}/${msg.total})`
        : `${msg.done}/${msg.total}`;
    Activity.progress('batch-move', { label: 'Moving', detail });
});

WS.on('batch_tag_completed', async (msg) => {
    const n = msg.updated || 0;
    const tags = (msg.add_tags || []).concat(msg.remove_tags || []);
    const label = tags.length === 1 ? `"${tags[0]}"` : `${tags.length} tags`;
    const verb = (msg.add_tags || []).length ? 'Tagged' : 'Untagged';
    ActivityLog.add(`${verb} <b>${n} file${n !== 1 ? 's' : ''}</b> with ${label}`);
    Toast.success(`${verb} ${n} file${n !== 1 ? 's' : ''}`);
    await refreshDetailPanel();
});

WS.on('status_bar_idle', () => {
    // Server explicitly requests idle — clear all ops
    Activity._ops.clear();
    Activity._render();
});

// --- ZIP download (async build) ---

WS.on('zip_progress', (msg) => {
    Activity.progress('zip-' + msg.jobId, {
        label: `Building ZIP: ${msg.filename}`,
        detail: `${msg.done}/${msg.total} files`,
    });
    if (_pendingZipBtn) {
        _pendingZipBtn.textContent = `Building ZIP\u2026 ${msg.done}/${msg.total}`;
    }
});

WS.on('zip_ready', (msg) => {
    Activity.completed('zip-' + msg.jobId, {
        log: `ZIP ready: <b>${msg.filename}</b>`,
    });
    Toast.success(`ZIP ready: ${msg.filename}`);
    // Trigger download via direct link
    const token = localStorage.getItem('fh-token');
    let url = `/api/zip/${msg.jobId}/download`;
    if (token) url += `?token=${encodeURIComponent(token)}`;
    const a = document.createElement('a');
    a.href = url;
    a.download = msg.filename;
    document.body.appendChild(a);
    a.click();
    a.remove();
    if (_pendingZipBtn) {
        _pendingZipBtn.disabled = false;
        _pendingZipBtn.textContent = _pendingZipOrigText;
        _pendingZipBtn = null;
    }
});

WS.on('zip_error', (msg) => {
    Activity.error('zip-' + msg.jobId, {
        log: `ZIP build failed: <b>${msg.filename}</b>`,
    });
    Toast.error(`ZIP build failed: ${msg.filename}`);
    if (_pendingZipBtn) {
        _pendingZipBtn.disabled = false;
        _pendingZipBtn.textContent = _pendingZipOrigText;
        _pendingZipBtn = null;
    }
});

WS.on('batch_moved', async (msg) => {
    Activity.completed('batch-move');
    selectedFile = null;
    selectedFileDups = [];
    await reloadTreeAndFileList();
    await StatusBar.loadStats();
    await refreshDetailPanel();
});

WS.on('folder_deleted', async (msg) => {
    await reloadTreeAndFileList(selectedFile ? selectedFile.id : null);
    await StatusBar.loadStats();
    await refreshDetailPanel();
});

WS.on('consolidate_started', (msg) => {
    Activity.started('consolidate', {
        label: 'Consolidating',
        detail: msg.filename,
        log: `Consolidation started: <b>${msg.filename}</b>`,
    });
    if (msg.locationId) Tree.setMergingLocation(msg.locationId, 'consolidating...');
});

WS.on('consolidate_progress', (msg) => {
    let detail;
    if (msg.phase === 'copying') {
        detail = StatusBar.formatCopyProgress(msg.filename, msg.bytesSent, msg.bytesTotal);
    } else if (msg.phase === 'verifying') {
        detail = `${msg.filename} — Verifying hashes`;
    } else if (msg.phase === 'stubs') {
        detail = `${msg.filename} — Writing stubs ${msg.current}/${msg.total}`;
    }
    if (detail) Activity.progress('consolidate', { detail });
});

WS.on('consolidate_completed', async (msg) => {
    if (msg.destLocationId) Tree.clearMergingLocation(msg.destLocationId);
    if (msg.batch) return; // batch_consolidate_completed handles UI
    Activity.completed('consolidate', {
        log: `Consolidation completed: <b>${msg.filename}</b> — ${msg.stubsWritten} stubs written, ${msg.stubsQueued} queued`,
    });
    Toast.success(`Consolidation completed: ${msg.filename}`);
    selectedFile = null;
    selectedFileDups = [];
    consolidateBtn.disabled = true;
    await StatusBar.loadStats();
    await reloadTreeAndFileList();
    await refreshDetailPanel();
});

WS.on('consolidate_error', (msg) => {
    if (msg.destLocationId) Tree.clearMergingLocation(msg.destLocationId);
    let detail = msg.error;
    if (msg.stubsWritten > 0 || msg.stubsQueued > 0) {
        detail += ` (${msg.stubsWritten} stubs written, ${msg.stubsQueued} queued before error)`;
    }
    Activity.error('consolidate', {
        log: `Consolidation error: <b>${msg.filename}</b> — ${detail}`,
    });
    Toast.error(`Consolidation error: ${msg.error}`);
});

WS.on('consolidate_queue_drained', (msg) => {
    ActivityLog.add(`Queue drained: ${msg.jobsCompleted} pending consolidation jobs completed`);
    Toast.info(`${msg.jobsCompleted} deferred consolidation job${msg.jobsCompleted === 1 ? '' : 's'} completed`);
});

WS.on('batch_consolidate_completed', async (msg) => {
    const parts = [`${msg.completed} of ${msg.total} files`];
    if (msg.skipped) parts.push(`${msg.skipped} duplicates skipped`);
    const summary = parts.join(', ');
    Activity.completed('consolidate', {
        log: `Batch consolidation completed: ${summary}`,
    });
    Toast.success(`Batch consolidation completed: ${summary}`);
    await StatusBar.loadStats();
    await reloadTreeAndFileList();
    await refreshDetailPanel();
});

WS.on('rehash_progress', (msg) => {
    Activity.progress('rehash', {
        label: 'Re-hashing',
        detail: `${msg.processed}/${msg.total} — ${msg.filename}`,
    });
});

WS.on('rehash_completed', async (msg) => {
    Activity.completed('rehash', {
        log: `Re-hash completed: ${msg.total} file${msg.total !== 1 ? 's' : ''}`,
    });
    Toast.success(`Re-hash completed: ${msg.total} file${msg.total !== 1 ? 's' : ''}`);
    if (selectedNode) {
        await FileList.showFolder(selectedNode.id);
    }
    await refreshDetailPanel();
});

WS.on('verify_progress', (msg) => {
    Activity.progress('verify', {
        label: 'Verifying',
        detail: `${msg.verified}/${msg.total} — ${msg.filename}`,
    });
});

WS.on('verify_completed', async (msg) => {
    const parts = [`${msg.verified} verified`];
    if (msg.deferred > 0) parts.push(`${msg.deferred} queued`);
    if (msg.skipped > 0) parts.push(`${msg.skipped} skipped`);
    Activity.completed('verify', {
        log: `Verification complete: ${parts.join(', ')}`,
    });
    Toast.success(`Verification complete: ${parts.join(', ')}`);
    if (selectedNode) {
        await FileList.showFolder(selectedNode.id);
    }
    await refreshDetailPanel();
});

WS.on('upload_transfer', (msg) => {
    const filePart = `${msg.current}/${msg.total} ${msg.filename}`;
    const sizePart = msg.totalMB > 1 ? ` — ${msg.sentMB}/${msg.totalMB} MB (${msg.pct}%)` : '';
    Activity.progress('upload', { detail: `sending to agent ${filePart}${sizePart}` });
});

WS.on('upload_started', (msg) => {
    Activity.started('upload', {
        label: `Uploading: ${msg.location}`,
        detail: `${msg.fileCount} file(s)...`,
        log: `Upload started: <b>${msg.location}</b> — ${msg.fileCount} file(s)`,
    });
});

WS.on('upload_progress', (msg) => {
    Activity.progress('upload', { detail: `${msg.processed}/${msg.total} (${msg.currentFile})` });
});

WS.on('upload_duplicate', (msg) => {
    Toast.info(`Duplicate: ${msg.filename} — already in ${msg.existingLocation}`);
    ActivityLog.add(`Upload duplicate: <b>${msg.filename}</b> — exists in ${msg.existingLocation}`);
});

WS.on('upload_file_error', (msg) => {
    Toast.error(`Upload error: ${msg.filename} — ${msg.error}`);
    ActivityLog.add(`Upload error: <b>${msg.filename}</b> — ${msg.error}`);
});

WS.on('file_freshness', (msg) => {
    if (!FileList.currentItems) return;
    const item = FileList.currentItems.find(f => f.id === msg.fileId);
    if (!item) return;
    if (msg.stale !== undefined) item.stale = msg.stale;
    if (msg.size !== undefined) item.size = msg.size;
    FileList.render();
});

WS.on('file_added', (msg) => {
    // Append to file list if viewing the same folder
    const viewingFolder = FileList.currentFolder;
    if (!viewingFolder || !msg.folderId) return;
    const viewId = String(viewingFolder);
    const folderId = String(msg.folderId);
    // Match loc-N to loc-N or fld-N to fld-N
    if (viewId === `loc-${folderId}` || viewId === `fld-${folderId}` || viewId === folderId) {
        if (!FileList.currentItems) FileList.currentItems = [];
        FileList.currentItems.push(msg.file);
        FileList.totalFiles++;
        FileList.render();
    }
});

WS.on('upload_completed', async (msg) => {
    Activity.completed('upload', {
        log: `Upload completed: <b>${msg.location}</b> — ${msg.cataloged} cataloged, ${msg.duplicates} duplicates`,
    });
    Toast.success(`Upload completed: ${msg.cataloged} cataloged, ${msg.duplicates} duplicates`);
    await StatusBar.loadStats();
    await reloadTreeAndFileList(selectedFile ? selectedFile.id : null);
    await refreshDetailPanel();
});

WS.on('merge_started', (msg) => {
    const verb = msg.mode === 'copy' ? 'Copying' : 'Moving';
    Activity.started('merge', {
        label: `${verb}: ${msg.source} → ${msg.destination}`,
        detail: 'starting...',
        log: `${verb} started: <b>${msg.source}</b> → <b>${msg.destination}</b>`,
    });
    const badge = msg.mode === 'copy' ? 'copying...' : 'merging...';
    if (msg.srcLocationId) Tree.setMergingLocation(msg.srcLocationId, badge);
    if (msg.destLocationId) Tree.setMergingLocation(msg.destLocationId, badge);
});

WS.on('merge_progress', (msg) => {
    const parts = [`${msg.copied} copied`];
    if (msg.duplicate > 0) parts.push(`${msg.duplicate} duplicate`);
    if (msg.skipped > 0) parts.push(`${msg.skipped} skipped`);
    Activity.progress('merge', {
        detail: `${msg.processed}/${msg.total} files (${parts.join(', ')})`,
        log: `${msg.mode === 'copy' ? 'Copying' : 'Moving'}: ${msg.source} → ${msg.destination} — ${msg.processed}/${msg.total} files`,
    });
});

WS.on('merge_completed', async (msg) => {
    if (msg.srcLocationId) Tree.clearMergingLocation(msg.srcLocationId);
    if (msg.destLocationId) Tree.clearMergingLocation(msg.destLocationId);
    const parts = [`${msg.filesCopied} copied`];
    if (msg.filesDuplicate > 0) parts.push(`${msg.filesDuplicate} duplicate`);
    if (msg.filesSkipped > 0) parts.push(`${msg.filesSkipped} skipped`);
    const verb = msg.mode === 'copy' ? 'Copy' : 'Merge';
    Activity.completed('merge', {
        log: `${verb} completed: <b>${msg.source}</b> → <b>${msg.destination}</b> — ${parts.join(', ')}`,
    });
    Toast.success(`${verb} completed: ${parts.join(', ')}`);
    await StatusBar.loadStats();
    await reloadTreeAndFileList(selectedFile ? selectedFile.id : null);
    await refreshDetailPanel();
});

WS.on('merge_cancelled', async (msg) => {
    if (msg.srcLocationId) Tree.clearMergingLocation(msg.srcLocationId);
    if (msg.destLocationId) Tree.clearMergingLocation(msg.destLocationId);
    const parts = [];
    if (msg.copied > 0) parts.push(`${msg.copied} copied`);
    if (msg.duplicate > 0) parts.push(`${msg.duplicate} duplicate`);
    const detail = parts.length > 0 ? ` — ${parts.join(', ')} before cancel` : '';
    Activity.completed('merge', {
        log: `Merge cancelled: <b>${msg.source}</b> → <b>${msg.destination}</b>${detail}`,
    });
    Toast.info(`Merge cancelled: ${msg.source} → ${msg.destination}`);
    await StatusBar.loadStats();
    await reloadTreeAndFileList(selectedFile ? selectedFile.id : null);
    await refreshDetailPanel();
});

WS.on('merge_error', (msg) => {
    if (msg.srcLocationId) Tree.clearMergingLocation(msg.srcLocationId);
    if (msg.destLocationId) Tree.clearMergingLocation(msg.destLocationId);
    Activity.error('merge', {
        log: `Merge error: <b>${msg.source}</b> → <b>${msg.destination}</b> — ${msg.error}`,
    });
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
    await reloadTreeAndFileList();
    await refreshDetailPanel();
});

WS.on('agent_status', (msg) => {
    const label = msg.agentId ? `Agent #${msg.agentId}` : 'Agent';
    ActivityLog.add(`${label} ${msg.status}`);
    const online = msg.status === 'online';
    if (msg.locationIds) {
        Tree.updateOnlineStatus(msg.locationIds, online, msg.diskStats);
    }
});

WS.on('agent_capabilities', (msg) => {
    const caps = msg.capabilities && msg.capabilities.length > 0
        ? msg.capabilities.join(', ')
        : 'none';
    const name = msg.agentName || msg.hostname || `Agent #${msg.agentId}`;
    ActivityLog.add(`${name} capabilities: ${caps}`);
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
