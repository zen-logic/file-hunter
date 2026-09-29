import API from '../api.js';
import Toast from './toast.js';
import { createFolderPicker } from './folderpicker.js';
import { wireModal } from './modal.js';

const SlideshowTriage = {
    // Delete dialog elements
    delOverlay: null,
    delText: null,
    delList: null,
    delDupsCheck: null,
    delCancel: null,
    delSubmit: null,

    // Consolidate — delegated to unified Consolidate component
    consolidateOpen: null,

    // Tag dialog elements
    tagOverlay: null,
    tagText: null,
    tagList: null,
    tagInput: null,
    tagCancel: null,
    tagSubmit: null,

    // Move dialog elements
    movOverlay: null,
    movText: null,
    movList: null,
    movTree: null,
    movDest: null,
    movCancel: null,
    movSubmit: null,

    // State
    deleteItems: [],
    consolidateItems: [],
    tagItems: [],
    moveItems: [],
    picker: null,

    init() {
        // Delete dialog
        this.delOverlay = document.getElementById('slideshow-delete-modal');
        this.delText = document.getElementById('slideshow-delete-text');
        this.delList = document.getElementById('slideshow-delete-list');
        this.delDupsCheck = document.getElementById('slideshow-delete-dups-check');
        this.delCancel = document.getElementById('slideshow-delete-cancel');
        this.delSubmit = document.getElementById('slideshow-delete-submit');

        wireModal(this.delOverlay, {
            close: () => this.closeDelete(),
            submit: () => this.doDelete(),
            enterFrom: 'dialog',
            cancelBtn: this.delCancel,
            submitBtn: this.delSubmit,
        });

        // Tag dialog
        this.tagOverlay = document.getElementById('slideshow-tag-modal');
        this.tagText = document.getElementById('slideshow-tag-text');
        this.tagList = document.getElementById('slideshow-tag-list');
        this.tagInput = document.getElementById('slideshow-tag-input');
        this.tagCancel = document.getElementById('slideshow-tag-cancel');
        this.tagSubmit = document.getElementById('slideshow-tag-submit');

        wireModal(this.tagOverlay, {
            close: () => this.closeTag(),
            submit: () => this.doTag(),
            enterFrom: 'dialog',
            cancelBtn: this.tagCancel,
            submitBtn: this.tagSubmit,
        });

        // Move dialog
        this.movOverlay = document.getElementById('slideshow-move-modal');
        this.movText = document.getElementById('slideshow-move-text');
        this.movList = document.getElementById('slideshow-move-list');
        this.movTree = document.getElementById('slideshow-move-tree');
        this.movDest = document.getElementById('slideshow-move-dest');
        this.picker = createFolderPicker(this.movTree, {
            isDisabled: (node) => node.online === false,
            onPick: (id, label) => {
                this.movDest.textContent = label;
            },
        });
        this.movCancel = document.getElementById('slideshow-move-cancel');
        this.movSubmit = document.getElementById('slideshow-move-submit');
        this.movCopy = document.getElementById('slideshow-move-copy');

        wireModal(this.movOverlay, {
            close: () => this.closeMove(),
            submit: () => this.doMove(),
            enterFrom: 'dialog',
            cancelBtn: this.movCancel,
            submitBtn: this.movSubmit,
        });
        this.movCopy.addEventListener('change', () => {
            this.movSubmit.textContent = this.movCopy.checked ? 'Copy' : 'Move';
        });
    },

    show(deleteItems, consolidateItems, tagItems, moveItems) {
        this.deleteItems = deleteItems || [];
        this.consolidateItems = consolidateItems || [];
        this.tagItems = tagItems || [];
        this.moveItems = moveItems || [];

        this.showNext();
    },

    showNext() {
        if (this.deleteItems.length > 0) {
            this.showDeleteDialog();
        } else if (this.moveItems.length > 0) {
            this.showMoveDialog();
        } else if (this.consolidateItems.length > 0) {
            const items = this.consolidateItems;
            this.consolidateItems = [];
            if (this.consolidateOpen) {
                this.consolidateOpen(items, () => this.showNext());
            }
        } else if (this.tagItems.length > 0) {
            this.showTagDialog();
        } else {
            this.finish();
        }
    },

    // ── Capped file list ──

    renderCappedList(container, items) {
        container.innerHTML = '';
        const max = 5;
        const shown = items.slice(0, max);
        for (const item of shown) {
            const div = document.createElement('div');
            div.textContent = item.name;
            container.appendChild(div);
        }
        if (items.length > max) {
            const more = document.createElement('div');
            more.textContent = `...and ${items.length - max} more`;
            more.style.opacity = '0.5';
            container.appendChild(more);
        }
    },

    // ── Delete dialog ──

    showDeleteDialog() {
        const n = this.deleteItems.length;
        this.delText.textContent = `Delete ${n} file${n !== 1 ? 's' : ''}? Files will be removed from disk and the catalog.`;
        this.renderCappedList(this.delList, this.deleteItems);
        this.delDupsCheck.checked = true;
        this.delSubmit.textContent = 'Delete';
        this.delSubmit.disabled = false;
        this.delOverlay.classList.remove('hidden');
    },

    closeDelete() {
        this.delOverlay.classList.add('hidden');
        this.deleteItems = [];
        this.showNext();
    },

    doDelete() {
        const allDups = this.delDupsCheck.checked;
        const fileIds = this.deleteItems.map(item => item.id);
        const n = fileIds.length;

        // Fire-and-forget — WS batch_deleted handles UI refresh
        API.post('/api/batch/delete', { file_ids: fileIds, all_duplicates: allDups });
        Toast.info(`Deleting ${n} file${n !== 1 ? 's' : ''}...`);

        this.delOverlay.classList.add('hidden');
        this.deleteItems = [];
        this.showNext();
    },

    // ── Tag dialog ──

    showTagDialog() {
        const n = this.tagItems.length;
        this.tagText.textContent = `Tag ${n} file${n !== 1 ? 's' : ''}.`;
        this.renderCappedList(this.tagList, this.tagItems);
        this.tagInput.value = '';
        this.tagSubmit.textContent = 'Tag';
        this.tagSubmit.disabled = false;
        this.tagOverlay.classList.remove('hidden');
        this.tagInput.focus();
    },

    closeTag() {
        this.tagOverlay.classList.add('hidden');
        this.tagItems = [];
        this.showNext();
    },

    doTag() {
        const tags = this.tagInput.value.split(',').map(t => t.trim()).filter(Boolean);
        if (tags.length === 0) return;
        const fileIds = this.tagItems.map(item => item.id);
        const n = fileIds.length;
        const label = tags.length === 1 ? `"${tags[0]}"` : `${tags.length} tags`;

        API.post('/api/batch/tag', { file_ids: fileIds, add_tags: tags });
        Toast.info(`Tagging ${n} file${n !== 1 ? 's' : ''} with ${label}`);

        this.tagOverlay.classList.add('hidden');
        this.tagItems = [];
        this.showNext();
    },

    finish() {
        this.deleteItems = [];
        this.consolidateItems = [];
        this.tagItems = [];
        this.moveItems = [];
    },

    // ── Move dialog ──

    async showMoveDialog() {
        const n = this.moveItems.length;
        this.movText.textContent = `Move or copy ${n} file${n !== 1 ? 's' : ''} to a new location.`;
        this.renderCappedList(this.movList, this.moveItems);

        this.movDest.textContent = 'No folder selected';
        this.movCopy.checked = false;
        this.movSubmit.textContent = 'Move';
        this.movSubmit.disabled = false;

        await this.picker.load();

        this.movOverlay.classList.remove('hidden');
    },

    closeMove() {
        this.movOverlay.classList.add('hidden');
        this.moveItems = [];
        this.showNext();
    },

    doMove() {
        if (!this.picker.selected) return;
        const fileIds = this.moveItems.map(item => item.id);
        const n = fileIds.length;
        const copy = this.movCopy.checked;
        const verb = copy ? 'Copying' : 'Moving';

        API.post('/api/batch/move', {
            file_ids: fileIds,
            destination_folder_id: this.picker.selected,
            copy: copy,
        });
        Toast.info(`${verb} ${n} file${n !== 1 ? 's' : ''}...`);

        this.movOverlay.classList.add('hidden');
        this.moveItems = [];
        this.showNext();
    },
};

export default SlideshowTriage;
