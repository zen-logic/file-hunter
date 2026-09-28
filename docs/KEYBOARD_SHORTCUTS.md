# Keyboard shortcuts

File Hunter can be used almost entirely from the keyboard. This page lists
every shortcut, grouped by where it works.

On a Mac, use **Cmd** wherever this page says **Ctrl/Cmd**. On Windows and
Linux, use **Ctrl**.

## Panels and focus

The main window has three panels: the **tree** of locations and folders on
the left, the **file list** in the middle, and the **details** panel on the
right. Keys act on the panel that has focus, which is marked with a coloured
outline. Click anywhere in a panel to give it focus, or use Tab.

Shortcuts are switched off while you are typing in a text box (a filter,
search field, tag field and so on), so letters type normally. The exceptions
are Ctrl/Cmd+F and Escape, described below. Shortcuts are also switched off
while a dialog is open; the dialog has its own keys.

## Anywhere in the main window

| Key | What it does |
|-----|--------------|
| Tab | Move focus to the next panel: tree, file list, details, then back to the tree |
| Shift+Tab | Move focus to the previous panel |
| Ctrl/Cmd+F | Open or close the search panel. Works even while typing in a text box |
| / | Put the cursor in the filter box of the focused panel (tree or file list) |
| Escape | In a filter box: clear the filter and return focus to its panel. In a search field: close the search panel |
| N | Add a new location |
| S | With no files selected: open the Scan dialog for the location or folder selected in the tree. With files selected: select or deselect a file (see [Selecting with S](#selecting-with-s)) |
| Delete or Backspace | Delete the selected files and folders, the same as the Delete button in the details panel. This asks for confirmation first |

## Tree

| Key | What it does |
|-----|--------------|
| Up / Down | Move the highlight up or down the tree |
| Right | Expand the highlighted item. If it's already expanded, move to its first child |
| Left | Collapse the highlighted item. If it's already collapsed, move to its parent |
| Home / End | Move to the first or last item in the tree |
| Enter | Open the highlighted location or folder in the file list |

## File list and gallery

These keys work in both the list view and the gallery view. The file list
shows 120 files per page; moving past the first or last file on a page opens
the next or previous page automatically.

| Key | What it does |
|-----|--------------|
| Up / Down | Select the previous or next file |
| Left / Right | Gallery view only: same as Up / Down |
| Shift+Up / Shift+Down | Extend the selection to include the previous or next file |
| Home / End | Select the very first or very last file, across all pages |
| Page Up / Page Down | Go to the previous or next page and select its first file. On the first or last page, select its first or last file |
| Ctrl/Cmd+A | Select every file and folder in the list, across all pages |
| Enter | Open the selected folder |
| Space | Open the selected file in a large preview, if it can be previewed |
| S | Select or deselect the file you're on (see [Selecting with S](#selecting-with-s)) |
| Escape | Stop selecting with S. The selection is kept |
| D, C, T, M, Z | Mark files for an action (see [Triage marks](#triage-marks)) |

When the **details** panel has focus, Up, Down, Home, End, Page Up,
Page Down, Space and the triage keys still act on the file list, so you can
move through files while reading their details.

## Selecting with S

S lets you pick out files one at a time from the keyboard, without holding
down Ctrl/Cmd and clicking.

1. Move to a file with the arrow keys and press **S**. The file is selected,
   and File Hunter switches into selecting mode.
2. In selecting mode, the arrow keys move a highlight (it looks the same as
   when you hover the mouse over a file) without changing what's selected.
   Move to another file and press **S** to add it, or press **S** on a
   selected file to remove it.
3. Shift+Up / Shift+Down move the highlight and add the file it lands on.
4. Press **Escape**, or click anywhere in the list, to stop. Your selection
   is kept.

With more than one file selected, the details panel shows the selection and
the actions you can take on it. **Clear Selection**, at the bottom of that
panel, deselects everything.

If nothing is selected and you are not in selecting mode, S opens the Scan
dialog instead.

## Preview

The preview opens with Space from the file list, or with the enlarge button
on the preview in the details panel.

| Key | What it does |
|-----|--------------|
| Up / Down | Show the previous or next file in the list |
| Space | Close the preview |
| Escape | Close the preview. In full screen, Escape first leaves full screen |
| S | Select or deselect the file being shown. A green **Selected** badge appears in the top-right corner when it's selected |
| D, C, T, M, Z | Mark the file being shown for an action |

## Slideshow and playlist

A slideshow shows the images in a folder or search result, and a playlist
plays the videos. Start one from the Slideshow or Playlist button in the
details panel.

| Key | What it does |
|-----|--------------|
| Left / Right | Show the previous or next item. This also stops the slideshow advancing by itself |
| Space | Slideshow: start or stop advancing automatically. Playlist: play or pause the video |
| Escape | Close. In full screen, Escape first leaves full screen |
| S | Select or deselect the item being shown, even if it is on a different page of the file list |
| D, C, T, M, Z | Mark the item being shown for an action |

When you close a slideshow or playlist, the file list moves to the last item
you were viewing. If you selected files with S, the selection is kept.

## Triage marks

Triage marks let you flag files while you browse and act on them all later.
Each key adds the mark, or removes it if it's already there:

| Key | Mark |
|-----|------|
| D | Delete |
| C | Consolidate |
| T | Tag |
| M | Move |
| Z | Download as ZIP |

In the file list, the mark goes on every selected file, or on the current
file if only one is selected. Folders are skipped. Marks stay in place as you
move between folders and slideshows. The triage bar above the file list shows
how many files have each mark, with a button to carry out that action.

## Dialogs

| Key | What it does |
|-----|--------------|
| Escape | Close the dialog without doing anything |
| Enter | In a confirmation dialog (delete, move, consolidate, ignore, and the triage action dialogs): confirm. In a dialog with a name box (rename, new folder): save the name |

## Text boxes

| Key | What it does |
|-----|--------------|
| Enter | In a search field, the similarity search box or the document search box: run the search. In a tag box: add the tag. In the hex viewer's find box: find |
