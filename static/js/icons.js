/* Inline SVG icons — render everywhere without emoji font support.
   Each icon uses currentColor so it inherits the active theme colour.
   Icons that need internal detail use fill-rule="evenodd" to punch
   out shapes from the filled silhouette. */

const svg = (d) =>
    `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="currentColor">${d}</svg>`;

const icons = {
    /* Hard-drive — location icon (replaces 💾)
       Rectangle with punched-out separator line and indicator dot. */
    location: svg(
        '<path fill-rule="evenodd" d="M2 4a2 2 0 0 1 2-2h16a2 2 0 0 1 2 2v16a2 2 0 0 1-2 2H4a2 2 0 0 1-2-2V4zm1 10h18v1H3v-1zm13.5 2a1.5 1.5 0 1 0 0 3 1.5 1.5 0 0 0 0-3z"/>'
    ),

    /* Folder (replaces 📁) */
    folder: svg(
        '<path d="M10 4H4a2 2 0 0 0-2 2v12a2 2 0 0 0 2 2h16a2 2 0 0 0 2-2V8a2 2 0 0 0-2-2h-8l-2-2z"/>'
    ),

    /* Generic file / blank page (replaces 📄) */
    file: svg(
        '<path d="M6 2a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8l-6-6H6z"/>'
    ),

    /* Document with text lines (replaces 📃) */
    document: svg(
        '<path fill-rule="evenodd" d="M6 2a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8l-6-6H6zm2 11h8v1.5H8V13zm0 4h5v1.5H8V17z"/>'
    ),

    /* Image / picture (replaces 🖼)
       Frame with punched-out sun circle. */
    image: svg(
        '<path fill-rule="evenodd" d="M3 5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2v14a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V5zm14 3.5a1.5 1.5 0 1 1 0-3 1.5 1.5 0 0 1 0 3z"/>'
    ),

    /* Video / film (replaces 🎬)
       Rectangle with punched-out play triangle. */
    video: svg(
        '<path fill-rule="evenodd" d="M3 5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2v14a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V5zm7 3v8l7-4-7-4z"/>'
    ),

    /* Music note (replaces 🎵) */
    audio: svg(
        '<circle cx="7" cy="17" r="4"/>' +
        '<path d="M11 17V3l10 4v3l-8-3v10z"/>'
    ),

    /* Heart — favourite indicator (filled) */
    heart: svg(
        '<path d="M12 21.35l-1.45-1.32C5.4 15.36 2 12.28 2 8.5 2 5.42 4.42 3 7.5 3c1.74 0 3.41.81 4.5 2.09C13.09 3.81 14.76 3 16.5 3 19.58 3 22 5.42 22 8.5c0 3.78-3.4 6.86-8.55 11.54L12 21.35z"/>'
    ),

    /* Heart outline — not-favourite indicator */
    heartOutline: svg(
        '<path d="M16.5 3c-1.74 0-3.41.81-4.5 2.09C10.91 3.81 9.24 3 7.5 3 4.42 3 2 5.42 2 8.5c0 3.78 3.4 6.86 8.55 11.54L12 21.35l1.45-1.32C18.6 15.36 22 12.28 22 8.5 22 5.42 19.58 3 16.5 3zm-4.4 15.55l-.1.1-.1-.1C7.14 14.24 4 11.39 4 8.5 4 6.5 5.5 5 7.5 5c1.54 0 3.04.99 3.57 2.36h1.87C13.46 5.99 14.96 5 16.5 5c2 0 3.5 1.5 3.5 3.5 0 2.89-3.14 5.74-7.9 10.05z"/>'
    ),

    /* Gear / settings icon — Heroicons cog-6-tooth solid (MIT) */
    settings: svg(
        '<path fill-rule="evenodd" d="M11.078 2.25c-.917 0-1.699.663-1.85 1.567L9.05 4.889c-.02.12-.115.26-.297.348a7.493 7.493 0 0 0-.986.57c-.166.115-.334.126-.45.083L6.3 5.508a1.875 1.875 0 0 0-2.282.819l-.922 1.597a1.875 1.875 0 0 0 .432 2.385l.84.692c.095.078.17.229.154.43a7.598 7.598 0 0 0 0 1.139c.015.2-.059.352-.153.43l-.841.692a1.875 1.875 0 0 0-.432 2.385l.922 1.597a1.875 1.875 0 0 0 2.282.818l1.019-.382c.115-.043.283-.031.45.082.312.214.641.405.985.57.182.088.277.228.297.35l.178 1.071c.151.904.933 1.567 1.85 1.567h1.844c.916 0 1.699-.663 1.85-1.567l.178-1.072c.02-.12.114-.26.297-.349.344-.165.673-.356.985-.57.167-.114.335-.125.45-.082l1.02.382a1.875 1.875 0 0 0 2.28-.819l.923-1.597a1.875 1.875 0 0 0-.432-2.385l-.84-.692c-.095-.078-.17-.229-.154-.43a7.614 7.614 0 0 0 0-1.139c-.016-.2.059-.352.153-.43l.84-.692c.708-.582.891-1.59.433-2.385l-.922-1.597a1.875 1.875 0 0 0-2.282-.818l-1.02.382c-.114.043-.282.031-.449-.083a7.49 7.49 0 0 0-.985-.57c-.183-.087-.277-.227-.297-.348l-.179-1.072a1.875 1.875 0 0 0-1.85-1.567h-1.843zM12 15.75a3.75 3.75 0 1 0 0-7.5 3.75 3.75 0 0 0 0 7.5z" clip-rule="evenodd"/>'
    ),
};

export default icons;
