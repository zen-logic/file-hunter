/** Display formatting shared by every component. */

/** Bytes as B/KB/MB/GB/TB/PB. Empty for a missing value, "0 B" for zero. */
export function formatSize(bytes) {
    if (bytes === null || bytes === undefined) return '';
    if (bytes < 1024) return bytes + ' B';
    if (bytes < 1048576) return (bytes / 1024).toFixed(1) + ' KB';
    if (bytes < 1073741824) return (bytes / 1048576).toFixed(1) + ' MB';
    if (bytes < 1099511627776) return (bytes / 1073741824).toFixed(1) + ' GB';
    if (bytes < 1125899906842624) return (bytes / 1099511627776).toFixed(1) + ' TB';
    return (bytes / 1125899906842624).toFixed(1) + ' PB';
}

function parseDate(isoStr) {
    if (!isoStr) return null;
    const d = new Date(isoStr);
    return isNaN(d) ? null : d;
}

/** Date only, in the browser's locale. Unparseable input is shown as-is. */
export function formatDate(isoStr) {
    const d = parseDate(isoStr);
    return d ? d.toLocaleDateString() : (isoStr || '');
}

/** Date and time, in the browser's locale. Unparseable input is shown as-is. */
export function formatDateTime(isoStr) {
    const d = parseDate(isoStr);
    return d ? d.toLocaleString() : (isoStr || '');
}

/** Escape text for insertion into HTML. */
/** Text made safe to put in HTML, in element content or a quoted attribute. */
export function esc(s) {
    if (s === null || s === undefined) return '';
    return String(s)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#39;');
}
