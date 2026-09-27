/**
 * Copy text to the clipboard.
 *
 * navigator.clipboard only exists over HTTPS or on localhost, so plain-HTTP
 * installs fall back to a hidden textarea and execCommand('copy').
 * With btn, shows "Copied" on the button for two seconds.
 */
export async function copyText(text, btn) {
    let copied = false;
    if (navigator.clipboard && navigator.clipboard.writeText) {
        try {
            await navigator.clipboard.writeText(text);
            copied = true;
        } catch {
            // fall through to the textarea method
        }
    }
    if (!copied) {
        const ta = document.createElement('textarea');
        ta.value = text;
        ta.style.position = 'fixed';
        ta.style.opacity = '0';
        document.body.appendChild(ta);
        ta.select();
        document.execCommand('copy');
        document.body.removeChild(ta);
    }
    if (btn) {
        if (!btn.dataset.label) btn.dataset.label = btn.textContent;
        btn.textContent = 'Copied';
        clearTimeout(btn._copiedTimer);
        btn._copiedTimer = setTimeout(() => { btn.textContent = btn.dataset.label; }, 2000);
    }
}
