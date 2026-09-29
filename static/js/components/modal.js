/**
 * The standard dialog wiring: a click on the backdrop closes it, Escape
 * closes it while it's open, and Enter submits it.
 *
 * options:
 *   close()            - required
 *   submit()           - what Enter (and submitBtn) does
 *   enterFrom          - 'dialog': Enter anywhere while it's open submits;
 *                        or a list of inputs: Enter in one of them submits
 *   canClose()         - optional: Escape only closes when this is true
 *   cancelBtn, submitBtn - optional buttons wired to close() / submit()
 */
export function wireModal(overlay, options) {
    const { close, submit, enterFrom, canClose, cancelBtn, submitBtn } = options;

    if (cancelBtn) cancelBtn.addEventListener('click', () => close());
    if (submitBtn) submitBtn.addEventListener('click', () => submit());

    overlay.addEventListener('click', (e) => {
        if (e.target === overlay) close();
    });

    document.addEventListener('keydown', (e) => {
        if (overlay.classList.contains('hidden')) return;
        if (e.key === 'Escape') {
            if (!canClose || canClose()) close();
        } else if (e.key === 'Enter' && enterFrom === 'dialog') {
            e.preventDefault();
            submit();
        }
    });

    if (enterFrom && enterFrom !== 'dialog') {
        enterFrom.forEach(input => input.addEventListener('keydown', (e) => {
            if (e.key === 'Enter') submit();
        }));
    }
}


/** Show message in a dialog's error line; an empty message clears and hides it. */
export function setDialogError(el, message) {
    if (!el) return;
    el.textContent = message || '';
    el.classList.toggle('hidden', !message);
}
