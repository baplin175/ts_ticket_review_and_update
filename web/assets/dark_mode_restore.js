/*  Restore dark-mode state from Dash's localStorage store on page load.
 *  Dash dcc.Store(storage_type="local") serialises under the key
 *  "dark-mode-store" as JSON.  We read it here synchronously so the
 *  body class is applied before first paint, avoiding a flash.        */
(function () {
    try {
        var raw = window.localStorage.getItem("dark-mode-store");
        if (raw) {
            var val = JSON.parse(raw);
            if (val === true) {
                document.body.classList.add("dark-mode");
            }
        }
    } catch (_) { /* ignore parse errors */ }

    /* Once Dash has rendered, sync the toggle switch to match */
    var observer = new MutationObserver(function () {
        var toggle = document.getElementById("dark-mode-toggle");
        if (toggle) {
            observer.disconnect();
            try {
                var raw = window.localStorage.getItem("dark-mode-store");
                if (raw && JSON.parse(raw) === true) {
                    /* Dash Mantine Switch: click it if not already checked */
                    var input = toggle.querySelector("input[type='checkbox']");
                    if (input && !input.checked) {
                        input.click();
                    }
                }
            } catch (_) {}
        }
    });
    observer.observe(document.body, { childList: true, subtree: true });
})();
