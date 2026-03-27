var dagcomponentfuncs = (window.dashAgGridComponentFunctions = window.dashAgGridComponentFunctions || {});

dagcomponentfuncs.DOLink = function (props) {
    if (!props.value) return null;
    return React.createElement('a', {
        href: 'https://dev.azure.com/inHanceUtilities/Impresa/_workitems/edit/' + props.value + '/',
        target: '_blank',
        style: {color: '#228be6', textDecoration: 'none', fontWeight: '500'},
        onClick: function (e) { e.stopPropagation(); }
    }, props.value);
};

// Clientside callbacks for Dash
window.dash_clientside = window.dash_clientside || {};
window.dash_clientside.clientside = window.dash_clientside.clientside || {};

window.dash_clientside.clientside.openTeamsLink = function (n_clicks, url) {
    if (n_clicks && url) { window.location.href = url; }
    return window.dash_clientside.no_update;
};

window.dash_clientside.clientside.openEmailLink = function (n_clicks, url) {
    if (n_clicks && url) { window.location.href = url; }
    return window.dash_clientside.no_update;
};

// Chat textarea: Enter sends, Shift+Enter inserts a newline.
// dmc.Textarea applies className to a wrapper div, not the <textarea> itself,
// so we identify the element by tagName and containment in .chat-left-col.
document.addEventListener('keydown', function (e) {
    if (e.key !== 'Enter' || e.shiftKey) return;
    if (e.target.tagName !== 'TEXTAREA') return;
    // Ticket-level chat
    var col = e.target.closest('.chat-left-col');
    if (col) {
        e.preventDefault();
        var btn = col.querySelector('.chat-send-btn');
        if (btn) btn.click();
        return;
    }
    // Customer-level chat
    var custCol = e.target.closest('.customer-chat-left-col');
    if (custCol) {
        e.preventDefault();
        var custBtn = custCol.querySelector('.customer-chat-send-btn');
        if (custBtn) custBtn.click();
    }
}, true);
