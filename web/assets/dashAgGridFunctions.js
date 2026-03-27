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
