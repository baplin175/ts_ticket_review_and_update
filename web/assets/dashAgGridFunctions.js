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
