from unittest.mock import patch

import web.dashboard_templates as dashboard_templates


def test_list_templates_returns_expected_keys():
    templates = dashboard_templates.list_templates()
    keys = {item["key"] for item in templates}
    assert "executive_summary" in keys
    assert "root_cause_watch" in keys


def test_apply_template_creates_sections_and_widgets():
    with patch.object(dashboard_templates.data, "get_dashboard_tree", return_value={"sections": []}), \
         patch.object(dashboard_templates.data, "create_dashboard_section", side_effect=[{"id": 10}, {"id": 20}]) as create_section, \
         patch.object(dashboard_templates.data, "create_dashboard_widget") as create_widget:
        dashboard_templates.apply_template(123, "root_cause_watch")

    assert create_section.call_count == 2
    assert create_widget.call_count >= 2
