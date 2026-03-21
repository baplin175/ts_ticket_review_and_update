"""Helpers for combining static pages and DB-backed dashboards."""

from . import data


DASHBOARD_ROUTE_PREFIX = "/dashboards/"


def build_static_nav_items(pages):
    return [
        {
            "kind": "static",
            "label": page["label"],
            "icon": page.get("icon", "tabler:point"),
            "href": page["route"],
            "aliases": page.get("aliases", []),
            "match_prefix": page.get("match_prefix"),
        }
        for page in pages
    ]


def list_dashboard_nav_items():
    items = []
    for dashboard in data.list_dashboards():
        items.append(
            {
                "kind": "dashboard",
                "label": dashboard["name"],
                "icon": dashboard.get("icon") or "tabler:layout-dashboard",
                "href": f"{DASHBOARD_ROUTE_PREFIX}{dashboard['slug']}",
                "slug": dashboard["slug"],
            }
        )
    return items


def build_nav_items(pages):
    return build_static_nav_items(pages) + list_dashboard_nav_items()


def nav_item_active(item, pathname):
    if item["kind"] == "dashboard":
        return pathname == item["href"]

    if pathname == item["href"]:
        return True
    if pathname in item.get("aliases", []):
        return True
    if item.get("match_prefix") and pathname:
        return pathname.startswith(item["match_prefix"])
    return False


def get_runtime_dashboard_definition(slug):
    dashboard = data.get_dashboard_by_slug(slug)
    if not dashboard:
        return None

    tree = data.get_dashboard_tree(dashboard["id"])
    if not tree:
        return None
    return dashboard_tree_to_definition(tree)


def dashboard_tree_to_definition(tree):
    sections = []
    for section in tree.get("sections", []):
        widgets = []
        for widget in section.get("widgets", []):
            widgets.append(
                {
                    "id": f"widget-{widget['id']}",
                    "type": widget["widget_type"],
                    "title": widget.get("title"),
                    "query": (
                        {
                            "key": widget["query_key"],
                            "params": widget.get("query_params_json") or {},
                        }
                        if widget.get("query_key")
                        else None
                    ),
                    **(widget.get("display_config_json") or {}),
                }
            )
        sections.append(
            {
                "id": f"section-{section['id']}",
                "title": section.get("title"),
                "description": section.get("description"),
                "layout": {"columns": section.get("layout_columns", 1)},
                "widgets": widgets,
            }
        )

    return {
        "id": tree["id"],
        "title": tree["name"],
        "description": tree.get("description"),
        "icon": tree.get("icon"),
        "route": f"{DASHBOARD_ROUTE_PREFIX}{tree['slug']}",
        "sections": sections,
    }
