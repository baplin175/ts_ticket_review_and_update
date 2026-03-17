import yaml
with open('web/dashboard.yaml') as f:
    cfg = yaml.safe_load(f)
pages = cfg['pages']
queries = cfg['queries']
print(f"Queries: {list(queries.keys())}")
for p in pages:
    kind = 'custom' if p.get('module') else 'yaml'
    tabs = len(p.get('tabs', []))
    comps = len(p.get('components', []))
    label = p['label']
    route = p['route']
    print(f"  {label:15s} {route:15s} [{kind}] tabs={tabs} components={comps}")
