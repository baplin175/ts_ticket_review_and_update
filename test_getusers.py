"""Quick probe: pull all users from TS API and dump raw JSON to output/users_raw.json."""

import json, os, requests, base64
from config import TS_BASE, TS_USER_ID, TS_KEY, OUTPUT_DIR

def probe(endpoint, out_file):
    url = f"{TS_BASE}/{endpoint}"
    auth = base64.b64encode(f"{TS_USER_ID}:{TS_KEY}".encode("ascii")).decode("ascii")
    headers = {"Authorization": f"Basic {auth}", "Accept": "application/json"}
    print(f"\n[probe] REQUEST: GET {url}")
    r = requests.get(url, headers=headers, timeout=60)
    print(f"[probe] Response status: {r.status_code}")
    if r.status_code != 200:
        print(f"[probe] Body preview: {r.text[:500]}")
        return
    data = r.json()
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    out = os.path.join(OUTPUT_DIR, out_file)
    envelope = {
        "request": {"method": "GET", "url": url},
        "response_status": r.status_code,
        "response_body": data,
    }
    with open(out, "w", encoding="utf-8") as f:
        json.dump(envelope, f, indent=2, ensure_ascii=False)
    # Peek at structure
    if isinstance(data, dict):
        print(f"[probe] Top-level keys: {list(data.keys())}")
        for k, v in data.items():
            if isinstance(v, list) and v:
                print(f"[probe] {k}[0] fields: {list(v[0].keys()) if isinstance(v[0], dict) else type(v[0])}")
                print(f"[probe] {k} count: {len(v)}")
            elif isinstance(v, dict):
                inner_keys = list(v.keys())
                print(f"[probe] {k} keys: {inner_keys[:10]}")
    print(f"[probe] Written to {out}")

def main():
    # 1) Users (agents/staff) — already confirmed working
    probe("Users", "users_raw.json")
    # 2) Customers (companies/orgs)
    probe("Customers", "customers_raw.json")
    # 3) Contacts (individual customer contacts)
    probe("Contacts", "contacts_raw.json")
    # Show first user's keys
    users = data
    if isinstance(data, dict):
        users = data.get("Users", data.get("User", data))
        if isinstance(users, dict):
            users = [users]
    if isinstance(users, list) and users:
        print(f"\n[probe] {len(users)} user(s) returned.")
        print(f"[probe] Fields on first user: {list(users[0].keys())}")
    print(f"[probe] Full response written to {out}")

if __name__ == "__main__":
    main()
