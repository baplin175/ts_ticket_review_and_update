"""
Fetch available LLMs from Matcha and write them to available_models.txt.
"""

import json
import requests

from config import MATCHA_API_KEY

LLMS_URL = "https://matcha.harriscomputer.com/rest/api/v1/llms"


def get_models():
    headers = {
        "Content-Type": "application/json",
        "MATCHA-API-KEY": MATCHA_API_KEY,
    }
    params = {
        "select": "id,name,allowed_file_input_mime_types",
    }

    print(f"GET {LLMS_URL}?select=id,name,allowed_file_input_mime_types")
    resp = requests.get(LLMS_URL, headers=headers, params=params, timeout=60)
    resp.raise_for_status()

    data = resp.json()
    formatted = json.dumps(data, indent=2)

    with open("available_models.txt", "w") as f:
        f.write(formatted)

    print(f"Status: {resp.status_code}")
    print(f"Written to available_models.txt")
    print(formatted)


if __name__ == "__main__":
    get_models()
