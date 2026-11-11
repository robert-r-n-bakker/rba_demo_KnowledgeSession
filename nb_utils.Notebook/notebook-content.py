# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.12"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

import time

def get(path: str, retries: int = 5, **params) -> dict:
    for attempt in range(retries):
        r = s.get(f"{BASE}{path}",
                  params={"language": "nl-NL", **params},
                  timeout=30)
        if r.status_code == 429:
            wait = int(r.headers.get("Retry-After", 2 ** attempt))
            time.sleep(wait)
            continue
        r.raise_for_status()
        return r.json()
    raise RuntimeError(f"Gave up on {path} after {retries} tries")

def paginate(path: str, max_pages: int = 3, **params):
    page = 1
    while True:
        data = get(path, page=page, **params)
        yield from data["results"]
        if page >= min(data["total_pages"], max_pages):
            break
        page += 1

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
