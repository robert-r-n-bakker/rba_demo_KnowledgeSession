# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.12"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "5c75dcaf-174c-4dab-ad65-04d465263b0d",
# META       "default_lakehouse_name": "landing",
# META       "default_lakehouse_workspace_id": "31dc5d10-f99a-484d-a82e-121e5cdb013f",
# META       "known_lakehouses": [
# META         {
# META           "id": "5c75dcaf-174c-4dab-ad65-04d465263b0d"
# META         }
# META       ]
# META     }
# META   }
# META }

# PARAMETERS CELL ********************

run_date   = "2026-09-30"   # parameter cell
max_movies = 500
token = ""                  # the pipeline passes it
load_type = "full"
last_watermark = None

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

vl = notebookutils.variableLibrary.getLibrary("vl_platform")
max_movies = vl.max_movies
print(max_movies)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

%run nb_utils

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

import requests

BASE = "https://api.themoviedb.org/3"
s = requests.Session()
s.headers.update({"Authorization": f"Bearer {token}",
                  "accept": "application/json"})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ### Collect the distinct movie ids from today's discover_* files — a set comprehension.

# CELL ********************

import json
from pathlib import Path

ids = set()

folder = "/lakehouse/default/Files/tmdb/discover_2025/2026/09/30"

for file in Path(folder).glob("*.json"):
    with open(file, encoding="utf-8-sig") as f:
        data = json.load(f)

    ids.update(movie["id"] for movie in data["results"])

print(len(ids))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

if load_type == "incremental" and last_watermark:
    changed = {r["id"] for r in paginate(
        "/movie/changes",
        max_pages=50,
        start_date=last_watermark,
        end_date=run_date
    )}
    ids = ids & changed
# tracked movies that changed

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ### Fetch the details with 6 workers

# CELL ********************

from concurrent.futures import ThreadPoolExecutor, as_completed

APPEND = "credits,keywords,release_dates,watch/providers"

def fetch(movie_id: int) -> dict:
    return get(f"/movie/{movie_id}", append_to_response=APPEND)

results, failures = [], []

with ThreadPoolExecutor(max_workers=6) as pool:
    futs = {pool.submit(fetch, m): m for m in list(ids)[:max_movies]}

    for f in as_completed(futs):
        try:
            results.append(f.result())
        except Exception as e:
            failures.append({
                "id": futs[f],
                "error": str(e)
            })

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

print(f"Movies loaded : {len(results)}")
print(f"Movies failed : {len(failures)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ### Write the results as JSON Lines. Failures go to their own file 

# CELL ********************

import json
import os

path = run_date.replace("-", "/")

folder = f"/lakehouse/default/Files/tmdb/movie_details/{path}"

os.makedirs(folder, exist_ok=True)

with open(f"{folder}/details.jsonl", "w", encoding="utf-8") as fh:
    for r in results:
        fh.write(json.dumps(r) + "\n")

with open(f"{folder}/failures.jsonl", "w", encoding="utf-8") as fh:
    for r in failures:
        fh.write(json.dumps(r) + "\n")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

notebookutils.notebook.exit(
    json.dumps({"fetched": len(results), "failed": len(failures)}))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

if load_type == "incremental":
    changed = {r["id"] for r in paginate(
        "/movie/changes", max_pages=50,
        start_date=last_watermark, end_date=run_date
    )}
    ids = ids & changed
# tracked movies that changed

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
