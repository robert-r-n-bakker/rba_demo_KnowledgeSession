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

token = "eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiI4ZmRhODU0OTAxODhkODdkZmU1YmFjZDAzNGY1ZTk4YyIsIm5iZiI6MTc5MDY3ODQ4My42ODYsInN1YiI6IjZhYmI5NWQzMzM0ZDUwNjM0MTJhY2QzNSIsInNjb3BlcyI6WyJhcGlfcmVhZCJdLCJ2ZXJzaW9uIjoxfQ.I-XUidPzEPGV5ymMv-4cJ1DVunA8dpggq7-pcHFJEbs"

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

def get(path: str, **params) -> dict:
    r = s.get(f"{BASE}{path}",
              params={"language": "nl-NL", **params}, timeout=30)
    r.raise_for_status()
    return r.json()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

genres = get("/genre/movie/list")["genres"]
for genre in genres:
    print(genre["name"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

page1 = get("/discover/movie", primary_release_year=2024,
            sort_by="popularity.desc")
print(list(page1.keys()))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

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

# CELL ********************

page_list = list(paginate("/discover/movie",max_pages=3, primary_release_year=2024))
len(page_list)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

import json
print(json.dumps(page_list[0], indent=2))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

import json
id = "1285366"
movie = get(f"/movie/{id}?append_to_response=credits,watch/providers")
print(json.dumps(movie, indent=2))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

sremaing_providers = movie["watch/providers"]["results"].get("NL", {}).get("flatrate", [])
sremaing_providers

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

page_list = list(paginate("/discover/movie",max_pages=3, primary_release_year=2024))
len(page_list)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# if the movie has no Dutch streaming information.
# 
# Using .get() makes the code safe and returns an empty list instead.
