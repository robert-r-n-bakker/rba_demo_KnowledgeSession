---
name: laugh-o-meter-notebook-fixes
description: "Use when fixing the Laugh-o-meter Spark/Fabric notebook: JSON export load returns only _corrupt_record or UNRESOLVED_COLUMN message_id, FIELD_NOT_FOUND reactionType on the reactions struct, laugh reactions per message, or the peak hour assertion failing (expected 9 Amsterdam, got 8) because hour_local is UTC instead of Europe/Amsterdam."
---

# Laugh-o-meter notebook fixes

Known fixes for the three failures in the Laugh-o-meter notebook (bronze → silver → gold, writing `gold_laughs_by_hour`). Apply them to the matching cells.

## 1. Parse the JSON export correctly (Cell 1)

**Symptom:** the first read yields only `_corrupt_record`, so `message_id` doesn't exist:
`UNRESOLVED_COLUMN ... Did you mean [_corrupt_record]`.

**Cause:** the export is a pretty-printed (multi-line) JSON file.

**Fix:** read with `multiLine=True`.

```python
# 1. Load the raw export
# The export is a pretty-printed JSON file; use multiLine=True so Spark parses it correctly
raw = spark.read.option("multiLine", True).json(SRC)
raw.select("message_id", "created_utc", "author_alias", "reactions").show(3, truncate=False)
```

**Expected:** `raw` has columns `message_id`, `created_utc`, `author_alias`, `reactions`.

## 2. Use the correct reaction struct fields (Cell 3)

**Symptom:** `[FIELD_NOT_FOUND] No such struct field reactionType in count, type`.

**Cause:** each element of `reactions` is a struct `{count, type}`; there is no `reactionType`.

**Fix:** filter on `r.type` and sum `r.count`.

```python
# 3. Laugh reactions per message
LAUGHS = ["😆", "😄", "🤣"]

# Schema from sample: reactions is an array of structs with fields `count` and `type`
# So filter on `type` and sum `count`
laughs = (silver
    .select("message_id", F.explode_outer("reactions").alias("r"))
    .where(F.col("r.type").isin(LAUGHS))
    .groupBy("message_id")
    .agg(F.sum("r.count").alias("laugh_reactions")))
```

**Expected:** `laughs` computes with per-message laugh counts.

## 3. Compute the hour in Amsterdam local time (Cell 4)

**Symptom:** `AssertionError: expected peak hour 9 (Amsterdam), got 8`.

**Cause:** `hour_local` was extracted directly from the UTC timestamp.

**Fix:** convert with `from_utc_timestamp(..., "Europe/Amsterdam")` first, then take the hour.

```python
# 4. Gold: the Laugh-o-meter by hour of day (Amsterdam time)
# Convert UTC timestamps to Europe/Amsterdam, then take the local hour
gold = (silver
    .join(laughs, "message_id", "left")
    .withColumn("created_ts_local", F.from_utc_timestamp("created_ts", "Europe/Amsterdam"))
    .withColumn("hour_local", F.hour("created_ts_local"))
    .groupBy("hour_local")
    .agg(F.count("*").alias("messages"),
         F.coalesce(F.sum("laugh_reactions"), F.lit(0)).alias("laugh_reactions"))
    .withColumn("laughs_per_message", F.round(F.col("laugh_reactions") / F.col("messages"), 2))
    .orderBy("hour_local"))

gold.write.mode("overwrite").format("delta").saveAsTable("gold_laughs_by_hour")
display(gold)
```

**Expected:** `gold_laughs_by_hour` reflects Amsterdam local time and `peak_hour` is 9.
