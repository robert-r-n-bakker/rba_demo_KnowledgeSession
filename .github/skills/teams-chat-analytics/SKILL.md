---
name: teams-chat-analytics
description: Build, fix or extend Fabric PySpark notebooks that analyse a Teams chat export (chat_messages.json in lakehouse lh_chat). Use for silver_chat_messages, laugh/reaction metrics, gold_laughs_by_* tables, local-time (Europe/Amsterdam) aggregations, and for running a notebook end-to-end until its VALIDATION cell passes.
---

# Teams chat analytics (Fabric / PySpark)

## When to use
- Loading the Teams chat export `Files/chat/chat_messages.json` from lakehouse `lh_chat`.
- Building or repairing `silver_chat_messages` or any `gold_laughs_by_*` table (hour, weekday, author, ...).
- Counting reactions (laughs or others) per message, or aggregating by local time.
- Running a chat notebook end-to-end and fixing errors until the final VALIDATION cell passes.

## Data contract
- Source: one JSON **array** (multi-line), not JSON Lines.
- Message fields: `message_id`, `created_utc` (ISO string, UTC), `author_alias`, `text`, `has_image`, `has_link`, `is_reply`, `reactions`.
- `reactions`: `array<struct<count: long, type: string>>`. `type` holds the emoji, so there is no `reactionType` field.
- Laugh emojis: `["😆", "😄", "🤣"]`.

## Steps
1. **Setup.** Attach `lh_chat` as the default lakehouse and set `spark.conf.set("spark.sql.session.timeZone", "UTC")`.
2. **Load raw.** `spark.read.option("multiLine", True).json("Files/chat/chat_messages.json")`. Check that `message_id` is a real column and not `_corrupt_record`.
3. **Silver.** Add `created_ts = F.to_timestamp("created_utc")`, select the message fields, and save as `silver_chat_messages` (Delta, overwrite).
4. **Reactions per message.** Run `explode_outer("reactions").alias("r")`, filter on `r.type`, group by `message_id`, then `sum("r.count")`.
5. **Gold.** Join silver to the reaction counts with a **left** join. Convert to local time with `F.from_utc_timestamp("created_ts", "Europe/Amsterdam")` and derive `hour`/`dayofweek` from the local column. Aggregate `messages = count(*)` and `laugh_reactions = coalesce(sum(...), 0)`, then `laughs_per_message = round(laugh_reactions / messages, 2)`. Save as Delta (overwrite).
6. **Validate.** Run the VALIDATION cell and iterate on steps 2–5 until it passes.
7. **Clean up.** Remove any temporary inspection cells, then summarise each fix: symptom, root cause and change.

## Known pitfalls and fixes
| Symptom | Root cause | Fix |
|---|---|---|
| Only `_corrupt_record` column; `message_id` cannot be resolved | File is a multi-line JSON array; Spark defaults to JSON Lines | `.option("multiLine", True)` (fallback: `spark.read.json(spark.sparkContext.wholeTextFiles(SRC).values())`) |
| `No such struct field reactionType in count, type` | Wrong struct field name | Use `r.type` for the emoji and `r.count` for the number |
| `expected peak hour 9 (Amsterdam), got 8` | Hour taken from UTC `created_ts` | Use `F.hour(F.from_utc_timestamp("created_ts", "Europe/Amsterdam"))` |
| Message total too low | `explode` or an inner join drops messages without reactions | Use `explode_outer` with a left join, and `coalesce(..., 0)` |
| Results change between sessions | Session time zone not fixed | Set `spark.sql.session.timeZone = UTC` in setup |

When an error names a field or column, run `printSchema()` before you change any code. Don't guess field names.

## Validation checks (reference dataset)
On `gold_laughs_by_hour`:
- `sum(messages) == 3569`
- `sum(laugh_reactions) == 4245`
- Hour with the most messages `== 9` (Amsterdam)

For new gold tables, add a final assert cell that checks:
- The message total is 3569 and the laugh total is 4245, matching the hour table.
- There are no null group keys.
- `laughs_per_message` is at least 0.

## Guardrails
- **Never edit the VALIDATION cell** or its expected values. Fix the pipeline instead.
- Use the IANA zone `Europe/Amsterdam`, never a fixed `+1`/`+2` offset, so DST is handled.
- Keep table names and columns stable: `silver_chat_messages`, `gold_laughs_by_hour(hour_local, messages, laugh_reactions, laughs_per_message)`.
- Write with `mode("overwrite")` and Delta format only. Don't drop or rename other lakehouse tables.
- Make one fix at a time, re-run from the failing cell, and don't skip cells.
- Debug or inspection cells are temporary. Remove them before you finish.
- Only report success after the VALIDATION cell has printed `ALL CHECKS PASSED` in an actual run.
