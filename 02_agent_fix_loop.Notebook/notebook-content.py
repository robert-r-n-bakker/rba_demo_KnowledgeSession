# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "8b0c4602-44f5-4558-a916-69a155f01b4c",
# META       "default_lakehouse_name": "lh_chat",
# META       "default_lakehouse_workspace_id": "41a0bda8-1019-448a-b360-11615d7ca374",
# META       "known_lakehouses": [
# META         {
# META           "id": "8b0c4602-44f5-4558-a916-69a155f01b4c"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Step 2 - Agent fixes it, runs it, checks it
# This notebook has **three seeded bugs** that only show up one at a time, plus a validation cell at the end.
# 
# Prompt for the Copilot chat pane:
# > Make this notebook run end-to-end. Run each cell, fix any error, and keep going until the final VALIDATION cell passes. Do not change the validation cell. Summarize every fix you made.
# 
# Choose *Allow and automatically approve* so the audience sees the loop.

# CELL ********************

# Setup: attach lakehouse `lh_chat` and upload chat_messages.json to Files/chat/
from pyspark.sql import functions as F
spark.conf.set("spark.sql.session.timeZone", "UTC")   # make the demo deterministic
SRC = "Files/chat/chat_messages.json"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 1. Load the raw export
# The export is a pretty-printed JSON file; use multiLine=True so Spark parses it correctly
raw = spark.read.option("multiLine", True).json(SRC)
raw.select("message_id", "created_utc", "author_alias", "reactions").show(3, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 2. Silver: one row per message, with a timestamp
silver = (raw
    .withColumn("created_ts", F.to_timestamp("created_utc"))
    .select("message_id", "created_ts", "author_alias", "text",
            "has_image", "has_link", "is_reply", "reactions"))
silver.write.mode("overwrite").format("delta").saveAsTable("silver_chat_messages")
print(silver.count(), "messages")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 3. Laugh reactions per message
LAUGHS = ["😆", "😄", "🤣"]
# Schema from sample: reactions is an array of structs with fields `count` and `type`
# So filter on `type` and sum `count`
laughs = (silver
    .select("message_id", F.explode_outer("reactions").alias("r"))
    .where(F.col("r.type").isin(LAUGHS))
    .groupBy("message_id")
    .agg(F.sum("r.count").alias("laugh_reactions")))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 5. VALIDATION - definition of done. Do not edit this cell.
g = spark.table("gold_laughs_by_hour")
total_msgs   = g.agg(F.sum("messages")).first()[0]
total_laughs = g.agg(F.sum("laugh_reactions")).first()[0]
peak_hour    = g.orderBy(F.desc("messages")).first()["hour_local"]

assert total_msgs == 3569,  f"expected 3569 messages, got {total_msgs}"
assert total_laughs == 4245, f"expected 4245 laugh reactions, got {total_laughs}"
assert peak_hour == 9,       f"expected peak hour 9 (Amsterdam), got {peak_hour}"
print("✅ ALL CHECKS PASSED - the Laugh-o-meter is correct")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
