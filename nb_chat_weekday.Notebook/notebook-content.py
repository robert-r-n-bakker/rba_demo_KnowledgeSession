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

# # nb_chat_weekday
# Builds `gold_laughs_by_weekday` (Amsterdam time) from `silver_chat_messages`.
# Requires the silver table to exist (created by `02_agent_fix_loop`).

# CELL ********************

from pyspark.sql import functions as F
spark.conf.set("spark.sql.session.timeZone", "UTC")   # make the demo deterministic

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 1. Silver input
silver = spark.table("silver_chat_messages")
print(silver.count(), "messages")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 2. Laugh reactions per message
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

# 3. Gold: the Laugh-o-meter by weekday (Amsterdam time)
# Convert UTC timestamps to Europe/Amsterdam first, then derive the local weekday.
# weekday_num is ISO: Monday = 1 ... Sunday = 7 (Spark's dayofweek is Sunday = 1)
gold = (silver
    .join(laughs, "message_id", "left")
    .withColumn("created_ts_local", F.from_utc_timestamp("created_ts", "Europe/Amsterdam"))
    .withColumn("weekday_num", ((F.dayofweek("created_ts_local") + 5) % 7 + 1).cast("int"))
    .withColumn("weekday_name", F.date_format("created_ts_local", "EEEE"))
    .groupBy("weekday_num", "weekday_name")
    .agg(F.count("*").alias("messages"),
         F.coalesce(F.sum("laugh_reactions"), F.lit(0)).cast("long").alias("laugh_reactions"))
    .withColumn("laughs_per_message", F.round(F.col("laugh_reactions") / F.col("messages"), 2))
    .orderBy("weekday_num"))

gold.write.mode("overwrite").format("delta").saveAsTable("gold_laughs_by_weekday")
display(gold)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 4. VALIDATION
g = spark.table("gold_laughs_by_weekday")
h = spark.table("gold_laughs_by_hour")   # built by 02_agent_fix_loop

total_msgs   = g.agg(F.sum("messages")).first()[0]
total_laughs = g.agg(F.sum("laugh_reactions")).first()[0]
rows         = g.orderBy("weekday_num").collect()

assert total_msgs == 3569,   f"expected 3569 messages, got {total_msgs}"
assert total_laughs == 4245, f"expected 4245 laugh reactions, got {total_laughs}"
assert [r["weekday_num"] for r in rows] == list(range(1, 8)), "expected one row per weekday 1..7"
assert [r["weekday_name"] for r in rows] == \
    ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"], "weekday names don't match numbers"
assert total_msgs == h.agg(F.sum("messages")).first()[0], "weekday and hour tables disagree on messages"
assert total_laughs == h.agg(F.sum("laugh_reactions")).first()[0], "weekday and hour tables disagree on laughs"
print("✅ ALL CHECKS PASSED - gold_laughs_by_weekday is correct")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
