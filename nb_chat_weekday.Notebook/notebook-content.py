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

# # Laugh-o-meter by weekday
# Builds `gold_laughs_by_weekday` (Amsterdam time) from `silver_chat_messages`.
# Requires `silver_chat_messages` and `gold_laughs_by_hour` (run `02_agent_fix_loop` first).

# CELL ********************

from pyspark.sql import functions as F
spark.conf.set("spark.sql.session.timeZone", "UTC")   # date_format below relies on a UTC session
LAUGHS = ["😆", "😄", "🤣"]
silver = spark.table("silver_chat_messages")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 1. Laugh reactions per message
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

# 2. Gold: the Laugh-o-meter by weekday (Amsterdam time); weekday_num follows Spark dayofweek (1 = Sunday)
gold = (silver
    .join(laughs, "message_id", "left")
    .withColumn("created_ts_local", F.from_utc_timestamp("created_ts", "Europe/Amsterdam"))
    .withColumn("weekday_num", F.dayofweek("created_ts_local"))
    .withColumn("weekday_name", F.date_format("created_ts_local", "EEEE"))
    .groupBy("weekday_num", "weekday_name")
    .agg(F.count("*").alias("messages"),
         F.coalesce(F.sum("laugh_reactions"), F.lit(0)).alias("laugh_reactions"))
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

# 3. VALIDATION - definition of done. Do not edit this cell.
g = spark.table("gold_laughs_by_weekday")
h = spark.table("gold_laughs_by_hour")
total_msgs   = g.agg(F.sum("messages")).first()[0]
total_laughs = g.agg(F.sum("laugh_reactions")).first()[0]

assert g.count() == 7, f"expected 7 weekdays, got {g.count()}"
assert g.where(F.col("weekday_num").isNull() | F.col("weekday_name").isNull()).count() == 0, "null weekday keys"
assert g.where(F.col("laughs_per_message") < 0).count() == 0, "negative laughs_per_message"
assert total_msgs == 3569,   f"expected 3569 messages, got {total_msgs}"
assert total_laughs == 4245, f"expected 4245 laugh reactions, got {total_laughs}"
assert total_msgs == h.agg(F.sum("messages")).first()[0], "message total differs from gold_laughs_by_hour"
assert total_laughs == h.agg(F.sum("laugh_reactions")).first()[0], "laugh total differs from gold_laughs_by_hour"

expected = {1: (45, 63), 2: (596, 681), 3: (694, 859), 4: (682, 925), 5: (728, 840), 6: (725, 764), 7: (99, 113)}
actual = {r["weekday_num"]: (r["messages"], r["laugh_reactions"]) for r in g.collect()}
assert actual == expected, f"per-weekday mismatch: {actual}"
print("✅ ALL CHECKS PASSED - the weekday Laugh-o-meter is correct")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
