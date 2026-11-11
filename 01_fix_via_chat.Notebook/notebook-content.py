# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # Step 1 - Fix code via chat
# The load cell works. The aggregation cell has one bug.
# Run it, then click **Fix with Copilot** (or type `/fix` in the Copilot pane). Review the suggestion and apply it yourself.

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

df = spark.read.option("multiLine", True).json(SRC)
# Use display() for rich tabular preview in Fabric notebooks
display(df.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Messages per author alias - top 10
(df.select("author_alias", "message_id")
   .grouBy("author_alias")
   .count()
   .orderBy(F.desc("count"))
   .limit(10)
   .show())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
