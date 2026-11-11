# Fabric notebook source


# MARKDOWN ********************

# # Step 1 - Fix code via chat
# The load cell works. The aggregation cell has one bug.
# Run it, then click **Fix with Copilot** (or type `/fix` in the Copilot pane). Review the suggestion and apply it yourself.

# CELL ********************

# Setup: attach lakehouse `lh_chat` and upload chat_messages.json to Files/chat/
from pyspark.sql import functions as F
spark.conf.set("spark.sql.session.timeZone", "UTC")   # make the demo deterministic
SRC = "Files/chat/chat_messages.json"

# CELL ********************

df = spark.read.option("multiLine", True).json(SRC)
display(df.limit(5))

# CELL ********************

# Messages per author alias - top 10
(df.select("author_alias", "message_id")
   .grouBy("author_alias")
   .count()
   .orderBy(F.desc("count"))
   .limit(10)
   .show())
