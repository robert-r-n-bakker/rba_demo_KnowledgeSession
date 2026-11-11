# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "b42cafac-e068-467c-9ffe-929335a09016",
# META       "default_lakehouse_name": "bronze",
# META       "default_lakehouse_workspace_id": "31dc5d10-f99a-484d-a82e-121e5cdb013f",
# META       "known_lakehouses": [
# META         {
# META           "id": "5c75dcaf-174c-4dab-ad65-04d465263b0d"
# META         },
# META         {
# META           "id": "b42cafac-e068-467c-9ffe-929335a09016"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql import functions as F

src = (
    "abfss://31dc5d10-f99a-484d-a82e-121e5cdb013f@onelake.dfs.fabric.microsoft.com/5c75dcaf-174c-4dab-ad65-04d465263b0d/Files/"
    "tmdb/movie_details/*/*/*/*.jsonl"
)

details = (
    spark.read.json(src)
    .withColumn("_source_file", F.col("_metadata.file_path"))
    .withColumn("_ingested_at", F.current_timestamp())
)

details.write.mode("append").saveAsTable("bronze.tmdb.movie_details")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

src = (
    "abfss://31dc5d10-f99a-484d-a82e-121e5cdb013f@onelake.dfs.fabric.microsoft.com/5c75dcaf-174c-4dab-ad65-04d465263b0d/Files/"
    "tmdb/discover_*/*/*/*/*.json"
)

discover_movies = (
    spark.read.option("multiline", "true")
    .json(src)
    .select(F.explode("results").alias('discover'))
    .select("discover.*")
    .withColumn("_source_file", F.col("_metadata.file_path"))
    .withColumn("_ingested_at", F.current_timestamp())
)

discover_movies.write.mode("append").saveAsTable("bronze.tmdb.discover_movies")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
