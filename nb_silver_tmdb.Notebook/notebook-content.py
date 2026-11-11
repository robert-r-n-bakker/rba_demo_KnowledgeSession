# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "dcf68879-71e4-4cad-b64b-057107420776",
# META       "default_lakehouse_name": "silver",
# META       "default_lakehouse_workspace_id": "31dc5d10-f99a-484d-a82e-121e5cdb013f",
# META       "known_lakehouses": [
# META         {
# META           "id": "b42cafac-e068-467c-9ffe-929335a09016"
# META         },
# META         {
# META           "id": "dcf68879-71e4-4cad-b64b-057107420776"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql import Window, functions as F

w = Window.partitionBy("id").orderBy(F.col("_ingested_at").desc())

latest = (
    spark.table("bronze.tmdb.movie_details")
    .withColumn("rn", F.row_number().over(w))
    .filter("rn = 1").drop("rn")
)

display(latest)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### bridge_movie_genre

# CELL ********************

movie_genre = (
    latest
    .select(
        F.col("id").alias("movie_id"),
        F.explode("genres").alias("g")
    )
    .select("movie_id", F.col("g.id").alias("genre_id")))

display(movie_genre)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

movie_genre.write.mode("overwrite").saveAsTable("silver.tmdb.bridge_movie_genre")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### dim_genre

# CELL ********************

dim_genre = (
    latest
    .select(F.explode(F.col("genres")).alias("genre"))
    .select(
        F.col("genre.id").alias("genre_id"),
        F.col("genre.name").alias("genre_name")
    )
    .distinct()
)

display(dim_genre)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_genre.write.mode("overwrite").saveAsTable("silver.tmdb.dim_genre")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### movie_provider_nl

# CELL ********************

movie_provider_nl = (
    latest
    .select(
        F.col("id").alias("movie_id"),
        F.explode_outer(F.col("`watch/providers`.results.NL.flatrate")).alias("p")
    )
    .select("movie_id", "p.*")
)
    
display(movie_provider_nl)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

movie_provider_nl.write.mode("overwrite").saveAsTable("silver.tmdb.movie_provider_nl")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### fact_movie_cast

# CELL ********************

fact_movie_cast = (
    latest
    .select(
        F.col("id").alias("movie_id"),
        F.explode("credits.cast").alias("cast")
    )
    .select(
        "movie_id",
        F.col("cast.id").alias("person_id"),
        F.col("cast.character").alias("character_name"),
        F.col("cast.order").alias("billing_order")
    )
)

display(fact_movie_cast)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact_movie_cast.write.mode("overwrite").saveAsTable("silver.tmdb.fact_movie_cast")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### dim_person

# CELL ********************

cast_people = (
    latest
    .select(F.explode("credits.cast").alias("person"))
    .select(
        F.col("person.id").alias("person_id"),
        F.col("person.name").alias("name"),
        F.col("person.known_for_department").alias("known_for_department")
    )
)

crew_people = (
    latest
    .select(F.explode("credits.crew").alias("person"))
    .select(
        F.col("person.id").alias("person_id"),
        F.col("person.name").alias("name"),
        F.col("person.known_for_department").alias("known_for_department")
    )
)

dim_person = (
    cast_people
    .unionByName(crew_people)
    .dropDuplicates(["person_id"])
)

display(dim_person)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_person.write.mode("overwrite").saveAsTable("silver.tmdb.dim_person")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### fact_movie_popularity_daily

# CELL ********************

fact_movie_popularity_daily = (
    spark.table("bronze.tmdb.discover_movies")
    .select(
        F.col("id").alias("movie_id"),
        F.col("release_date").alias("date"),
        F.col("popularity").alias("popularity_score")
    )
)
display(fact_movie_popularity_daily)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact_movie_popularity_daily.write.mode("overwrite").saveAsTable("silver.tmdb.fact_movie_popularity_daily")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### dim_movie

# CELL ********************

from pyspark.sql.types import StructType, ArrayType, MapType

dim_movie = (
    latest
    .withColumnRenamed("id", "movie_id")
    .withColumn("budget", F.when(F.col("budget") > 0, F.col("budget")))
    .withColumn("revenue", F.when(F.col("revenue") > 0, F.col("revenue")))
    .withColumn("valid_from", F.current_timestamp())
    .withColumn("valid_to", F.lit(None).cast("timestamp"))
    .withColumn("is_current", F.lit(True).cast("boolean"))
)

complex_cols = [
    f.name
    for f in dim_movie.schema.fields
    if isinstance(f.dataType, (StructType, ArrayType, MapType))
]

dim_movie = dim_movie.drop(*complex_cols)

display(dim_movie)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_movie.write.mode("overwrite").saveAsTable("silver.tmdb.stg_movie")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from delta.tables import DeltaTable
target = "silver.tmdb.dim_movie"

if not spark.catalog.tableExists(target):
    dim_movie.write.saveAsTable(target) # first run only
else:
    (DeltaTable.forName(spark, target).alias("t")
        .merge(dim_movie.alias("s"), "t.movie_id = s.movie_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Stretch & checkpoint

# MARKDOWN ********************

# ### 1. Find the MERGE and inspect the previous version

# CELL ********************

# %%sql
# -- Inspect recent changes to the Delta table
# DESCRIBE HISTORY silver.tmdb.dim_movie;

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# %%sql
# SELECT *
# FROM silver.tmdb.dim_movie VERSION AS OF 0;

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# %%sql
# SELECT *
# FROM silver.tmdb.dim_movie VERSION AS OF 1;

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 2. Convert dim_movie into SCD Type 2

# CELL ********************

# %%sql
# ALTER TABLE silver.tmdb.dim_movie
# ADD COLUMNS (
#     valid_from TIMESTAMP,
#     valid_to   TIMESTAMP,
#     is_current BOOLEAN
# );

# UPDATE silver.tmdb.dim_movie
# SET
#     valid_from = current_timestamp(),
#     valid_to   = NULL,
#     is_current = TRUE
# WHERE is_current IS NULL;

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# %%sql
# -- First pass: expire changed current records.
# MERGE INTO silver.tmdb.dim_movie AS t
# USING silver.tmdb.stg_movie AS s
# ON  t.movie_id = s.movie_id
# AND t.is_current = TRUE
# AND NOT (t.vote_average <=> s.vote_average)

# WHEN MATCHED THEN UPDATE SET
#     t.valid_to   = current_timestamp(),
#     t.is_current = FALSE;

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# %%sql
# -- Second pass: insert new current records.
# INSERT INTO silver.tmdb.dim_movie (
#     movie_id,
#     imdb_id,
#     title,
#     vote_average,
#     valid_from,
#     valid_to,
#     is_current
# )
# SELECT
#     s.movie_id,
#     s.imdb_id,
#     s.title,
#     s.vote_average,
#     current_timestamp(),
#     CAST(NULL AS TIMESTAMP),
#     TRUE
# FROM silver.tmdb.stg_movie AS s
# LEFT ANTI JOIN silver.tmdb.dim_movie AS t
#     ON  t.movie_id = s.movie_id
#     AND t.is_current = TRUE;

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# %%sql
# -- There should be no more than one current version per movie.
# SELECT movie_id, COUNT(*) AS current_versions
# FROM silver.tmdb.dim_movie
# WHERE is_current = TRUE
# GROUP BY movie_id
# HAVING COUNT(*) > 1;

# -- Inspect the rating history for a particular movie.
# SELECT
#     movie_id,
#     title,
#     vote_average,
#     valid_from,
#     valid_to,
#     is_current
# FROM silver.tmdb.dim_movie
# WHERE movie_id = '<movie_id>'
# ORDER BY valid_from;

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 3. IMDb preview: download only the ratings file

# CELL ********************

# import requests
# import os

# # Download the IMDb file
# url = "https://datasets.imdbws.com/title.ratings.tsv.gz"

# folder = "/lakehouse/default/Files"
# file_path = f"{folder}/title.ratings.tsv.gz"

# os.makedirs(folder, exist_ok=True)

# response = requests.get(url, timeout=120)
# response.raise_for_status()

# with open(file_path, "wb") as f:
#     f.write(response.content)

# print("Downloaded IMDb ratings:", len(response.content), "bytes")
# print("Saved to:", file_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# # Read the ratings with PySpark
# imdb_df = (
#     spark.read
#     .option("header", True)
#     .option("sep", "\t")
#     .option("nullValue", r"\N")
#     .csv("Files/title.ratings.tsv.gz")
#     .select(
#         F.col("tconst").alias("imdb_id"),
#         F.col("averageRating").cast("double").alias("imdb_rating"),
#         F.col("numVotes").cast("long").alias("imdb_votes")
#     )
# )

# display(imdb_df.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# #  Compare IMDb with TMDB
# tmdb_df = (
#     spark.table("silver.tmdb.dim_movie")
#     .filter(F.col("is_current") == True)
# )

# comparison = (
#     tmdb_df.join(imdb_df, on="imdb_id", how="inner")
#     .withColumn(
#         "rating_gap",
#         F.col("vote_average") - F.col("imdb_rating")
#     )
# )

# display(
#     comparison.select(
#         "title",
#         "imdb_id",
#         "vote_average",
#         "imdb_rating",
#         "imdb_votes",
#         "rating_gap"
#     )
# )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# f = notebookutils.udf.getFunctions("udf_film")
# m = spark.table("silver.tmdb.dim_movie")
# for r in m.limit(5).collect():
#     print(
#         r.title, f.movie_label(
#             budget=r.budget or 0,
#             revenue=r.revenue or 0,
#             voteAverage=r.vote_average,
#             voteCount=r.vote_count
#         )
#     )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
