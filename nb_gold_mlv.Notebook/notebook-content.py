# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "422c14b7-cf0d-4c0d-bdb3-5d746b8b096e",
# META       "default_lakehouse_name": "gold",
# META       "default_lakehouse_workspace_id": "31dc5d10-f99a-484d-a82e-121e5cdb013f",
# META       "known_lakehouses": [
# META         {
# META           "id": "422c14b7-cf0d-4c0d-bdb3-5d746b8b096e"
# META         },
# META         {
# META           "id": "dcf68879-71e4-4cad-b64b-057107420776"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE MATERIALIZED LAKE VIEW IF NOT EXISTS gold.dbo.mlv_movie_summary
# MAGIC ( CONSTRAINT valid_runtime CHECK (runtime > 0) ON MISMATCH DROP )
# MAGIC AS
# MAGIC SELECT
# MAGIC     m.movie_id,
# MAGIC     m.title,
# MAGIC     m.release_date,
# MAGIC     m.vote_average,
# MAGIC     m.runtime,
# MAGIC     m.budget,
# MAGIC     m.revenue,
# MAGIC     COUNT(DISTINCT c.person_id) AS cast_size
# MAGIC FROM silver.tmdb.dim_movie m
# MAGIC LEFT JOIN silver.tmdb.fact_movie_cast c
# MAGIC ON c.movie_id = m.movie_id
# MAGIC GROUP BY ALL;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Notebook changed
