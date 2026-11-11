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
# META           "id": "b42cafac-e068-467c-9ffe-929335a09016"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Allow notebooks with different default lakehouses to run in this DAG.
dag = {
    "activities": [
        {
            "name": "Bronze",
            "path": "nb_bronze_tmdb",
            "timeoutPerCellInSeconds": 600,
            "args": {"useRootDefaultLakehouse": True}
        },
        {
            "name": "Silver",
            "path": "nb_silver_tmdb",
            "timeoutPerCellInSeconds": 600,
            "args": {"useRootDefaultLakehouse": True},

            "dependencies": ["Bronze"]
        },
        {
            "name": "Gold",
            "path": "nb_gold_mlv",
            "timeoutPerCellInSeconds": 600,
            "args": {"useRootDefaultLakehouse": True},
            "dependencies": ["Silver"]
        }
    ],
    "timeoutInSeconds": 1800,
    "concurrency": 1
}

# Check that the DAG configuration and notebook dependencies are valid.
notebookutils.notebook.validateDAG(dag)

# Execute the notebooks in dependency order: Bronze -> Silver -> Gold.
results = notebookutils.notebook.runMultiple(dag)

print(results)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
