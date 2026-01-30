# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "ca6b0737-c713-46a3-a252-aa2a977d0891",
# META       "default_lakehouse_name": "TutorialLakehouse",
# META       "default_lakehouse_workspace_id": "141128c0-87b9-48bd-853c-cefc72bfaf12",
# META       "known_lakehouses": [
# META         {
# META           "id": "ca6b0737-c713-46a3-a252-aa2a977d0891"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
spark.sql("DROP TABLE IF EXISTS Silver_Rides")
spark.sql("DROP TABLE IF EXISTS Gold_Fact_Rides")
spark.sql("DROP TABLE IF EXISTS Gold_Dim_Date")
spark.sql("DROP TABLE IF EXISTS Gold_Dim_Station")
spark.sql("DROP TABLE IF EXISTS Gold_Dim_BikeType")
spark.sql("DROP TABLE IF EXISTS Gold_Dim_Rider")
spark.sql("DROP TABLE IF EXISTS bike_table_load_from_file")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
