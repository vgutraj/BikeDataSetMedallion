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

# MARKDOWN ********************

# **Bronze to Silver**

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
df_bronze = spark.read.table("bike_table_load_from_file")

from pyspark.sql.functions import col, to_date
df_silver = (
    df_bronze
        .withColumn("started_at", col("started_at").cast("timestamp"))
        .withColumn("ended_at", col("ended_at").cast("timestamp"))
        .withColumn("started_ride_date", to_date(col("started_at")))
        .withColumn("ended_ride_date", to_date(col("ended_at")))
        .withColumn("start_lat", col("start_lat").cast("double"))
        .withColumn("start_lng", col("start_lng").cast("double"))
        .withColumn("end_lat", col("end_lat").cast("double"))
        .withColumn("end_lng", col("end_lng").cast("double"))
        .select(
            "ride_id",
            "rideable_type",
            "started_at",
            "ended_at",
            "started_ride_date",
            "ended_ride_date",
            "start_station_id",
            "start_station_name",
            "start_lat",
            "start_lng",
            "end_station_id",
            "end_station_name",
            "end_lat",
            "end_lng",
            "member_casual"   # ← ADD THIS BACK
        )
)


from pyspark.sql.functions import (col, unix_timestamp) 

df_silver = df_silver.withColumn( "ride_duration_minutes", (unix_timestamp(col("ended_at")) - unix_timestamp(col("started_at"))) / 60 )                        
                        
display(df_silver.limit(20))
df_silver.write.format("delta").mode("overwrite").saveAsTable("Silver_CitiBike")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
