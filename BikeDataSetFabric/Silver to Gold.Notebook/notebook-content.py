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
from pyspark.sql.functions import col, count, avg 
df_silver = spark.read.table("Silver_CitiBike")

from pyspark.sql.functions import count

df_gold_daily = (
    df_silver
        .groupBy("started_ride_date")
        .agg(count("*").alias("ride_count"))
)

df_gold_daily.write.format("delta").mode("overwrite").saveAsTable("Gold_RidesPerDay")




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Date Dimension**

# CELL ********************

from pyspark.sql.functions import sequence, to_date, col, explode

df_dim_date = (
    spark.createDataFrame([("2018-01-01", "2030-12-31")], ["start", "end"])
        .select(explode(sequence(to_date(col("start")), to_date(col("end")))).alias("date"))
)
from pyspark.sql.functions import year, month, dayofmonth, weekofyear, date_format

df_dim_date = (
    df_dim_date
        .withColumn("year", year(col("date")))
        .withColumn("month", month(col("date")))
        .withColumn("day", dayofmonth(col("date")))
        .withColumn("week", weekofyear(col("date")))
        .withColumn("day_of_week", date_format(col("date"), "E"))
)
df_dim_date.write.format("delta").mode("overwrite").saveAsTable("Gold_Dim_Date")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Ride Fact**

# CELL ********************

from pyspark.sql.functions import col

df_fact_rides = (
    df_silver.select(
        col("ride_id"),

        # Foreign keys
        col("started_ride_date").alias("date_key"),
        col("start_station_id").alias("start_station_key"),
        col("end_station_id").alias("end_station_key"),
        col("rideable_type").alias("bike_type_key"),
        col("member_casual").alias("rider_type_key"),

        # Measures
        col("ride_duration_minutes"),

        # Optional but useful timestamps
        col("started_at"),
        col("ended_at")
    )
)
df_fact_rides.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("Gold_Fact_Rides")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Station Dimension**

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.functions import col

# Extract start stations
df_start = (
    df_silver
        .select(
            col("start_station_id").alias("station_id"),
            col("start_station_name").alias("station_name"),
            col("start_lat").alias("latitude"),
            col("start_lng").alias("longitude")
        )
)

# Extract end stations
df_end = (
    df_silver
        .select(
            col("end_station_id").alias("station_id"),
            col("end_station_name").alias("station_name"),
            col("end_lat").alias("latitude"),
            col("end_lng").alias("longitude")
        )
)

# Combine
df_stations_combined = df_start.unionByName(df_end)

# ⭐ Filter out invalid station IDs
df_stations_clean = (
    df_stations_combined
        .filter(
            (col("station_id").isNotNull()) &
            (F.trim(col("station_id")) != "")
        )
)

# Deduplicate by station_id
df_dim_station = (
    df_stations_clean
        .groupBy("station_id")
        .agg(
            F.first("station_name", ignorenulls=True).alias("station_name"),
            F.first("latitude", ignorenulls=True).alias("latitude"),
            F.first("longitude", ignorenulls=True).alias("longitude")
        )
)

# Write to Gold
df_dim_station.write.format("delta").mode("overwrite").saveAsTable("Gold_Dim_Station")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Bike Dimension**

# CELL ********************

df_dim_biketype = (
    df_silver
        .select(col("rideable_type"))
        .distinct()
        .withColumnRenamed("rideable_type", "bike_type")
)
df_dim_biketype.write.format("delta").mode("overwrite").saveAsTable("Gold_Dim_BikeType")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM TutorialLakehouse.dbo.bike_table_load_from_file LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Rider Dimension**

# CELL ********************

df_dim_rider = (
    df_silver
        .select("member_casual")
        .distinct()
        .withColumnRenamed("member_casual", "rider_type")
)
df_dim_rider.write.format("delta").mode("overwrite").saveAsTable("Gold_Dim_Rider")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
