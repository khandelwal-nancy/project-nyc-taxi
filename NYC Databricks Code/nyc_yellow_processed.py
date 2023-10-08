# Databricks notebook source
# MAGIC %run Jobs/NYC/nyc_yellow_load_to_sql

# COMMAND ----------

dbutils.widgets.text("file_date", "2022-01")
file_date = dbutils.widgets.get("file_date")
print(file_date)

# COMMAND ----------

v_date_id = file_date.replace("-","")

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *
from delta.tables import *

# COMMAND ----------

yellow_df = spark.read.format("parquet").load(f"/mnt/nyc/raw/yellow_raw/yellow_tripdata_{file_date}.parquet")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformations performed
# MAGIC 1. Renamed columns - vendorID, RatecodeID, PULocationID, DOLocationID
# MAGIC 2. Added a new columns for trip hour, pickup_date, dropoff_date, 
# MAGIC 3. Replaced null values in congestion_surcharge and airport_fee columns with 0
# MAGIC 4. Replaced null values in store_and_fwd_flag with 'N'
# MAGIC 5. Added new columns for date_id and trip_id, when combined will work as composite keys for incremental loads
# MAGIC 6. Change data type of congestion_surcharge column to double (Error with few monthly file having different dta types )

# COMMAND ----------

yellow_final_df = yellow_df.withColumn("pickup_hour", date_format("tpep_pickup_datetime", "H")) \
    .withColumnRenamed("VendorID", "vendor_id") \
    .withColumn("pickup_time", col("tpep_pickup_datetime").cast("date")) \
    .withColumn("dropoff_time", col("tpep_dropoff_datetime").cast("date")) \
    .withColumnRenamed("RatecodeID", "rate_code") \
    .withColumnRenamed("PULocationID", "pickup_location_id") \
    .withColumnRenamed("DOLocationID", "dropoff_location_id") \
    .fillna(0, subset = ["congestion_surcharge", "airport_fee"]) \
    .fillna(5, subset = ["rate_code"]) \
    .fillna('N', subset = ["store_and_fwd_flag"]) \
    .withColumn("congestion_surcharge", col("congestion_surcharge").cast("float")) \
    .drop("tpep_pickup_datetime", "tpep_dropoff_datetime") \
    .withColumn("date_id", lit(v_date_id)) \
    .withColumn("trip_id", monotonically_increasing_id() + 1 ) \
    

# COMMAND ----------

delta_yellow_target = DeltaTable.forPath(spark, '/mnt/nyc/processed/nyc_yellow_trip_data')

# COMMAND ----------

delta_yellow_target.alias("target") \
  .merge(yellow_final_df.alias("updates"), "target.date_id = updates.date_id AND target.trip_id = updates.trip_id") \
  .whenMatchedUpdateAll() \
  .whenNotMatchedInsertAll() \
  .execute()

# COMMAND ----------

db_table = "nyc_yellow_trip_data"

# COMMAND ----------

load_to_adw(yellow_final_df, db_table)