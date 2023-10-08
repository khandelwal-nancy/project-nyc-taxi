# Databricks notebook source
# MAGIC %md
# MAGIC ##### Load dim_payment_mode.csv as delta table

# COMMAND ----------

# MAGIC %run /Jobs/NYC/nyc_yellow_load_to_sql

# COMMAND ----------

dim_payment_mode = spark.read.csv("/mnt/nyc/raw/dim_raw/dim_payment_mode.csv", header = True, inferSchema = True)

# COMMAND ----------

dim_payment_mode.write.mode("overwrite").format("delta").saveAsTable("nyc_processed.dim_payment_mode")

# COMMAND ----------

truncate_load_to_adw(dim_payment_mode,"dim_payment_mode")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Load dim_rate_code.csv as delta table

# COMMAND ----------

dim_rate_code = spark.read.csv("/mnt/nyc/raw/dim_raw/dim_rate_code.csv", header = True, inferSchema = True)

# COMMAND ----------

dim_rate_code.write.mode("overwrite").format("delta").saveAsTable("nyc_processed.dim_rate_code")

# COMMAND ----------

truncate_load_to_adw(dim_rate_code,"dim_rate_code")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Load dim_taxi_zone.csv as delta table

# COMMAND ----------

dim_taxi_zone = spark.read.csv("/mnt/nyc/raw/dim_raw/dim_taxi_zone.csv", header = True, inferSchema = True)

# COMMAND ----------

dim_taxi_zone.write.mode("overwrite").format("delta").saveAsTable("nyc_processed.dim_taxi_zone")

# COMMAND ----------

truncate_load_to_adw(dim_taxi_zone,"dim_taxi_zone")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Load dim_vendor.csv as delta table

# COMMAND ----------

dim_vendor = spark.read.csv("/mnt/nyc/raw/dim_raw/dim_vendor.csv", header = True, inferSchema = True)

# COMMAND ----------

dim_vendor.write.mode("overwrite").format("delta").saveAsTable("nyc_processed.dim_vendor")

# COMMAND ----------

truncate_load_to_adw(dim_vendor,"dim_vendor")