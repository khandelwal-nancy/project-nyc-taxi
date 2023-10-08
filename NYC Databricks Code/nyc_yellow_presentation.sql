-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Data Granuality
-- MAGIC 1. Vendor
-- MAGIC 2. Pickup zone
-- MAGIC 3. Dropoff zone
-- MAGIC 4. Payment mode
-- MAGIC 5. Year, Month, Day of week, Day, Hour wise
-- MAGIC 6. Number of passengers
-- MAGIC
-- MAGIC #### Aggregations
-- MAGIC 1. Number of rides
-- MAGIC 2. Average trip duration (in minutes)
-- MAGIC 3. Average fare
-- MAGIC 4. Average trip distance

-- COMMAND ----------

DROP TABLE IF EXISTS nyc_presentation.yellow_taxi_trip_agg

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS nyc_presentation.yellow_taxi_trip_agg
(
  vendor_name	string,
  pickup_zone string,
  dropoff_zone string,
  payment_mode	string,
  passenger_count	bigint,
  year	int	,
  month	int	,
  week	int	,
  day	int	,
  hour	int	,
  no_of_trips	bigint	,
  avg_trip_distance	double	,
  avg_amount	double	,
  avg_trip_duration_in_minutes	double
) USING DELTA

-- COMMAND ----------

INSERT OVERWRITE nyc_presentation.yellow_taxi_trip_agg
SELECT b.vendor_name,
  d.zone AS pickup_zone,
  e.zone AS dropoff_zone,
  c.payment_mode,
  a.passenger_count,
  year(a.pickup_time) as year, 
  month(a.pickup_time) as month,
  dayofweek(a.pickup_time) as week,
  day(a.pickup_time) as day,
  hour(a.pickup_time) as hour,
  count(*) as no_of_trips,
  ROUND(AVG(a.trip_distance), 2) AS avg_trip_distance,
  ROUND(AVG(a.total_amount), 2) AS avg_amount,
  ROUND(AVG(TRY_CAST(dropoff_time - pickup_time AS INT)/60),2) AS avg_trip_distance_in_minutes
FROM nyc_processed.yellow_trip_data a
JOIN nyc_processed.dim_vendor b
  ON a.vendor_id = b.vendor_id
JOIN nyc_processed.dim_payment_mode c
  ON a.payment_type = c.payment_type
JOIN nyc_processed.dim_taxi_zone d
  ON a.pickup_location_id = d.LocationID
JOIN nyc_processed.dim_taxi_zone e
  on a.dropoff_location_id = e.LocationID
GROUP BY b.vendor_name, d.zone, e.zone, c.payment_mode, a.passenger_count, year, month, week, day, hour 
ORDER BY year, month, week, day, hour

-- COMMAND ----------

DROP TABLE IF EXISTS nyc_presentation.yellow_taxi_trip_zone;

CREATE TABLE IF NOT EXISTS nyc_presentation.yellow_taxi_trip_zone
AS
SELECT b.vendor_name,
  d.zone AS pickup_zone,
  e.zone AS dropoff_zone,
  c.payment_mode,
  a.passenger_count,
  ROUND(AVG(a.trip_distance), 2) AS avg_trip_distance,
  ROUND(AVG(a.total_amount), 2) AS avg_amount,
  ROUND(AVG(TRY_CAST(dropoff_time - pickup_time AS INT)/60),2) AS avg_trip_distance_in_minutes
FROM nyc_processed.yellow_trip_data a
JOIN nyc_processed.dim_vendor b
  ON a.vendor_id = b.vendor_id
JOIN nyc_processed.dim_payment_mode c
  ON a.payment_type = c.payment_type
JOIN nyc_processed.dim_taxi_zone d
  ON a.pickup_location_id = d.LocationID
JOIN nyc_processed.dim_taxi_zone e
  on a.dropoff_location_id = e.LocationID
GROUP BY b.vendor_name, d.zone, e.zone, c.payment_mode, a.passenger_count

-- COMMAND ----------

DROP TABLE IF EXISTS nyc_presentation.yellow_taxi_trip_hourly;

CREATE TABLE IF NOT EXISTS nyc_presentation.yellow_taxi_trip_hourly
AS
SELECT b.vendor_name,
  c.payment_mode,
  a.passenger_count,
  year(a.pickup_time) as year, 
  month(a.pickup_time) as month,
  dayofweek(a.pickup_time) as week,
  day(a.pickup_time) as day,
  hour(a.pickup_time) as hour,
  count(*) as no_of_trips,
  ROUND(AVG(a.trip_distance), 2) AS avg_trip_distance,
  ROUND(AVG(a.total_amount), 2) AS avg_amount,
  ROUND(AVG(TRY_CAST(dropoff_time - pickup_time AS INT)/60),2) AS avg_trip_distance_in_minutes
FROM nyc_processed.yellow_trip_data a
JOIN nyc_processed.dim_vendor b
  ON a.vendor_id = b.vendor_id
JOIN nyc_processed.dim_payment_mode c
  ON a.payment_type = c.payment_type
JOIN nyc_processed.dim_taxi_zone d
  ON a.pickup_location_id = d.LocationID
JOIN nyc_processed.dim_taxi_zone e
  on a.dropoff_location_id = e.LocationID
GROUP BY b.vendor_name, c.payment_mode, a.passenger_count, year, month, week, day, hour 
ORDER BY year, month, week, day, hour;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Analysis
-- MAGIC 1. Compare no. of rides over time of day, day of week, monthly, yearly
-- MAGIC 2. Compare the ride duration over time of day, day of week, month wise, year wise
-- MAGIC 3. Identify outliers in trip duration that could indicate issues or anomalies
-- MAGIC 4. Plot pickup and drop-off locations on a map to visualize popular routes and areas.
-- MAGIC 5. Analyze whether there are specific areas with higher demand or longer rides.
-- MAGIC 6. Calculate the average fare per ride and explore how it changes over time or based on trip characteristics.
-- MAGIC 7. Investigate if there are any correlations between fare, distance, and time. 
-- MAGIC 8. Analyze the distribution of payment methods used by passengers (credit card, cash, etc.)
-- MAGIC 9. Check if there's a difference in payment methods based on factors like ride distance or time of day. 
-- MAGIC 10. Investigate the distribution of passenger counts per ride. 
-- MAGIC 11. Analyze if ride characteristics like distance or fare change significantly based on the number of passengers. 
-- MAGIC

-- COMMAND ----------

DROP TABLE IF EXISTS nyc_presentation.yellow_taxi_trip_agg;

CREATE TABLE IF NOT EXISTS nyc_presentation.yellow_taxi_trip_agg
USING DELTA
AS
SELECT b.vendor_name,
  c.payment_mode,
  a.passenger_count,
  to_date(pickup_time) as trip_date_time,
  count(*) as no_of_trips,
  ROUND(AVG(a.trip_distance), 2) AS avg_trip_distance,
  ROUND(AVG(a.total_amount), 2) AS avg_amount,
  ROUND(AVG(TRY_CAST(dropoff_time - pickup_time AS INT)/60),2) AS avg_trip_distance_in_minutes
FROM nyc_processed.yellow_trip_data a
JOIN nyc_processed.dim_vendor b
  ON a.vendor_id = b.vendor_id
JOIN nyc_processed.dim_payment_mode c
  ON a.payment_type = c.payment_type
JOIN nyc_processed.dim_taxi_zone d
  ON a.pickup_location_id = d.LocationID
JOIN nyc_processed.dim_taxi_zone e
  on a.dropoff_location_id = e.LocationID
GROUP BY b.vendor_name, c.payment_mode, a.passenger_count, trip_date_time
ORDER BY trip_date_time;

-- COMMAND ----------

SELECT date_format(pickup_time, "yyyy-MM-dd HH") as trip_date_time, *
FROM nyc_processed.yellow_trip_data
WHERE date_id = 201101

-- COMMAND ----------

SELECT b.vendor_name,
  d.zone AS pickup_zone,
  e.zone AS dropoff_zone,
  c.payment_mode,
  a.passenger_count,
  to_date(pickup_time) as trip_date_time,
  count(*) as no_of_trips,
  ROUND(AVG(a.trip_distance), 2) AS avg_trip_distance,
  ROUND(AVG(a.total_amount), 2) AS avg_amount,
  ROUND(AVG(TRY_CAST(dropoff_time - pickup_time AS INT)/60),2) AS avg_trip_distance_in_minutes
FROM nyc_processed.yellow_trip_data a
JOIN nyc_processed.dim_vendor b
  ON a.vendor_id = b.vendor_id
JOIN nyc_processed.dim_payment_mode c
  ON a.payment_type = c.payment_type
JOIN nyc_processed.dim_taxi_zone d
  ON a.pickup_location_id = d.LocationID
JOIN nyc_processed.dim_taxi_zone e
  on a.dropoff_location_id = e.LocationID
GROUP BY b.vendor_name, pickup_zone, dropoff_zone, c.payment_mode, a.passenger_count, trip_date_time
ORDER BY trip_date_time

-- COMMAND ----------

SELECT COUNT(*)
FROM nyc_processed.yellow_trip_data
WHERE date_id = 201101

-- COMMAND ----------

