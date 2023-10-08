-- Databricks notebook source
DROP DATABASE IF EXISTS nyc_processed CASCADE

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS nyc_processed
LOCATION "/mnt/nyc/processed/"

-- COMMAND ----------

DROP DATABASE IF EXISTS nyc_presentation CASCADE

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS nyc_presentation
LOCATION "/mnt/nyc/presentation/"