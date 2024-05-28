-- Databricks notebook source

CREATE CATALOG IF NOT EXISTS formulaf1_dev;

-- COMMAND ----------



USE CATALOG formulaf1_dev;

-- COMMAND ----------


CREATE SCHEMA IF NOT EXISTS bronze
MANAGED LOCATION "abfss://bronze@formulaf1dlsa.dfs.core.windows.net/"

-- COMMAND ----------


CREATE SCHEMA IF NOT EXISTS silver
MANAGED LOCATION "abfss://silver@formulaf1dlsa.dfs.core.windows.net/"

-- COMMAND ----------



CREATE SCHEMA IF NOT EXISTS gold
MANAGED LOCATION "abfss://gold@formulaf1dlsa.dfs.core.windows.net/"

-- COMMAND ----------

SHOW SCHEMAS;