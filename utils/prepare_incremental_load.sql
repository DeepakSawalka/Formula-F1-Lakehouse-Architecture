-- Databricks notebook source

DROP DATABASE IF EXISTS f1_processed CASCADE;

-- COMMAND ----------


CREATE DATABASE IF NOT EXISTS f1_processed
LOCATION "/mnt/formulaf1dlsa/silver";

-- COMMAND ----------


DROP DATABASE IF EXISTS f1_presentation CASCADE;

-- COMMAND ----------


CREATE DATABASE IF NOT EXISTS f1_presentation 
LOCATION "/mnt/formulaf1dlsa/gold";