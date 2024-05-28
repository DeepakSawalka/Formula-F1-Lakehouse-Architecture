-- Databricks notebook source

create database if not exists f1_processed
location "/mnt/formulaf1dlsa/silver"

-- COMMAND ----------


DESC DATABASE f1_raw;

-- COMMAND ----------


DESC extended f1_processed.circuits;