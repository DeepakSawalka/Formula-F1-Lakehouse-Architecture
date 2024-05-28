# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest Qualifying folder

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read multiple json files(multi line) using spark dataframe reader api

# COMMAND ----------

# MAGIC
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC
# MAGIC %run "../includes/external_location_uc"

# COMMAND ----------

# MAGIC
# MAGIC
# MAGIC %run "../includes/create_catalog_schema"

# COMMAND ----------

# MAGIC
# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, FloatType
from pyspark.sql.functions import lit, col, concat, current_timestamp

# COMMAND ----------


dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------


dbutils.widgets.text("p_file_date", "")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

qualifying_schema = StructType(fields=[StructField("qualifyId", IntegerType(),False),
                                 StructField("raceId", IntegerType(),True),
                                 StructField("driverId", IntegerType(),True),
                                  StructField("constructorId", IntegerType(),True),
                                 StructField("number", IntegerType(),True),
                                 StructField("position", IntegerType(),True),
                                 StructField("q1", StringType(),True),
                                 StructField("q2", StringType(),True),
                                 StructField("q3", StringType(),True)
                                                                  
]
)

# COMMAND ----------


qualifying_df = spark.read\
.schema(qualifying_schema)\
.option("multiLine",True)\
.json(f"{bronze_url}/{v_file_date}/qualifying")

# COMMAND ----------


display(qualifying_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Rename the column and add new column

# COMMAND ----------

qualifying_final_df = qualifying_df.withColumnRenamed("qualifyId","qualifying_id") \
.withColumnRenamed("raceId","race_id") \
.withColumnRenamed("driverId","driver_id") \
.withColumnRenamed("constructorId","constructor_id") \
.withColumn("ingestion_date", current_timestamp()) \
.withColumn("data_source", lit(v_data_source)) \
.withColumn("file_date", lit(v_file_date))

# COMMAND ----------

display(qualifying_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Writing data to data lake as parquet

# COMMAND ----------


#overwrite_partition(qualifying_final_df, 'f1_processed', 'results', 'race_id')


# COMMAND ----------


merge_condition = "tgt.qualifying_id = src.qualifying_id AND tgt.race_id = src.race_id"
merge_delta_data(qualifying_final_df, 'formulaf1_dev.silver', 'qualifying', merge_condition, 'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM formulaf1_dev.silver.qualifying

# COMMAND ----------


dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DROP TABLE IF EXISTS formulaf1_dev.silver.qualifying

# COMMAND ----------

