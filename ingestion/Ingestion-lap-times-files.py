# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest lap_times folder

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read multiple csv files using spark dataframe reader api

# COMMAND ----------

# MAGIC
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC
# MAGIC %run "../includes/external_location_uc"

# COMMAND ----------

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

lap_times_schema = StructType(fields=[StructField("raceId", IntegerType(),False),
                                 StructField("driverId", IntegerType(),True),
                                 StructField("lap", IntegerType(),True),
                                  StructField("position", IntegerType(),True),
                                 StructField("time", StringType(),True),
                                 StructField("milliseconds", IntegerType(),True)
                                                                  
]
)

# COMMAND ----------


lap_times_df = spark.read\
.schema(lap_times_schema)\
.csv(f"{bronze_url}/{v_file_date}/lap_times")

# COMMAND ----------


display(lap_times_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Rename the column and add new column

# COMMAND ----------

lap_times_final_df = lap_times_df.withColumnRenamed("driverId","driver_id") \
.withColumnRenamed("raceId","race_id") \
.withColumn("ingestion_date", current_timestamp()) \
.withColumn("data_source", lit(v_data_source)) \
.withColumn("file_date", lit(v_file_date))


# COMMAND ----------

display(lap_times_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Writing data to data lake as parquet

# COMMAND ----------


# overwrite_partition(lap_times_final_df, 'f1_processed', 'results', 'race_id')


# COMMAND ----------


merge_condition = "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.lap = src.lap AND tgt.race_id = src.race_id"
merge_delta_data(lap_times_final_df, 'formulaf1_dev.silver', 'lap_times', merge_condition, 'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM formulaf1_dev.silver.lap_times

# COMMAND ----------

dbutils.notebook.exit("Success")