# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest pit_stops.json file

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read the json file using spark dataframe reader api

# COMMAND ----------

# MAGIC
# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC
# MAGIC %run "../includes/external_location_uc"

# COMMAND ----------

# MAGIC
# MAGIC %run "../includes/create_catalog_schema"

# COMMAND ----------

# MAGIC
# MAGIC %run "../includes/configuration"

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

pit_stops_schema = StructType(fields=[StructField("raceId", IntegerType(),False),
                                 StructField("driverId", IntegerType(),True),
                                 StructField("stop", StringType(),True),
                                 StructField("lap", IntegerType(),True),
                                 StructField("time", StringType(),True),
                                 StructField("duration", StringType(),True),
                                 StructField("milliseconds", IntegerType(),True)
                                                                  
]
)

# COMMAND ----------


pit_stops_df = spark.read\
.schema(pit_stops_schema)\
.option("multiLine",True)\
.json(f"{bronze_url}/{v_file_date}/pit_stops.json")

# COMMAND ----------

pit_stops_df.printSchema()

# COMMAND ----------


display(pit_stops_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Rename the column and add new column

# COMMAND ----------

pit_stops_final_df = pit_stops_df.withColumnRenamed("raceId","race_id") \
.withColumnRenamed("driverId","driver_id") \
.withColumn("ingestion_date", current_timestamp()) \
.withColumn("data_source", lit(v_data_source)) \
.withColumn("file_date", lit(v_file_date))


# COMMAND ----------

display(pit_stops_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Writing data to data lake as parquet

# COMMAND ----------


#overwrite_partition(results_final_df, 'f1_processed', 'results', 'race_id')


# COMMAND ----------


merge_condition = "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.stop = src.stop AND tgt.race_id = src.race_id"
merge_delta_data(pit_stops_final_df, 'formulaf1_dev.silver', 'pit_stops', merge_condition, 'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM formulaf1_dev.silver.pit_stops

# COMMAND ----------


dbutils.notebook.exit("Success")

# COMMAND ----------

