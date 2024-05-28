# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest results.json file

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read the json file using spark dataframe reader api

# COMMAND ----------

# MAGIC
# MAGIC %run "../includes/configuration"

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

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, FloatType
from pyspark.sql.functions import lit, col, concat, current_timestamp

# COMMAND ----------


dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------


dbutils.widgets.text("p_file_date", "")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

results_schema = StructType(fields=[StructField("resultId", IntegerType(),False),
                                 StructField("raceId", IntegerType(),False),
                                 StructField("driverId", IntegerType(),False),
                                 StructField("constructorId", IntegerType(),False),
                                 StructField("number", IntegerType(),True),
                                 StructField("grid", IntegerType(),False),
                                 StructField("position", IntegerType(),True),
                                 StructField("positionText", StringType(),False),
                                 StructField("positionOrder", IntegerType(),False),
                                 StructField("points", FloatType(),False),
                                 StructField("laps", IntegerType(),False),
                                 StructField("time", StringType(),True),
                                 StructField("milliseconds", IntegerType(),True),
                                 StructField("fastestLap", IntegerType(),True),
                                 StructField("rank", IntegerType(),True),
                                 StructField("fastestLapTime", StringType(),True),
                                 StructField("fastestLapSpeed", StringType(),True),
                                 StructField("statusId", IntegerType(),False)
                                                                  
]
)

# COMMAND ----------


results_df = spark.read\
.schema(results_schema)\
.json(f"{bronze_url}/{v_file_date}/results.json")

# COMMAND ----------

results_df.printSchema()

# COMMAND ----------


display(results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Rename the column and add new column

# COMMAND ----------

results_with_column_df = results_df.withColumnRenamed("resultId","result_id") \
.withColumnRenamed("raceId","race_id") \
.withColumnRenamed("driverId","driver_id") \
.withColumnRenamed("constructorId","constructor_id") \
.withColumnRenamed("positionText","position_text") \
.withColumnRenamed("positionOrder","position_order") \
.withColumnRenamed("fastestLap","fastest_lap") \
.withColumnRenamed("fastestLapTime","fastest_lap_time") \
.withColumnRenamed("fastestLapSpeed","fastest_lap_speed") \
.withColumn("ingestion_date", current_timestamp()) \
.withColumn("data_source", lit(v_data_source)) \
.withColumn("file_date", lit(v_file_date)) 


# COMMAND ----------

display(results_with_column_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Drop unwanted column from dataframe

# COMMAND ----------

results_final_df = results_with_column_df.drop(col('statusId'))

# COMMAND ----------


results_deduped_df = results_final_df.dropDuplicates(['race_id', 'driver_id'])

# COMMAND ----------

display(results_deduped_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Writing data to data lake as parquet

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC #### Method 1

# COMMAND ----------


#for race_id_list in results_final_df.select("race_id").distinct().collect():
 #   if (spark._jsparkSession.catalog().tableExists("f1_processed.results")):
  #      spark.sql(f"ALTER TABLE f1_processed.results DROP IF EXISTS PARTITION (race_id = {race_id_list.race_id})")

# COMMAND ----------


#results_final_df.write.mode("overwrite").partitionBy('race_id').format("parquet").saveAsTable("f1_processed.results")


# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC #### Method 2

# COMMAND ----------


#overwrite_partition(results_final_df, 'f1_processed', 'results', 'race_id')

# COMMAND ----------


merge_condition = "tgt.result_id = src.result_id AND tgt.race_id = src.race_id"
merge_delta_data(results_deduped_df, 'formulaf1_dev.silver', 'results', merge_condition, 'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM formulaf1_dev.silver.results

# COMMAND ----------


dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, COUNT(1) 
# MAGIC FROM formulaf1_dev.silver.results
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC;

# COMMAND ----------

