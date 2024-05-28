# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest races.csv file

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read the csv file using spark datagrame reader api

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

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType
from pyspark.sql.functions import col, current_timestamp, to_timestamp, lit, concat

# COMMAND ----------


dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------


dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date" )

# COMMAND ----------

races_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                             StructField("year", IntegerType(), True),
                             StructField("round", IntegerType(), True),
                             StructField("circuitId", IntegerType(), True),
                             StructField("name", StringType(), True),
                             StructField("date", DateType(), True),
                             StructField("time", StringType(), True),
                             StructField("url", StringType(), True)
                
])

# COMMAND ----------


races_df = spark.read.option("header",True)\
.schema(races_schema)\
.csv(f"{bronze_url}/{v_file_date}/races.csv")

# COMMAND ----------


display(races_df)

# COMMAND ----------

races_df.printSchema()

# COMMAND ----------

races_df.describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Select the required columns

# COMMAND ----------

races_selected_df = races_df.select(col("raceId"),
                                          col("year"),col("round"),col("circuitId"),col("name")
                                          ,col("date"),col("time"))

# COMMAND ----------

display(races_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Renaming the columns

# COMMAND ----------

races_renamed_df = races_selected_df.withColumnRenamed("raceId","race_id") \
.withColumnRenamed("year","race_year") \
.withColumnRenamed("circuitId","circuit_id") 



# COMMAND ----------

display(races_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Transforming and adding a new columns of race timestamp and ingestion date

# COMMAND ----------

races_final_df = races_renamed_df.withColumn("ingestion_date",current_timestamp()) \
.withColumn("race_timestamp",to_timestamp(concat(col('date'),lit(' '),col('time')),'yyyy-MM-dd HH:mm:ss')) \
.withColumn("data_source", lit(v_data_source)) \
.withColumn("file_date", lit(v_file_date))

# COMMAND ----------

display(races_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Writing data to data lake as parquet

# COMMAND ----------


races_final_df.write.mode("overwrite").format("delta").saveAsTable("formulaf1_dev.silver.races")


# COMMAND ----------


dbutils.notebook.exit("Success")
