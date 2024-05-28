# Databricks notebook source
# MAGIC %run "../includes/external_location_uc"

# COMMAND ----------

# MAGIC
# MAGIC %run "../includes/create_catalog_schema"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ingest circuits.csv file

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read the csv file using spark datagrame reader api

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql.functions import col, lit

# COMMAND ----------


dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------


dbutils.widgets.text("p_file_date", "")
v_file_date = dbutils.widgets.get("p_file_date" )

# COMMAND ----------


v_data_source

# COMMAND ----------


v_file_date

# COMMAND ----------

circuits_schema = StructType(fields=[StructField("circuitId", IntegerType(), False),
                             StructField("circuitRef", StringType(), True),
                             StructField("name", StringType(), True),
                             StructField("location", StringType(), True),
                             StructField("Country", StringType(), True),
                             StructField("lat", DoubleType(), True),
                             StructField("lng", DoubleType(), True),
                             StructField("alt", IntegerType(), True),
                             StructField("url", StringType(), True)
])

# COMMAND ----------


circuits_df = spark.read.option("header",True)\
.schema(circuits_schema)\
.csv(f"{bronze_url}/{v_file_date}/circuits.csv")

# COMMAND ----------


display(circuits_df)

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

circuits_df.describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Select the required columns

# COMMAND ----------

circuits_selected_df = circuits_df.select(col("circuitId"),
                                          col("circuitRef"),col("name"),col("location"),col("Country")
                                          ,col("lat"),col("lng"),col("alt"))

# COMMAND ----------

display(circuits_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Renaming the columns

# COMMAND ----------

circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId","circuit_id") \
.withColumnRenamed("circuitRef","circuit_ref") \
.withColumnRenamed("lat","latitude") \
.withColumnRenamed("lng","longitude") \
.withColumnRenamed("alt","altitude") \
.withColumn("data_source", lit(v_data_source)) \
.withColumn("file_date", lit(v_file_date))


# COMMAND ----------

display(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Adding a new column of ingestion date

# COMMAND ----------

circuits_final_df = add_ingestion_date(circuits_renamed_df)

# COMMAND ----------

display(circuits_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Writing data to data lake as parquet

# COMMAND ----------


circuits_final_df.write.mode("overwrite").format("delta").saveAsTable("formulaf1_dev.silver.circuits")


# COMMAND ----------





# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM formulaf1_dev.silver.circuits

# COMMAND ----------


dbutils.notebook.exit("Success")