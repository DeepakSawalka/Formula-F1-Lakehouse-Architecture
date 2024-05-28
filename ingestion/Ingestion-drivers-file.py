# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest drivers.json file

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read the json file using spark dataframe reader api

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

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from pyspark.sql.functions import lit, col, concat, current_timestamp

# COMMAND ----------


dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------


dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date" )

# COMMAND ----------

name_schema = StructType(fields=[StructField("forename", StringType(),True),
                                 StructField("surname", StringType(),True)
                                 
]
)

# COMMAND ----------

drivers_schema = StructType(fields=[StructField("driverId", IntegerType(),False),
                                 StructField("driverRef", IntegerType(),True),
                                 StructField("number", IntegerType(),True),
                                 StructField("code", StringType(),True),
                                 StructField("name", name_schema,True),
                                 StructField("dob", DateType(),True),
                                 StructField("nationality", StringType(),True),
                                 StructField("url", StringType(),True)
                                 
]
)

# COMMAND ----------


drivers_df = spark.read\
.schema(drivers_schema)\
.json(f"{bronze_url}/{v_file_date}/drivers.json")

# COMMAND ----------

drivers_df.printSchema()

# COMMAND ----------


display(drivers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Rename the column and add new column

# COMMAND ----------

drivers_with_column_df = drivers_df.withColumnRenamed("driverId","driver_id") \
.withColumnRenamed("driverRef","driver_ref") \
.withColumn("ingestion_date", current_timestamp()) \
.withColumn("name", concat(col("name.forename"),lit(" "), col("name.surname"))) \
.withColumn("data_source", lit(v_data_source)) \
.withColumn("file_date", lit(v_file_date))


# COMMAND ----------

display(drivers_with_column_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Drop unwanted column from dataframe

# COMMAND ----------

drivers_final_df = drivers_with_column_df.drop(col('url'))

# COMMAND ----------

display(drivers_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Writing data to data lake as parquet

# COMMAND ----------


drivers_final_df.write.mode("overwrite").format("delta").saveAsTable("formulaf1_dev.silver.drivers")


# COMMAND ----------


dbutils.notebook.exit("Success")
