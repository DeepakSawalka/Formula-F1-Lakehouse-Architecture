# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest constructors.json file

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read the json file using spark dataframe reader api

# COMMAND ----------

# MAGIC %md
# MAGIC #### Defining DDL type schema

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

from pyspark.sql.functions import current_timestamp,lit,col

# COMMAND ----------


dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------


dbutils.widgets.text("p_file_date", "")
v_file_date = dbutils.widgets.get("p_file_date" )

# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------


constructor_df = spark.read\
.schema(constructors_schema)\
.json(f"{bronze_url}/{v_file_date}/constructors.json")

# COMMAND ----------

constructor_df.printSchema()

# COMMAND ----------


display(constructor_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Drop unwanted column from dataframe

# COMMAND ----------

constructor_dropped_df = constructor_df.drop(constructor_df['url'])

# COMMAND ----------

display(constructor_dropped_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Rename the column and add new column

# COMMAND ----------

constructor_final_df = constructor_dropped_df.withColumnRenamed("constructorId","constructor_id") \
.withColumnRenamed("constructorRef","constructor_ref") \
.withColumn("ingestion_date", current_timestamp()) \
.withColumn("data_source", lit(v_data_source)) \
.withColumn("file_date", lit(v_file_date))


# COMMAND ----------

display(constructor_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Writing data to data lake as parquet

# COMMAND ----------


constructor_final_df.write.mode("overwrite").format("delta").saveAsTable("formulaf1_dev.silver.constructors")


# COMMAND ----------


dbutils.notebook.exit("Success")
