# Databricks notebook source
# MAGIC %md
# MAGIC #### Read the data from required files

# COMMAND ----------

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


dbutils.widgets.text("p_file_date", "")
v_file_date = dbutils.widgets.get("p_file_date" )

# COMMAND ----------

drivers_df = spark.table("formulaf1_dev.silver.drivers") \
.withColumnRenamed("number","driver_number") \
.withColumnRenamed("name","driver_name") \
.withColumnRenamed("nationality","driver_nationality")

# COMMAND ----------

constructors_df = spark.table("formulaf1_dev.silver.constructors") \
.withColumnRenamed("name","team")

# COMMAND ----------

circuits_df = spark.table("formulaf1_dev.silver.circuits") \
.withColumnRenamed("location","circuit_location")

# COMMAND ----------

races_df = spark.table("formulaf1_dev.silver.races") \
.withColumnRenamed("name","race_name") \
.withColumnRenamed("race_timestamp","race_date")

# COMMAND ----------


results_df = spark.table("formulaf1_dev.silver.results") \
.filter(f"file_date = '{v_file_date}'") \
.withColumnRenamed("time","race_time") \
.withColumnRenamed("race_id", "result_race_id") \
.withColumnRenamed("file_date", "result_file_date") 


# COMMAND ----------

# MAGIC %md
# MAGIC #### Join circuits to races

# COMMAND ----------

race_circuits_df =  races_df.join(circuits_df, races_df.circuit_id == circuits_df.circuit_id, "inner").select(races_df.race_id, races_df.race_year, races_df.race_name, races_df.race_date, circuits_df.circuit_location)


# COMMAND ----------

# MAGIC %md
# MAGIC #### Join all other dataframes

# COMMAND ----------

race_results_df =  results_df.join(race_circuits_df, results_df.result_race_id == race_circuits_df.race_id) \
                             .join(drivers_df, results_df.driver_id == drivers_df.driver_id) \
                             .join(constructors_df, results_df.constructor_id == constructors_df.constructor_id)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

race_final_df = race_results_df.select("race_id","race_name","race_date","race_year", "circuit_location", "driver_number", "driver_name", "driver_nationality", "team", "grid", "fastest_lap","race_time", "points","position","result_file_date") \
.withColumn("created_date", current_timestamp()) \
.withColumnRenamed("result_file_date", "file_date")

# COMMAND ----------


display(race_final_df)

# COMMAND ----------

#overwrite_partition(race_final_df, 'f1_presentation', 'race_results', 'race_id')

# COMMAND ----------


merge_condition = "tgt.driver_name = src.driver_name AND tgt.race_id = src.race_id AND tgt.team = src.team"
merge_delta_data(race_final_df, 'formulaf1_dev.gold', 'race_results', merge_condition, 'race_id')

# COMMAND ----------


display(race_final_df)

# COMMAND ----------


display(race_final_df.filter("race_year == 2020 and race_name == 'Abu Dhabi Grand Prix'").orderBy(race_final_df.points.desc()))

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC SELECT * FROM formulaf1_dev.gold.race_results where race_year = 2021;

# COMMAND ----------



dbutils.notebook.exit("Success")

# COMMAND ----------


# final_df.write.mode("overwrite").format("parquet").saveAsTable(f"f1_presentation.race_results")

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

