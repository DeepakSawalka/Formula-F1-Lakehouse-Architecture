# Databricks notebook source
# MAGIC %md
# MAGIC #### Produce driver standings

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


dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date" )

# COMMAND ----------

race_results_list = spark.table("formulaf1_dev.gold.race_results").filter(f"file_date = '{v_file_date}'") \
.select("race_year") \
.collect()

# COMMAND ----------


display(race_results_list)

# COMMAND ----------


race_year_list = []
for race_year in race_results_list:
    race_year_list.append(race_year.race_year)
# print(race_year_list)

# COMMAND ----------


from pyspark.sql.functions import col
race_results_df = spark.table("formulaf1_dev.gold.race_results").filter(col("race_year").isin(race_year_list)) 


# COMMAND ----------

display(race_results_df)

# COMMAND ----------

from pyspark.sql.functions import col, sum, count, when 

driver_standings_df = race_results_df.groupBy("race_year", "driver_name", "driver_nationality").agg(sum("points").alias("total_points"), count(when(col("position") == 1, True)).alias("wins"))

# COMMAND ----------

display(driver_standings_df)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank

driver_standings_specs = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
final_df = driver_standings_df.withColumn("rank", rank().over(driver_standings_specs))



# COMMAND ----------

display(final_df)

# COMMAND ----------

# final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.driver_standings")

# COMMAND ----------


# overwrite_partition(final_df, 'f1_presentation', 'driver_standings', 'race_year')

# COMMAND ----------


merge_condition = "tgt.driver_name = src.driver_name AND tgt.race_year = src.race_year"
merge_delta_data(final_df, 'formulaf1_dev.gold', 'driver_standings', merge_condition, 'race_year')


# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC select * from formulaf1_dev.gold.driver_standings;

# COMMAND ----------

dbutils.notebook.exit("Success")


# COMMAND ----------

