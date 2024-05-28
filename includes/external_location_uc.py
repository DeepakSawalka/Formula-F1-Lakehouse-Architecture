# Databricks notebook source
# MAGIC
# MAGIC %md
# MAGIC #### Create External locations
# MAGIC ##### 1. Bronze
# MAGIC ##### 2. Silver
# MAGIC ##### 3. Gold
# MAGIC

# COMMAND ----------

bronze_url = 'abfss://bronze@formulaf1dlsa.dfs.core.windows.net/'
silver_url = 'abfss://silver@formulaf1dlsa.dfs.core.windows.net/'
gold_url = 'abfss://gold@formulaf1dlsa.dfs.core.windows.net/'

# COMMAND ----------


spark.sql(f"""
CREATE EXTERNAL LOCATION IF NOT EXISTS `formulaf1dlsa_bronze`
URL '{bronze_url}'
WITH (STORAGE CREDENTIAL formula_f1_sc);""")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC DESC EXTERNAL LOCATION formulaf1dlsa_bronze
# MAGIC

# COMMAND ----------

spark.sql(f"""
CREATE EXTERNAL LOCATION IF NOT EXISTS `formulaf1dlsa_silver`
URL '{silver_url}'
WITH (STORAGE CREDENTIAL formula_f1_sc);""")

# COMMAND ----------

spark.sql(f"""
CREATE EXTERNAL LOCATION IF NOT EXISTS `formulaf1dlsa_gold`
URL '{gold_url}'
WITH (STORAGE CREDENTIAL formula_f1_sc);""")

# COMMAND ----------

