# Databricks notebook source
# MAGIC %md
# MAGIC ### Unity Catalog Object Names
# MAGIC

# COMMAND ----------

catalog_name = "formula1_project"
bronze_schema = "bronze"
silver_schema = "silver"
gold_schema = "gold"


# COMMAND ----------

# MAGIC %md
# MAGIC ### Folder Paths
# MAGIC

# COMMAND ----------

landing_folder_path = "/Volumes/formula1_project/landing/data_source"