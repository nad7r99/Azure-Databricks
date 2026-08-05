# Databricks notebook source
# MAGIC %md
# MAGIC ### Add metadata function

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

def Add_metadata(df):
    return(
        df.withColumn('ingestion_timestamp', F.current_timestamp())\
          .withColumn('data_source', F.col('_metadata.file_path'))
    )