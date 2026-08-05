# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %run ../00.Common/1.environment-config

# COMMAND ----------

target_table =  f'{catalog_name}.{gold_schema}.fact_session_result'

# COMMAND ----------

# MAGIC %md
# MAGIC ### read resource tables

# COMMAND ----------

result_df = (
    spark.table(f'{catalog_name}.{silver_schema}.results')\
         .withColumn('session_type', F.lit('RACE'))\
         .drop('race_name', 'race_date', 'ingestion_timestamp', 'data_source')
    
           )

# COMMAND ----------

sprints_df = (
    
    spark.table(f'{catalog_name}.{silver_schema}.sprints')\
         .withColumn('session_type', F.lit('SPRINTS'))\
         .drop('race_name', 'race_date', 'ingestion_timestamp', 'data_source')
    
    
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Union by name

# COMMAND ----------

result_sprints_df = (
    result_df.unionByName(sprints_df)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add dervied columns 

# COMMAND ----------

result_sprints_df = (
    result_sprints_df.withColumn('is_win', F.col('position') == 1)\
                     .withColumn('is_podium', F.col('position').between(1, 3))\
                     .withColumn('has_points', F.col('points') > 0)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### write to gold_layer tables

# COMMAND ----------

result_sprints_df.write.format('delta')\
                       .mode('overwrite')\
                       .saveAsTable(target_table)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validation

# COMMAND ----------

display(spark.table(target_table))