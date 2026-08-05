# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %run ../00.Common/1.environment-config

# COMMAND ----------

target_table = f'{catalog_name}.{gold_schema}.dim_races'

# COMMAND ----------

# MAGIC %md
# MAGIC ### read resource tables

# COMMAND ----------

circuits_df = spark.table(f'{catalog_name}.{silver_schema}.circuits')

# COMMAND ----------

races_df = spark.table(f'{catalog_name}.{silver_schema}.races')

# COMMAND ----------

# MAGIC %md
# MAGIC #### JOIN, The Relation is one-to-one so there no duplication

# COMMAND ----------

dim_races_df = circuits_df.join(
                               races_df, 
                                on='circuit_id', 
                                how='inner'
                                )

display(dim_races_df)

# COMMAND ----------

dim_races_df = dim_races_df.select(
                                   F.col('season'),
                                   F.col('round'),
                                   F.col('race_name'),
                                   F.col('race_date'),
                                   F.col('circuit_name'),
                                   F.col('locality'),
                                   F.col('country')               
                                 )

# COMMAND ----------

# MAGIC %md
# MAGIC ### write to gold_layer as dim_races Table

# COMMAND ----------

dim_races_df.write.format('delta')\
                  .mode('overwrite')\
                  .saveAsTable(target_table)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validation

# COMMAND ----------

display(spark.table(target_table))