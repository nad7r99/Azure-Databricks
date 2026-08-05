# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %run ../00.Common/1.environment-config

# COMMAND ----------

target_table =  f'{catalog_name}.{gold_schema}.dim_drivers'

# COMMAND ----------

# MAGIC %md
# MAGIC ### read resources tables

# COMMAND ----------

drivers_df = spark.table(f'{catalog_name}.{silver_schema}.drivers')
region_df = spark.table(f'{catalog_name}.{gold_schema}.ref_nationality_region')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Join

# COMMAND ----------

dim_drivers_df = drivers_df.join(region_df, 
                                 on='nationality', 
                                 how='left'
                                 )

# COMMAND ----------

dim_drivers_df = dim_drivers_df.select(
                                       'driver_id',
                                       'driver_name',
                                       'date_of_birth',
                                       'nationality',
                                       'region'
                                       )

# COMMAND ----------

# MAGIC %md
# MAGIC ### write to gold_layer tables
# MAGIC

# COMMAND ----------

dim_drivers_df.write.format('delta')\
                    .mode('overwrite')\
                    .saveAsTable(target_table)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validation

# COMMAND ----------

display(spark.table(target_table))