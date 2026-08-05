# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %run ../00.Common/1.environment-config

# COMMAND ----------

target_table =  f'{catalog_name}.{gold_schema}.dim_constructors'

# COMMAND ----------

# MAGIC %md
# MAGIC ### read resource table

# COMMAND ----------

constructors_df = spark.table(f'{catalog_name}.{silver_schema}.constructors')
region_df = spark.table(f'{catalog_name}.{gold_schema}.ref_nationality_region')

# COMMAND ----------

# MAGIC %md
# MAGIC ### join

# COMMAND ----------

dim_constructors_df = constructors_df.join(region_df, 
                                           on='nationality', 
                                           how='left') 

# COMMAND ----------

dim_constructors_df = dim_constructors_df.select(
                                                 F.col('constructor_id'), 
                                                 F.col('constructor_name'), 
                                                 F.col('nationality'), 
                                                 F.col('region')
                                            )

# COMMAND ----------

# MAGIC %md
# MAGIC ### write to gold_layer table 

# COMMAND ----------

dim_constructors_df.write.format('delta')\
                         .mode('overwrite')\
                         .saveAsTable(target_table)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validation

# COMMAND ----------

display(spark.table(target_table))