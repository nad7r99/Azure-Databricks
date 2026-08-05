# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

dbutils.widgets.text('p_batch_id', '')
v_batch_id = dbutils.widgets.get('p_batch_id')

# COMMAND ----------

# MAGIC %run ../00.Common/1.environment-config

# COMMAND ----------

target_table =  f'{catalog_name}.{gold_schema}.dim_drivers'

# COMMAND ----------

# MAGIC %md
# MAGIC ### read resources tables

# COMMAND ----------

drivers_df = spark.table(f'{catalog_name}.{silver_schema}.drivers').filter(F.col('batch_id') == v_batch_id)


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
# MAGIC ### Add Columns

# COMMAND ----------

dim_drivers_df = dim_drivers_df.withColumn('created_timestamp', F.current_timestamp())\
                               .withColumn('updated_timestamp', F.current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ### write to gold_layer tables
# MAGIC

# COMMAND ----------

from delta import DeltaTable

if not spark.catalog.tableExists(target_table):
     dim_drivers_df.write\
           .format('delta')\
           .mode('overwrite')\
           .saveAsTable(target_table)


else :
    deltaTable = DeltaTable.forName(spark, target_table)
    (
    deltaTable.alias('target')
    .merge(
        dim_drivers_df.alias('source'),
        'source.driver_id=target.driver_id'
    ).whenMatchedUpdate(
        set = {
           'driver_name': 'source.driver_name',
            'date_of_birth'   : 'source.date_of_birth',
            'nationality'  : 'source.nationality',
            'region' : 'source.region',
            'updated_timestamp': 'source.updated_timestamp'
        
        }
    )
    .whenNotMatchedInsertAll()
    .execute() 
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validation

# COMMAND ----------

display(spark.table(target_table))