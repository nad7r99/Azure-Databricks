# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

dbutils.widgets.text('p_batch_id', '')
v_batch_id = dbutils.widgets.get('p_batch_id')

# COMMAND ----------

# MAGIC %run ../00.Common/1.environment-config

# COMMAND ----------

target_table =  f'{catalog_name}.{gold_schema}.dim_constructors'

# COMMAND ----------

# MAGIC %md
# MAGIC ### read resource table

# COMMAND ----------

constructors_df = spark.table(f'{catalog_name}.{silver_schema}.constructors').filter(F.col('batch_id') == v_batch_id)


region_df = spark.table(f'{catalog_name}.{gold_schema}.ref_nationality_region')

# COMMAND ----------

# MAGIC %md
# MAGIC ### join

# COMMAND ----------

dim_constructors_df = constructors_df.join(region_df, 
                                           on='nationality', 
                                           how='left') 


display(dim_constructors_df)                                           

# COMMAND ----------

dim_constructors_df = dim_constructors_df.select(
                                                 F.col('constructor_id'), 
                                                 F.col('constructor_name'), 
                                                 F.col('nationality'), 
                                                 F.col('region')
                                            )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add Columns

# COMMAND ----------

dim_constructors_df = dim_constructors_df.withColumn('created_timestamp', F.current_timestamp())\
                                         .withColumn('updated_timestamp', F.current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ### write to gold_layer table 

# COMMAND ----------

from delta import DeltaTable

if not spark.catalog.tableExists(target_table):
     dim_constructors_df.write\
           .format('delta')\
           .mode('overwrite')\
           .saveAsTable(target_table)


else :
    deltaTable = DeltaTable.forName(spark, target_table)
    (
    deltaTable.alias('target')
    .merge(
        dim_constructors_df.alias('source'),
        'source.constructor_id=target.constructor_id'
    ).whenMatchedUpdate(
        set = {
           'constructor_name': 'source.constructor_name',
            'nationality'   : 'source.nationality',
            'region'  : 'source.region',
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