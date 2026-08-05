# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

dbutils.widgets.text('p_batch_id', '')
v_batch_id = dbutils.widgets.get('p_batch_id')

# COMMAND ----------

# MAGIC %run ../00.Common/1.environment-config

# COMMAND ----------

target_table = f'{catalog_name}.{gold_schema}.dim_races'

# COMMAND ----------

# MAGIC %md
# MAGIC ### read resource tables

# COMMAND ----------

circuits_df = spark.table(f'{catalog_name}.{silver_schema}.circuits').filter(F.col('batch_id') == v_batch_id)

# COMMAND ----------

races_df = spark.table(f'{catalog_name}.{silver_schema}.races').filter(F.col('batch_id') == v_batch_id)

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
# MAGIC ### Add Columns

# COMMAND ----------

dim_races_df = dim_races_df.withColumn('created_timestamp', F.current_timestamp())\
                           .withColumn('updated_timestamp', F.current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ### write to gold_layer as dim_races Table

# COMMAND ----------

from delta import DeltaTable

if not spark.catalog.tableExists(target_table):
     dim_races_df.write\
           .format('delta')\
           .mode('overwrite')\
           .saveAsTable(target_table)


else :
    deltaTable = DeltaTable.forName(spark, target_table)
    (
    deltaTable.alias('target')
    .merge(
        dim_races_df.alias('source'),
        'source.season=target.season and source.round=target.round'
    ).whenMatchedUpdate(
        set = {
           'race_name': 'source.race_name',
            'race_date'   : 'source.race_date',
            'circuit_name'  : 'source.circuit_name',
            'locality'   : 'source.locality',
            'country'    : 'source.country',
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