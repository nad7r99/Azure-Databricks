# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

dbutils.widgets.text('p_batch_id', '')
v_batch_id = dbutils.widgets.get('p_batch_id')

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
         .drop('race_name', 'race_date', 'ingestion_timestamp', 'data_source', 'batch_id')\
         .filter(F.col('batch_id') == v_batch_id)
    
           )

# COMMAND ----------

sprints_df = (
    
    spark.table(f'{catalog_name}.{silver_schema}.sprints')\
         .withColumn('session_type', F.lit('SPRINTS'))\
         .drop('race_name', 'race_date', 'ingestion_timestamp', 'data_source', 'batch_id' )\
         .filter(F.col('batch_id') == v_batch_id)
    
    
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

from delta import DeltaTable

if not spark.catalog.tableExists(target_table):
     result_sprints_df.write\
           .format('delta')\
           .mode('overwrite')\
           .saveAsTable(target_table)


else :
    deltaTable = DeltaTable.forName(spark, target_table)
    (
    deltaTable.alias('target')
    .merge(
        result_sprints_df.alias('source'),
        'source.driver_id=target.driver_id and source.round=target.round and source.season=target.season and source.session_type=target.session_type and source.constructor_id=target.constructor_id'
    
    
    ).whenMatchedUpdate(
        set = {
           'date': 'source.date',
            'grid'   : 'source.grid',
            'number' : 'source.number',
            'laps' : 'source.laps',
            'position' : 'source.position',
            'position_text' : 'source.position_text',
            'status' : 'source.status',
            'is_win' :'source.is_win',
            'is_podium' : 'source.is_podium',
            'has_points' : 'source.has_points',
            'points' : 'source.points',
            'updated_timestamp' : 'source.updated_timestamp'
        }
    ).whenNotMatchedInsertAll(

    ).execute())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validation

# COMMAND ----------

display(spark.table(target_table))