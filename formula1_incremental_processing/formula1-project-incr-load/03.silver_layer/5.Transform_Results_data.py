# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

dbutils.widgets.text('p_batch_id', '')
v_batch_id = dbutils.widgets.get('p_batch_id')

# COMMAND ----------

# MAGIC %run ../00.Common/1.environment-config

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.results"
silver_table = f"{catalog_name}.{silver_schema}.results"

# COMMAND ----------

results_df = spark.table(bronze_table).filter(F.col('batch_id') == v_batch_id)
display(results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### keep the require columns for analytics only
# MAGIC ### Renaming columns to cleaner business **names**

# COMMAND ----------

results_df = results_df.select('constructorId', 
                               'date', 
                               'driverId',
                               'grid',
                               'laps',
                               'number', 
                               'points', 
                               'position', 
                               'positionText',
                               'raceName', 
                               'round',  
                               'season', 
                               'status', 
                               'ingestion_timestamp', 
                               'data_source',
                               'batch_id')


# COMMAND ----------

results_df = results_df.withColumnsRenamed({
                           'constructorId': 'constructor_id',
                           'driverId': 'driver_id',
                           'raceName':'race_name',
                           'positionText': 'position_text'                             
                                             })

# COMMAND ----------

# MAGIC %md
# MAGIC ### Handling Null values(Completeness)

# COMMAND ----------

# checks if there Null values or not 

results_df.select([
    F.count(F.when(F.col(col).isNull(), True)).alias(f'{col}_Missing')
    for col in results_df.columns]).display()

# COMMAND ----------

results_df = results_df.filter(F.col('round').isNotNull() &
                               F.col('season').isNotNull())

# COMMAND ----------

results_df.select([
    F.count(F.when(F.col(col).isNull(), True)).alias(f'{col}_Missing')
    for col in results_df.columns]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### remove duplicates (uniqueness)

# COMMAND ----------

#checks if there duplication or not
results_df.groupBy('driver_id','constructor_id','season','round').count().filter(F.col('count') > 1).display()

# COMMAND ----------

results_df = results_df.dropDuplicates(["driver_id", "constructor_id", 'season', 'round'])

# COMMAND ----------

# Validation
results_df.groupBy('driver_id','constructor_id','season','round').count().filter(F.col('count') > 1).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Standardization(consistency)

# COMMAND ----------

results_df = results_df.withColumn('race_name', F.initcap(F.col('race_name')))

display(results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add Columns

# COMMAND ----------

results_df = results_df.withColumn('created_timestamp', F.current_timestamp())\
                       .withColumn('updated_timestamp', F.current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write to silver schema 

# COMMAND ----------

from delta.tables import DeltaTable

if not spark.catalog.tableExists(silver_table):
     results_df.write\
           .format('delta')\
           .mode('overwrite')\
           .saveAsTable(silver_table)


else :
    deltaTable = DeltaTable.forName(spark, silver_table)
    (
    deltaTable.alias('target')
    .merge(
        results_df.alias('source'),
        'source.driver_id=target.driver_id and source.constructor_id=target.constructor_id and source.round=target.round and source.season=target.season'
    ).whenMatchedUpdate(
        condition='source.batch_id>=target.batch_id',
        set = {
           'date': 'source.date',
           'grid': 'source.grid',
           'laps': 'source.laps',
           'number': 'source.number',
           'points': 'source.points',
           'position': 'source.position',
           'position_text': 'source.position_text',
           'updated_timestamp'    : 'source.updated_timestamp',
           'race_name' : 'source.race_name',
           'status' : 'source.status'

        }
    )
    .whenNotMatchedInsertAll()
    .execute() 
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### validation

# COMMAND ----------

display(spark.table(silver_table))