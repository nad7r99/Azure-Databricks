# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

dbutils.widgets.text('p_batch_id', '')
v_batch_id = dbutils.widgets.get('p_batch_id')

# COMMAND ----------

# MAGIC %run ../00.Common/1.environment-config

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.sprints"
silver_table = f"{catalog_name}.{silver_schema}.sprints"

# COMMAND ----------

sprints_df = spark.table(bronze_table).filter(F.col('batch_id') == v_batch_id)
display(sprints_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### keep the require columns for analytics only
# MAGIC ### Renaming columns to cleaner business **names**

# COMMAND ----------

sprints_df = sprints_df.select('constructorId', 
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
                               'batch_id')\
                       .withColumnsRenamed({
                           'constructorId':  'constructor_id',
                           'driverId':       'driver_id',
                           'raceName':       'race_name',
                           'positionText':   'position_text'
                           
                       })

display(sprints_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Handling Null values(Completeness)

# COMMAND ----------

# checks if there Null values or not 

sprints_df.select([
    F.count(F.when(F.col(col).isNull(), True)).alias(f'{col}_Missing')
    for col in sprints_df.columns]).display()

# COMMAND ----------

sprints_df = sprints_df.filter(F.col('round').isNotNull() &
                               F.col('season').isNotNull())

# COMMAND ----------

sprints_df.select([
    F.count(F.when(F.col(col).isNull(), True)).alias(f'{col}_Missing')
    for col in sprints_df.columns]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### remove duplicates (uniqueness)

# COMMAND ----------

#checks if there duplication or not
sprints_df.groupBy('driver_id','constructor_id','season','round').count().filter(F.col('count') > 1).display()

# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

window_spec = Window.partitionBy("driver_id", "constructor_id", 'season', 'round').orderBy(F.col("date").desc())

sprints_df = (
    sprints_df
    .withColumn("row_num", F.row_number().over(window_spec))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

# COMMAND ----------

# Validation
sprints_df.groupBy('driver_id','constructor_id','season','round').count().filter(F.col('count') > 1).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Standardization(consistency)

# COMMAND ----------

sprints_df = sprints_df.withColumn('race_name', F.initcap(F.col('race_name')))

display(sprints_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add Columns

# COMMAND ----------

sprints_df = sprints_df.withColumn('created_timestamp', F.current_timestamp())\
                       .withColumn('updated_timestamp', F.current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write to silver schema 

# COMMAND ----------

from delta.tables import DeltaTable

if not spark.catalog.tableExists(silver_table):
     sprints_df.write\
           .format('delta')\
           .mode('overwrite')\
           .saveAsTable(silver_table)


else :
    deltaTable = DeltaTable.forName(spark, silver_table)
    (
    deltaTable.alias('target')
    .merge(
        sprints_df.alias('source'),
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