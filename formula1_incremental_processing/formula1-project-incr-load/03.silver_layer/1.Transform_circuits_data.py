# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

dbutils.widgets.text('p_batch_id', '')
v_batch_id = dbutils.widgets.get('p_batch_id')

# COMMAND ----------

# MAGIC %run ../00.Common/1.environment-config

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.circuits"
silver_table = f"{catalog_name}.{silver_schema}.circuits"

# COMMAND ----------

silver_table

# COMMAND ----------

circuits_df = spark.table(bronze_table).filter(F.col('batch_id') == v_batch_id)
display(circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## keep the require columns for analytics only.

# COMMAND ----------

circuits_df = circuits_df.select(
    F.col('circuitId'),
    F.col('circuitName'),
    F.col('lat'),
    F.col('long'),
    F.col('locality'),
    F.col('country'),
    F.col('ingestion_timestamp'),
    F.col('data_source'),
    F.col('batch_id')
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Renaming columns to cleaner business **names**

# COMMAND ----------

circuits_df = circuits_df.withColumnsRenamed({
    'circuitId':   'circuit_id',
    'circuitName': 'circuit_name',
    'lat':         'latitude',
    'long':        'longitude'
})

    


# COMMAND ----------

# MAGIC %md
# MAGIC ### Handling Null values(Completeness)

# COMMAND ----------

# checks if there Null values or not 
circuits_df.select([
    F.count(F.when(F.col(col).isNull(), True)).alias(f'{col}_Missing')
    for col in circuits_df.columns]).display()

# COMMAND ----------

# filtering by circuit_id cause will be use in join

circuits_df = circuits_df.filter(F.col('circuit_id').isNotNull())

# COMMAND ----------

# Validation 

circuits_df.select([
    F.count(F.when(F.col(col).isNull(), True)).alias(f'{col}_Missing')
    for col in circuits_df.columns]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### remove duplicates (uniqueness)

# COMMAND ----------

#checks if there duplication or not
circuits_df.groupBy('circuit_id').count().filter(F.col('count') > 1).display()

# COMMAND ----------

circuits_df = circuits_df.dropDuplicates(['circuit_id'])

# COMMAND ----------

# Validation 

circuits_df.groupBy('circuit_id').count().filter(F.col('count') > 1).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Standardization(consistency)

# COMMAND ----------

circuits_df = circuits_df.withColumn('circuit_name', F.initcap(F.col('circuit_name')))\
                         .withColumn('locality', F.initcap(F.col('locality')))

display(circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add Columns

# COMMAND ----------

circuits_df = circuits_df.withColumn('created_timestamp', F.current_timestamp())\
                         .withColumn('updated_timestamp', F.current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write to silver schema 

# COMMAND ----------

# circuits_df.write\
#            .format('delta')\
#            .mode('overwrite')\
#            .saveAsTable(silver_table)

# COMMAND ----------

# MAGIC %md
# MAGIC ### MERGE()

# COMMAND ----------

from delta import DeltaTable

if not spark.catalog.tableExists(silver_table):
     circuits_df.write\
           .format('delta')\
           .mode('overwrite')\
           .saveAsTable(silver_table)


else :
    deltaTable = DeltaTable.forName(spark, silver_table)
    (
    deltaTable.alias('target')
    .merge(
        circuits_df.alias('source'),
        'target.circuit_id = source.circuit_id'
    ).whenMatchedUpdate(
        condition='source.batch_id>=target.batch_id',
        set = {
           'circuit_name': 'source.circuit_name',
            'latitude'   : 'source.latitude',
            'longitude'  : 'source.longitude',
            'locality'   : 'source.locality',
            'country'    : 'source.country',
            'ingestion_timestamp'  :  'source.ingestion_timestamp',
            'data_source' : 'source.data_source',
            'batch_id': 'source.batch_id',
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

display(spark.table(silver_table))