# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

dbutils.widgets.text('p_batch_id', '')
v_batch_id = dbutils.widgets.get('p_batch_id')

# COMMAND ----------

# MAGIC %run ../00.Common/1.environment-config

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.drivers"
silver_table = f"{catalog_name}.{silver_schema}.drivers"

# COMMAND ----------

drivers_df = spark.table(bronze_table).filter(F.col('batch_id') == v_batch_id)
display(drivers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## keep the require columns for analytics only.

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

drivers_df = drivers_df.drop('url')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Renaming columns to cleaner business **names**

# COMMAND ----------

drivers_df = drivers_df.withColumnsRenamed({
    'driverID': 'driver_id',
    'dateOfBirth': 'date_of_birth'
})

display(drivers_df)

# COMMAND ----------

drivers_df = drivers_df.withColumn('driver_name', F.concat_ws(' ', F.col('name.givenName'), F.col('name.familyName')))

# COMMAND ----------

drivers_df = drivers_df.select('driver_id', 
                               'driver_name', 
                               'date_of_birth', 
                               'nationality', 
                               'ingestion_timestamp',
                               'data_source',
                               'batch_id')

display(drivers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Handling Null values(Completeness)

# COMMAND ----------

# checks if there Null values or not 

drivers_df.select([
    F.count(F.when(F.col(col).isNull(), True)).alias(f'{col}_Missing')
    for col in drivers_df.columns]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### remove duplicates (uniqueness)

# COMMAND ----------

#checks if there duplication or not
drivers_df.groupBy('driver_id',).count().filter(F.col('count') > 1).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Standardization(consistency)

# COMMAND ----------

drivers_df = drivers_df.withColumn('nationality', F.initcap(F.col('nationality')))\
                       .withColumn('driver_name', F.initcap(F.col('driver_name')))

display(drivers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add Columns

# COMMAND ----------

drivers_df = drivers_df.withColumn('created_timestamp', F.current_timestamp())\
                       .withColumn('updated_timestamp', F.current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write to silver schema 

# COMMAND ----------

from delta.tables import DeltaTable

if not spark.catalog.tableExists(silver_table):
     drivers_df.write\
           .format('delta')\
           .mode('overwrite')\
           .saveAsTable(silver_table)


else :
    deltaTable = DeltaTable.forName(spark, silver_table)
    (
    deltaTable.alias('target')
    .merge(
        drivers_df.alias('source'),
        'source.driver_id=target.driver_id'
    ).whenMatchedUpdate(
        condition='source.batch_id>=target.batch_id',
        set = {
           'driver_name': 'source.driver_name',
           'date_of_birth' : 'source.date_of_birth',
           'nationality'   : 'source.nationality',
           'updated_timestamp'    : 'source.updated_timestamp'

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