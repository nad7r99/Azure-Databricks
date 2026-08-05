# Databricks notebook source
# MAGIC %md
# MAGIC ### ingest results file

# COMMAND ----------

dbutils.widgets.text('p_batch_id', '')
v_batch_id = dbutils.widgets.get('p_batch_id')

# COMMAND ----------

# MAGIC %run ../00.Common/1.environment-config

# COMMAND ----------

# MAGIC %run ../00.Common/2.bronze_helpers

# COMMAND ----------

source_file = f'{landing_folder_path}/{v_batch_id}/results/'
table_name = f'{catalog_name}.{bronze_schema}.results'

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, FloatType

# COMMAND ----------

results_schema = StructType([
    StructField('constructorId', StringType()),
    StructField('date',          DateType()),
    StructField('driverId',      StringType()),
    StructField('grid',          IntegerType()),
    StructField('laps',          IntegerType()),
    StructField('number',        IntegerType()),
    StructField('points',        FloatType()),
    StructField('position',      IntegerType()),
    StructField('positionText',  StringType()),
    StructField('raceName',      StringType()),
    StructField('round',         IntegerType()),
    StructField('season',        IntegerType()),
    StructField('status',        StringType()),
    StructField('url',           StringType())
   
                
])

# COMMAND ----------

results_df = spark.read.format('json')\
                       .option('mode', 'FailFast')\
                       .schema(results_schema)\
                       .load(source_file)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Adding metadata columns

# COMMAND ----------

results_df = Add_metadata(results_df)

# COMMAND ----------

results_df = results_df.withColumn('batch_id', F.lit(v_batch_id))

# COMMAND ----------

# MAGIC %md
# MAGIC ### write to bronze as delta table

# COMMAND ----------

results_df.write.format('delta')\
                     .mode('overwrite')\
                     .partitionBy('batch_id')\
                     .option('replaceWhere', f"batch_id = '{v_batch_id}'")\
                     .saveAsTable(table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validation Checks

# COMMAND ----------

display(spark.table(table_name))