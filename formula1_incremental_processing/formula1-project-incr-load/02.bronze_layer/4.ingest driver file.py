# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest constructors.json file

# COMMAND ----------

dbutils.widgets.text('p_batch_id', '')
v_batch_id = dbutils.widgets.get('p_batch_id')

# COMMAND ----------

import pyspark.sql.functions as F

# COMMAND ----------

# MAGIC %run ../00.Common/1.environment-config

# COMMAND ----------

# MAGIC %run ../00.Common/2.bronze_helpers

# COMMAND ----------

source_file = f'{landing_folder_path}/{v_batch_id}/drivers.json'
table_name = f'{catalog_name}.{bronze_schema}.drivers'

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType

# COMMAND ----------

name_schmea = StructType([
    StructField('givenName',  StringType()),
    StructField('familyName', StringType())
])

drivers_schema = StructType([
    StructField('driverId',    StringType()),
    StructField('name',        name_schmea),
    StructField('dateOfBirth', DateType()),
    StructField('nationality', StringType()),
    StructField('url',         StringType())
])

# COMMAND ----------

# MAGIC %md
# MAGIC ### read the data

# COMMAND ----------

drivers_df = spark.read.format('json')\
                            .schema(drivers_schema)\
                            .option('mode', 'FailFast')\
                            .load(source_file)    

# COMMAND ----------

# MAGIC %md
# MAGIC ### Adding metadata columns

# COMMAND ----------

drivers_df = Add_metadata(drivers_df)

# COMMAND ----------

drivers_df = drivers_df.withColumn('batch_id', F.lit(v_batch_id))

# COMMAND ----------

# MAGIC %md
# MAGIC ### write to bronze as delta table

# COMMAND ----------

drivers_df.write.format('delta')\
                     .mode('overwrite')\
                     .partitionBy('batch_id')\
                     .option('replaceWhere', f"batch_id = '{v_batch_id}'")\
                     .saveAsTable(table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validation Checks

# COMMAND ----------

display(spark.table(table_name))