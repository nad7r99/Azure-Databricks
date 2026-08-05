# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest constructors.json file

# COMMAND ----------

import pyspark.sql.functions as F

# COMMAND ----------

# MAGIC %run ../00.Common/1.environment-config

# COMMAND ----------

# MAGIC %run ../00.Common/2.bronze_helpers

# COMMAND ----------

source_file = f'{landing_folder_path}/drivers.json'
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

# MAGIC %md
# MAGIC ### write to bronze as delta table

# COMMAND ----------

drivers_df.write.format('delta')\
                     .mode('overwrite')\
                     .saveAsTable(table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validation Checks

# COMMAND ----------

display(spark.table(table_name))