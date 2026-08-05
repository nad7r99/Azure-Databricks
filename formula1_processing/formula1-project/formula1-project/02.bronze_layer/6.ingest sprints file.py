# Databricks notebook source
# MAGIC %md
# MAGIC ### ingest results file

# COMMAND ----------

# MAGIC %run ../00.Common/1.environment-config

# COMMAND ----------

# MAGIC %run ../00.Common/2.bronze_helpers

# COMMAND ----------

source_file = f'{landing_folder_path}/sprints/'
table_name = f'{catalog_name}.{bronze_schema}.sprints'

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, FloatType

# COMMAND ----------

sprints_schema = StructType([
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

sprints_df = spark.read.format('json')\
                       .option('mode', 'FailFast')\
                       .schema(sprints_schema)\
                       .option('multiLine', True)\
                       .load(source_file)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Adding metadata columns

# COMMAND ----------

sprints_df = Add_metadata(sprints_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### write to bronze as delta table

# COMMAND ----------

sprints_df.write.format('delta')\
                     .mode('overwrite')\
                     .saveAsTable(table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validation Checks

# COMMAND ----------

display(spark.table(table_name))