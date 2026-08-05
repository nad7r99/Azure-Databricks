# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest races.csv file

# COMMAND ----------

dbutils.widgets.text('p_batch_id', '')
v_batch_id = dbutils.widgets.get('p_batch_id')

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType
import pyspark.sql.functions as F

# COMMAND ----------

# MAGIC %run ../00.Common/1.environment-config

# COMMAND ----------

races_schema = StructType([
    StructField('season',        IntegerType(), True),
    StructField('round',         IntegerType(), True),
    StructField('url',           StringType(), True),
    StructField('raceName',      StringType(), True),
    StructField('date',          DateType(), True),
    StructField('circuitId',     StringType(), True)

])



# COMMAND ----------

source_file = f'{landing_folder_path}/{v_batch_id}/races.csv'
target_table = f'{catalog_name}.{bronze_schema}.races'

# COMMAND ----------

races_df = spark.read.format('csv')\
                .option('header','true')\
                .option('mode', 'FailFast')\
                .schema(races_schema)\
                .load(source_file)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Adding Metadata Columns

# COMMAND ----------

# MAGIC %run ../00.Common/2.bronze_helpers

# COMMAND ----------

races_df = Add_metadata(races_df)

# COMMAND ----------

races_df = races_df.withColumn('batch_id', F.lit(v_batch_id))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Wirte to bronze (delta table)

# COMMAND ----------

races_df.write.format('delta')\
              .mode('overwrite')\
              .partitionBy('batch_id')\
              .option('replaceWhere', f"batch_id = '{v_batch_id}'")\
              .saveAsTable(target_table)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validation Checks

# COMMAND ----------

display(spark.table(f'{catalog_name}.{bronze_schema}.races'))