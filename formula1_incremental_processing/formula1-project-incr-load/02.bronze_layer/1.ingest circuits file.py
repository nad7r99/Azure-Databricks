# Databricks notebook source
dbutils.widgets.text('p_batch_id', '')
v_batch_id =dbutils.widgets.get('p_batch_id')

# COMMAND ----------

display(v_batch_id)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ingest circuits.csv file

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
import pyspark.sql.functions as F

# COMMAND ----------

# MAGIC %run ../00.Common/1.environment-config

# COMMAND ----------

source_file = f'{landing_folder_path}/{v_batch_id}/circuits.csv'
target_table = f'{catalog_name}.{bronze_schema}.circuits'

# COMMAND ----------

target_table

# COMMAND ----------

circuits_schema = StructType([
    StructField('circuitId',     StringType(), True),
    StructField('url',           StringType(), True),
    StructField('circuitName',   StringType(), True),
    StructField('lat',           DoubleType(), True),
    StructField('long',          DoubleType(), True),
    StructField('locality',      StringType(), True),
    StructField('country',       StringType(), True)

])



# COMMAND ----------

circuits_df = spark.read.format('csv')\
                        .option('header','true')\
                        .option('mode', 'FailFast')\
                        .schema(circuits_schema)\
                        .load(source_file)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Adding Metadata Columns

# COMMAND ----------

# MAGIC %run ../00.Common/2.bronze_helpers

# COMMAND ----------

circuits_df = Add_metadata(circuits_df)


# COMMAND ----------

circuits_df = circuits_df.withColumn('batch_id', F.lit(v_batch_id))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Wirte to bronze (delta table)

# COMMAND ----------

circuits_df.write.format('delta')\
                 .mode('overwrite')\
                 .partitionBy('batch_id')\
                 .option('replaceWhere', f"batch_id = '{v_batch_id}'")\
                 .saveAsTable(f'{catalog_name}.{bronze_schema}.circuits')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validation Checks

# COMMAND ----------

display(spark.table(f'{catalog_name}.{bronze_schema}.circuits'))