# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest circuits.csv file

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
import pyspark.sql.functions as F

# COMMAND ----------

# MAGIC %run ../00.Common/1.environment-config

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
                        .load(f'{landing_folder_path}/circuits.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Adding Metadata Columns

# COMMAND ----------

# MAGIC %run ../00.Common/2.bronze_helpers

# COMMAND ----------

circuits_df = Add_metadata(circuits_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Wirte to bronze (delta table)

# COMMAND ----------

circuits_df.write.format('delta')\
                 .mode('overwrite')\
                 .saveAsTable(f'{catalog_name}.{bronze_schema}.circuits')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validation Checks

# COMMAND ----------

display(spark.table(f'{catalog_name}.{bronze_schema}.circuits'))