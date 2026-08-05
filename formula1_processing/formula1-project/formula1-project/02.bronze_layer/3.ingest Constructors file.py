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

source_file = f'{landing_folder_path}/constructors.json'
table_name = f'{catalog_name}.{bronze_schema}.constructors'

# COMMAND ----------

constructors_schema = '''
           
            constructorId STRING,
            name          STRING,
            nationality   STRING,
            url           STRING
'''

# COMMAND ----------

constructors_df = spark.read.format('json')\
                            .schema(constructors_schema)\
                            .option('mode', 'FailFast')\
                            .load(source_file)    

# COMMAND ----------

# MAGIC %md
# MAGIC ### Adding metadata columns

# COMMAND ----------

constructors_df = Add_metadata(constructors_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### write to bronze as delta table

# COMMAND ----------

constructors_df.write.format('delta')\
                     .mode('overwrite')\
                     .saveAsTable(table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validation Checks

# COMMAND ----------

display(spark.table(table_name))