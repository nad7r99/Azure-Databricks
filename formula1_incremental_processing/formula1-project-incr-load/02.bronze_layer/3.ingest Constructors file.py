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

source_file = f'{landing_folder_path}/{v_batch_id}/constructors.json'
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

constructors_df = constructors_df.withColumn('batch_id', F.lit(v_batch_id))

# COMMAND ----------

# MAGIC %md
# MAGIC ### write to bronze as delta table

# COMMAND ----------

constructors_df.write.format('delta')\
                     .mode('overwrite')\
                     .partitionBy('batch_id')\
                     .option('replaceWhere', f"batch_id = '{v_batch_id}'")\
                     .saveAsTable(table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validation Checks

# COMMAND ----------

display(spark.table(table_name))