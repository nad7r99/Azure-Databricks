# Databricks notebook source
# MAGIC %run ../00.Common/1.environment-config

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.races"
silver_table = f"{catalog_name}.{silver_schema}.races"

# COMMAND ----------

races_df = spark.table(bronze_table)
display(races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## keep the require columns for analytics only.``

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

races_df = races_df.select(
    F.col('season'),
    F.col('round'),
    F.col('raceName'),
    F.col('date'),
    F.col('circuitId'),
    F.col('ingestion_timestamp'),
    F.col('data_source')
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Renaming columns to cleaner business **names**

# COMMAND ----------

races_df = races_df.withColumnsRenamed({
    'circuitId': 'circuit_id',
    'raceName':'race_name',
    'date':'race_date'
})

display(races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Handling Null values(Completeness)

# COMMAND ----------

# checks if there Null values or not 

races_df.select([
    F.count(F.when(F.col(col).isNull(), True)).alias(f'{col}_Missing')
    for col in races_df.columns]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### remove duplicates (uniqueness)

# COMMAND ----------

#checks if there duplication or not
races_df.groupBy('season', 'round').count().filter(F.col('count') > 1).display()

# COMMAND ----------

races_df = races_df.dropDuplicates(['season', 'round'])

# COMMAND ----------

# Validation
races_df.groupBy('season', 'round').count().filter(F.col('count') > 1).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Standardization(consistency)

# COMMAND ----------

races_df = races_df.withColumn('race_name', F.initcap(F.col('race_name')))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write to silver schema 

# COMMAND ----------

races_df.write\
        .format('delta')\
        .mode('overwrite')\
        .saveAsTable(silver_table)

# COMMAND ----------

# MAGIC %md
# MAGIC ### validation

# COMMAND ----------

display(spark.table(silver_table))