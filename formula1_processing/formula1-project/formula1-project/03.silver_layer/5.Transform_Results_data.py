# Databricks notebook source
# MAGIC %run ../00.Common/1.environment-config

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.results"
silver_table = f"{catalog_name}.{silver_schema}.results"

# COMMAND ----------

results_df = spark.table(bronze_table)
display(results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### keep the require columns for analytics only
# MAGIC ### Renaming columns to cleaner business **names**

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

results_df = results_df.select('constructorId', 
                               'date', 
                               'driverId',
                               'grid',
                               'laps',
                               'number', 
                               'points', 
                               'position', 
                               'positionText',
                               'raceName', 
                               'round',  
                               'season', 
                               'status', 
                               'ingestion_timestamp', 
                               'data_source')


# COMMAND ----------

results_df = results_df.withColumnsRenamed({
                           'constructorId': 'constructor_id',
                           'driverId': 'driver_id',
                           'raceName':'race_name',
                           'positionText': 'position_text'                             
                                             })

# COMMAND ----------

# MAGIC %md
# MAGIC ### Handling Null values(Completeness)

# COMMAND ----------

# checks if there Null values or not 

results_df.select([
    F.count(F.when(F.col(col).isNull(), True)).alias(f'{col}_Missing')
    for col in results_df.columns]).display()

# COMMAND ----------

results_df = results_df.filter(F.col('round').isNotNull() &
                               F.col('season').isNotNull())

# COMMAND ----------

results_df.select([
    F.count(F.when(F.col(col).isNull(), True)).alias(f'{col}_Missing')
    for col in results_df.columns]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### remove duplicates (uniqueness)

# COMMAND ----------

#checks if there duplication or not
results_df.groupBy('driver_id','constructor_id','season','round').count().filter(F.col('count') > 1).display()

# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

results_df = results_df.dropDuplicates(["driver_id", "constructor_id", 'season', 'round'])

# COMMAND ----------

# Validation
results_df.groupBy('driver_id','constructor_id','season','round').count().filter(F.col('count') > 1).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Standardization(consistency)

# COMMAND ----------

results_df = results_df.withColumn('race_name', F.initcap(F.col('race_name')))

display(results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write to silver schema 

# COMMAND ----------

results_df.write\
        .format('delta')\
        .mode('overwrite').saveAsTable(silver_table)

# COMMAND ----------

# MAGIC %md
# MAGIC ### validation

# COMMAND ----------

display(spark.table(silver_table))