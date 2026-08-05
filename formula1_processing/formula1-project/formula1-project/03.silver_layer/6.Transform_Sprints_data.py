# Databricks notebook source
# MAGIC %run ../00.Common/1.environment-config

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.sprints"
silver_table = f"{catalog_name}.{silver_schema}.sprints"

# COMMAND ----------

sprints_df = spark.table(bronze_table)
display(sprints_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### keep the require columns for analytics only
# MAGIC ### Renaming columns to cleaner business **names**

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

sprints_df = sprints_df.select('constructorId', 
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
                               'data_source')\
                       .withColumnsRenamed({
                           'constructorId':  'constructor_id',
                           'driverId':       'driver_id',
                           'raceName':       'race_name',
                           'positionText':   'position_text'
                           
                       })

display(sprints_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Handling Null values(Completeness)

# COMMAND ----------

# checks if there Null values or not 

sprints_df.select([
    F.count(F.when(F.col(col).isNull(), True)).alias(f'{col}_Missing')
    for col in sprints_df.columns]).display()

# COMMAND ----------

sprints_df = sprints_df.filter(F.col('round').isNotNull() &
                               F.col('season').isNotNull())

# COMMAND ----------

sprints_df.select([
    F.count(F.when(F.col(col).isNull(), True)).alias(f'{col}_Missing')
    for col in sprints_df.columns]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### remove duplicates (uniqueness)

# COMMAND ----------

#checks if there duplication or not
sprints_df.groupBy('driver_id','constructor_id','season','round').count().filter(F.col('count') > 1).display()

# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

window_spec = Window.partitionBy("driver_id", "constructor_id", 'season', 'round').orderBy(F.col("date").desc())

sprints_df = (
    sprints_df
    .withColumn("row_num", F.row_number().over(window_spec))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

# COMMAND ----------

# Validation
sprints_df.groupBy('driver_id','constructor_id','season','round').count().filter(F.col('count') > 1).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Standardization(consistency)

# COMMAND ----------

sprints_df = sprints_df.withColumn('race_name', F.initcap(F.col('race_name')))

display(sprints_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write to silver schema 

# COMMAND ----------

sprints_df.write\
        .format('delta')\
        .mode('overwrite')\
        .saveAsTable(silver_table)

# COMMAND ----------

# MAGIC %md
# MAGIC ### validation

# COMMAND ----------

display(spark.table(silver_table))