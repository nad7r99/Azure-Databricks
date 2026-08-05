# Databricks notebook source
# MAGIC %run ../00.Common/1.environment-config

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.drivers"
silver_table = f"{catalog_name}.{silver_schema}.drivers"

# COMMAND ----------

drivers_df = spark.table(bronze_table)
display(drivers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## keep the require columns for analytics only.

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

drivers_df = drivers_df.drop('url')
display(drivers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Renaming columns to cleaner business **names**

# COMMAND ----------

drivers_df = drivers_df.withColumnsRenamed({
    'driverID': 'driver_id',
    'dateOfBirth': 'date_of_birth'
})

display(drivers_df)

# COMMAND ----------

drivers_df = drivers_df.withColumn('driver_name', F.concat_ws(' ', F.col('name.givenName'), F.col('name.familyName')))

# COMMAND ----------

drivers_df = drivers_df.select('driver_id', 'driver_name', 'date_of_birth', 'nationality', 'ingestion_timestamp','data_source')

display(drivers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Handling Null values(Completeness)

# COMMAND ----------

# checks if there Null values or not 

drivers_df.select([
    F.count(F.when(F.col(col).isNull(), True)).alias(f'{col}_Missing')
    for col in drivers_df.columns]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### remove duplicates (uniqueness)

# COMMAND ----------

#checks if there duplication or not
drivers_df.groupBy('driver_id',).count().filter(F.col('count') > 1).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Standardization(consistency)

# COMMAND ----------

drivers_df = drivers_df.withColumn('nationality', F.initcap(F.col('nationality')))\
                       .withColumn('driver_name', F.initcap(F.col('driver_name')))

display(drivers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write to silver schema 

# COMMAND ----------

drivers_df.write\
        .format('delta')\
        .mode('overwrite')\
        .saveAsTable(silver_table)

# COMMAND ----------

# MAGIC %md
# MAGIC ### validation

# COMMAND ----------

display(spark.table(silver_table))