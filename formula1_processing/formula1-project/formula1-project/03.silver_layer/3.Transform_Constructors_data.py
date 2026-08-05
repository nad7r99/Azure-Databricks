# Databricks notebook source
# MAGIC %run ../00.Common/1.environment-config

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.constructors"
silver_table = f"{catalog_name}.{silver_schema}.constructors"

# COMMAND ----------

constructor_df = spark.table(bronze_table)
display(constructor_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## keep the require columns for analytics only.

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

constructor_df = constructor_df.drop('url')
display(constructor_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Renaming columns to cleaner business **names**

# COMMAND ----------

constructor_df = constructor_df.withColumnsRenamed({
    'constructorId': 'constructor_id',
    'name':'constructor_name'
})

display(constructor_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Handling Null values(Completeness)

# COMMAND ----------

# checks if there Null values or not 

constructor_df.select([
    F.count(F.when(F.col(col).isNull(), True)).alias(f'{col}_Missing')
    for col in constructor_df.columns]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### remove duplicates (uniqueness)

# COMMAND ----------

#checks if there duplication or not
constructor_df.groupBy('constructor_id',).count().filter(F.col('count') > 1).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Standardization(consistency)

# COMMAND ----------

constructor_df = constructor_df.withColumn('nationality', F.initcap(F.col('nationality')))
display(constructor_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write to silver schema 

# COMMAND ----------

constructor_df.write\
        .format('delta')\
        .mode('overwrite')\
        .saveAsTable(silver_table)

# COMMAND ----------

# MAGIC %md
# MAGIC ### validation

# COMMAND ----------

display(spark.table(silver_table))