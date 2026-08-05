-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Set-up the project environment for Formula1 Project
-- MAGIC - Create External Location databricks-course-ext-dl1-formula1-incr
-- MAGIC - Create Catalog formula1_incr
-- MAGIC - Create Schemas landing, bronze, silver and gold
-- MAGIC - Create Volume Files in the landing schema

-- COMMAND ----------

select current_metastore();

-- COMMAND ----------

-- MAGIC %fs ls 'abfss://formula1-incr@databricksstorageaccou.dfs.core.windows.net/landing/'

-- COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS databricks_ex_loc_f1_incr
URL 'abfss://formula1-incr@databricksstorageaccou.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL databricks_f1_cred)


-- COMMAND ----------

show catalogs;

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS `formula1_project_incr` 
MANAGED LOCATION 'abfss://formula1-incr@databricksstorageaccou.dfs.core.windows.net/'

-- COMMAND ----------

use catalog `formula1_project_incr`

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS landing

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS bronze
MANAGED LOCATION 'abfss://formula1-incr@databricksstorageaccou.dfs.core.windows.net/bronze'

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS silver
MANAGED LOCATION 'abfss://formula1-incr@databricksstorageaccou.dfs.core.windows.net/silver'

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS gold
MANAGED LOCATION 'abfss://formula1-incr@databricksstorageaccou.dfs.core.windows.net/gold'

-- COMMAND ----------

show schemas;

-- COMMAND ----------

create external volume formula1_project_incr.landing.data_source
LOCATION 'abfss://formula1-incr@databricksstorageaccou.dfs.core.windows.net/landing'

-- COMMAND ----------

-- MAGIC %fs ls /Volumes/formula1_project/landing/data_source