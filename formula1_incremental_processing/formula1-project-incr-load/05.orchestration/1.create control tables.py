# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC create schema if not exists formula1_project_incr.control;
# MAGIC
# MAGIC create table if not exists formula1_project_incr.control.batch_control(
# MAGIC
# MAGIC     batch_id string,
# MAGIC     status string,
# MAGIC     created_timestamp timestamp,
# MAGIC     updated_timestamp timestamp
# MAGIC )
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from formula1_project_incr.control.batch_control

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from formula1_project_incr.control.batch_control