-- Databricks notebook source
create schema if not exists formula1_project.control
managed location 'abfss://formula1@databricksstorageaccou.dfs.core.windows.net/control'

-- COMMAND ----------

create table if not exists formula1_project.control.batch_events(
    batch_id int,
    batch_timestamp timestamp
)

-- COMMAND ----------

insert into formula1_project.control.batch_events 
values 
(1, current_timestamp())

-- COMMAND ----------

insert into formula1_project.control.batch_events 
values 
(2, current_timestamp())