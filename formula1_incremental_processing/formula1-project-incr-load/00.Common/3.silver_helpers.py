# Databricks notebook source
from pyspark.sql import functions as F
from delta.tables import DeltaTable

def write_to_silver(
    input_df, 
    target_table, 
    merge_condition,
    columns_to_update):

    final_df = (
        input_df
        .withColumn("created_timestamp", F.current_timestamp())
        .withColumn("updated_timestamp", F.current_timestamp())
    )
    
    if not spark.catalog.tableExists(target_table):
        final_df.write\
                .mode('overwrite')\
                .format("delta")\
                .saveAsTable(target_table)


    else:
        delta_table = DeltaTable.forName(spark, target_table)
        
        update_dict = {col: f"source.{col}" for col in columns_to_update}
        update_dict["updated_timestamp"] = "source.updated_timestamp"
        
        (
            delta_table.alias("target")
            .merge(final_df.alias("source"), merge_condition)
            .whenMatchedUpdate(
                condition="source.batch_id >= target.batch_id",
                set=update_dict

            )
            .whenNotMatchedInsertAll()
            .execute()
        )