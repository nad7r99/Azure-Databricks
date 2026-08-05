# Databricks notebook source
Notebook_path = [
      '/Workspace/Databricks_course/formula1-project/02.bronze_layer/1.ingest circuits file',
      '/Workspace/Databricks_course/formula1-project/02.bronze_layer/2.ingest races file',
      '/Workspace/Databricks_course/formula1-project/02.bronze_layer/3.ingest Constructors file',
      '/Workspace/Databricks_course/formula1-project/02.bronze_layer/4.ingest driver file',
      '/Workspace/Databricks_course/formula1-project/02.bronze_layer/5.ingest results file',
      '/Workspace/Databricks_course/formula1-project/02.bronze_layer/6.ingest sprints file'
]

# COMMAND ----------


success_list = []
failed_list = []

for nb in Notebook_path:
   try :
        dbutils.notebook.run(nb, timeout_seconds=0)
        print(f"Successfully finished {nb}")
        success_list.append(nb)

   except Exception as e :
        print(f"Failed to run {nb}")
        failed_list.append(nb)
        raise e
    
print(f"Success list : {success_list}")
print(f"Failed list : {failed_list}")
             