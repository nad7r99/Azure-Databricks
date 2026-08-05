# Databricks notebook source
Notebook_paths = [
    '/Workspace/Databricks_course/formula1-project/03.silver_layer/1.Transform_circuits_data',
    '/Workspace/Databricks_course/formula1-project/03.silver_layer/2.Transfrom_Races_data',
    '/Workspace/Databricks_course/formula1-project/03.silver_layer/3.Transform_Constructors_data',
    '/Workspace/Databricks_course/formula1-project/03.silver_layer/4.Transform _drivers_data',
    '/Workspace/Databricks_course/formula1-project/03.silver_layer/5.Transform_Results_data',
    '/Workspace/Databricks_course/formula1-project/03.silver_layer/6.Transform_Sprints_data'

]


# COMMAND ----------


success_list = []
failed_list = []

for nb in Notebook_paths:
   try :
        dbutils.notebook.run(nb, timeout_seconds=0)
        print(f"Successfully finished {nb} \n")
        success_list.append(nb)

   except Exception as e :
        print(f"Failed to run {nb} \n")
        failed_list.append(nb)
         
    
print(f"Success list : {success_list} \n")
print(f"Failed list : {failed_list} \n")