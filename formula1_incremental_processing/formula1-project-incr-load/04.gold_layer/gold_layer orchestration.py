# Databricks notebook source
Notebook_paths = [
    '/Workspace/Databricks_course/formula1-project/04.gold_layer/1.Build Races dim',
    '/Workspace/Databricks_course/formula1-project/04.gold_layer/2.Build Constructors dim',
    '/Workspace/Databricks_course/formula1-project/04.gold_layer/3.Region references',
    '/Workspace/Databricks_course/formula1-project/04.gold_layer/4.Build Drivers dim',
    '/Workspace/Databricks_course/formula1-project/04.gold_layer/5.Build Session fact'

]

# COMMAND ----------

success_list = []
failed_list = []

for nb in Notebook_paths:

    try :
        dbutils.notebook.run(nb, timeout_seconds=0)
        print(f'Success Notebook {nb} \n')
        success_list.append(nb)

    except:
        print('failed Notebook {nb} \n')
        failed_list.append(nb)

print(f"Success: {success_list} \n")
print(f"Failed: {failed_list} \n")