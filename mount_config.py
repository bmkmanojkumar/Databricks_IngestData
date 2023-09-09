# Databricks notebook source
dbutils.fs.mount(
				source = "wasbs://bronze@skdatalake97.blob.core.windows.net/input_files",
				mount_point = "/mnt/input_files",
				extra_configs = {"fs.azure.account.key.skdatalake97.blob.core.windows.net":"f/2Pcy02mrIbu1yPlwIM21LGOThl0yDxRk9xVt4HJcpc6mOvOC4tbwbdcFfQulEtaInSO7iN4DTs+AStuSootQ=="})
dbutils.fs.ls('/mnt/input_files')

# COMMAND ----------

dbutils.fs.ls('/mnt/input_files')

# COMMAND ----------

dbutils.fs.ls('/mnt/gold')

# COMMAND ----------

dbutils.fs.mount(
				source = "wasbs://silver@skdatalake97.blob.core.windows.net/parquet",
				mount_point = "/mnt/silver/parquet",
				extra_configs = {"fs.azure.account.key.skdatalake97.blob.core.windows.net":"f/2Pcy02mrIbu1yPlwIM21LGOThl0yDxRk9xVt4HJcpc6mOvOC4tbwbdcFfQulEtaInSO7iN4DTs+AStuSootQ=="})


# COMMAND ----------

dbutils.fs.mount(
				source = "wasbs://gold@skdatalake97.blob.core.windows.net/parquet",
				mount_point = "/mnt/gold/parquet",
				extra_configs = {"fs.azure.account.key.skdatalake97.blob.core.windows.net":"f/2Pcy02mrIbu1yPlwIM21LGOThl0yDxRk9xVt4HJcpc6mOvOC4tbwbdcFfQulEtaInSO7iN4DTs+AStuSootQ=="})

# COMMAND ----------


