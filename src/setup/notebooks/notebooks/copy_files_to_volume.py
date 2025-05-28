# Databricks notebook source
# create widgets
dbutils.widgets.text('catalog', 'dxc_sql_workshop')
dbutils.widgets.text('schema', 'employee_data')
dbutils.widgets.text('volume', 'raw_files')
dbutils.widgets.text('sas_token', "")

# COMMAND ----------

# assign parameters to variables
catalog = dbutils.widgets.get(name = "catalog")
schema = dbutils.widgets.get(name = "schema")
volume = dbutils.widgets.get(name = "volume")
volume_path = f"/Volumes/{catalog}/{schema}/{volume}"
sas_token = dbutils.widgets.get(name = "sas_token")

# print values
print(f"""
  catalog = {catalog}
  schema = {schema}
  volume = {volume_path}
""")

# COMMAND ----------

## set configurations to use SAS token to connect to ADLS Gen2 Blob storage container
source_container_name = "dxc"
source_account_name = "dbsqlworkshop"

spark.conf.set(f"fs.azure.account.auth.type.{source_account_name}.dfs.core.windows.net", "SAS")
spark.conf.set(f"fs.azure.sas.token.provider.type.{source_account_name}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set(f"fs.azure.sas.fixed.token.{source_account_name}.dfs.core.windows.net", sas_token)

source_url = f"abfss://{source_container_name}@{source_account_name}.dfs.core.windows.net"
dbutils.fs.ls(source_url)

# COMMAND ----------

## copy csv files into Volume
dbutils.fs.cp(source_url, volume_path, recurse=True)
