# Databricks notebook source
# create widgets
dbutils.widgets.text('catalog', 'hls_sql_workshop')
dbutils.widgets.text('schema', 'cms')
dbutils.widgets.text('volume', 'raw_files')
dbutils.widgets.text('sas_token', "")

# COMMAND ----------

# assign parameters to variables
catalog = dbutils.widgets.get(name = "catalog")
schema = dbutils.widgets.get(name = "schema")
volume = dbutils.widgets.get(name = "volume")
volume_path = f"/Volumes/{catalog}/{schema}/{volume}/medicare_claims"
sas_token = dbutils.widgets.get(name = "sas_token")

# print values
print(f"""
  catalog = {catalog}
  schema = {schema}
  volume = {volume_path}
""")

# COMMAND ----------

# Replace these with your actual values
source_account_name = "hlssqlworkshopsa"
source_container_name = "cmsdata"

# Set Spark config to use the SAS token
spark.conf.set(
    f"fs.azure.sas.{source_container_name}.{source_account_name}.blob.core.windows.net",
    sas_token
)

# Construct the source URL
source_url = f"wasbs://{source_container_name}@{source_account_name}.blob.core.windows.net/"

# List files in the container
files = dbutils.fs.ls(source_url)

# Display the list of files
display(files)

# COMMAND ----------

# define folders to check if exist
folders_to_check = ['beneficiary_summary', 'carrier_claims', 'date', 'icd_codes', 'inpatient_claims', 'lookup', 'npi_code', 'outpatient_claims', 'prescription_drug_events']

# check if folders already exist in volume. If not, copy the data over
for folder in folders_to_check:
  target = f"{volume_path}/{folder}"
  try:
    print(f'Checking if folder exists in volume. Folder: {folder}')
    dbutils.fs.ls(f"{target}")
    print(f'  Folder already exists in volume. Skipping copy.')
  except:
    source = f'{source_url}{folder}'
    print(f'Path does not exist. Copying files from source to target \n source: {source} \n target: {target}')
    dbutils.fs.cp(source, target, True)
