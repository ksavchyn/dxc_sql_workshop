# Databricks notebook source
dbutils.widgets.text('catalog','hls_sql_workshop')
catalog = dbutils.widgets.get('catalog')
print(f'catalog = {catalog}')

# COMMAND ----------

from pprint import pprint
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import *

w = WorkspaceClient()

# Create an online table
spec = OnlineTableSpec(
  primary_key_columns=["beneficiary_code"],
  source_table_full_name=f"{catalog}.ai.feature_beneficiary",
  run_triggered=OnlineTableSpecTriggeredSchedulingPolicy.from_dict({"full_refresh": True})
  )

online_table = OnlineTable(
  name = f"{catalog}.ai.online_beneficiary",
  spec = spec
)
# ignore "already exists" error
try:
 online_table_pipeline = w.online_tables.create(table=online_table)
except Exception as e:
 if "already exists" in str(e):
   pass
 else:
   raise e
