# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Accelerating Data Science with Databricks AutoML
# MAGIC
# MAGIC ##  Predicting patient readmission risk: Single click deployment with AutoML
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/hls/patient-readmission/patient-risk-ds-flow-2.png?raw=true" width="700px" style="float: right; margin-left: 10px;" />
# MAGIC
# MAGIC
# MAGIC In this notebook, we will explore how to use Databricks AutoML to generate the best notebooks to predict our patient readmission risk and deploy our model in production.
# MAGIC
# MAGIC Databricks AutoML allows you to quickly generate baseline models and notebooks. 
# MAGIC
# MAGIC ML experts can accelerate their workflow by fast-forwarding through the usual trial-and-error and focus on customizations using their domain knowledge, and citizen data scientists can quickly achieve usable results with a low-code approach.

# COMMAND ----------

dbutils.widgets.text("catalog", 'hls_sql_workshop')

# COMMAND ----------

from pyspark.sql import SparkSession
spark: SparkSession

catalog = dbutils.widgets.get("catalog")
spark.sql(f"USE {catalog}.ai")

# COMMAND ----------

import mlflow
mlflow.set_registry_uri("databricks-uc")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Getting our training dataset 
# MAGIC
# MAGIC Let's use the our training dataset determinining the readmission after 30 days for all our population. We will use that as our training label and what we want our model to predict.

# COMMAND ----------

training_dataset = spark.table(f'{catalog}.ai.training_beneficiary')
display(training_dataset)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Define what features to look up for our model
# MAGIC
# MAGIC Let's only keep the relevant features for our model training. We are removing columns such as `SSN` or `IDs`.
# MAGIC
# MAGIC This step could also be done selecting the training_dataset table from the UI and selecting the column of interest.
# MAGIC
# MAGIC *Note: this could also be retrived from our Feature Store tables. For more details on that open the companion notebook.*

# COMMAND ----------

excluded_columns = ['beneficiary_code', 'claim_amount']
excluded_columns

# COMMAND ----------

feature_lookups = [
    {
        "table_name": "hls_sql_workshop.ai.feature_beneficiary",
        "lookup_key": ["beneficiary_code"]
    }
]

# COMMAND ----------

from databricks import automl

summary = automl.regress(
    dataset=training_dataset,
    target_col="claim_amount",
    feature_store_lookups=feature_lookups,
    primary_metric="mae",
    timeout_minutes=10
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deploying our model in production
# MAGIC
# MAGIC Our model is now ready. We can review the notebook generated by the auto-ml run and customize if if required.
# MAGIC
# MAGIC For this demo, we'll consider that our model is ready and deploy it in production in our Model Registry:

# COMMAND ----------

model_name = f"{catalog}.ai.predict_claims_amount_model"
model_registered = mlflow.register_model(f"runs:/{summary.best_trial.mlflow_run_id}/model", model_name)

#Move the model in production
client = mlflow.tracking.MlflowClient()
print("registering model version "+model_registered.version+" as production model")
client.set_registered_model_alias(model_name, "Production", model_registered.version)

# COMMAND ----------

# MAGIC %md
# MAGIC We just moved our automl model as production ready! 
# MAGIC
# MAGIC Open [the dbdemos_hls_patient_readmission model](#mlflow/models/dbdemos_hls_patient_readmission) to explore its artifact and analyze the parameters used, including traceability to the notebook used for its creation.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Our model predicting default risks is now deployed in production
# MAGIC
# MAGIC So far we have:
# MAGIC * ingested all required data in a single source of truth using the OMOP data model,
# MAGIC * properly secured all data (including granting granular access controls, masked PII data, applied column level filtering),
# MAGIC * enhanced that data through feature engineering (and Feature Store as an option),
# MAGIC * used MLFlow AutoML to track experiments and build a machine learning model,
# MAGIC * registered the model.
# MAGIC
# MAGIC ### Next steps
# MAGIC We're now ready to use our model use it for:
# MAGIC
# MAGIC - Batch inferences in notebook [04.3-Batch-Scoring-patient-readmission]($./04.3-Batch-Scoring-patient-readmission) to start using it for identifying patient at risk and providing cusom care to reduce readmission risk,
# MAGIC - Real time inference with [04.4-Model-Serving-patient-readmission]($./04.4-Model-Serving-patient-readmission) to enable realtime capabilities and instantly get insight for a specific patient.
# MAGIC - Explain model for our entire population or a specific patient to understand the risk factors and further personalize care with [04.5-Explainability-patient-readmission]($./04.5-Explainability-patient-readmission)
