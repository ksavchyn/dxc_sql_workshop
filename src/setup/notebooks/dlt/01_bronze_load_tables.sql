-- Databricks notebook source
-- volume_path = f"/Volumes/katsavchyn/employee_analytics/raw_files"
CREATE STREAMING LIVE TABLE bronze_employee_data
  COMMENT "raw data containing employee details"
AS 
SELECT 
  * 
  ,current_timestamp as load_timestamp
  ,current_timestamp as update_timestamp
  ,_metadata
FROM cloud_files("${volume_path}/employee_data.csv", 'csv')
-- FROM cloud_files("/Volumes/katsavchyn/employee_analytics/raw_files/employee_data.csv", 'csv')


-- COMMAND ----------

CREATE STREAMING LIVE TABLE bronze_employee_survey
  COMMENT "raw data from employee survey responses"
AS 
SELECT 
  * 
  ,current_timestamp as load_timestamp
  ,current_timestamp as update_timestamp
  ,_metadata
FROM cloud_files("${volume_path}/employee_engagement_survey_data.csv", 'csv')

-- COMMAND ----------

CREATE STREAMING LIVE TABLE bronze_recruitment_data
  COMMENT "raw data from candidate recruitment and interviews"
AS 
SELECT 
  * 
  ,current_timestamp as load_timestamp
  ,current_timestamp as update_timestamp
  ,_metadata
FROM cloud_files("${volume_path}/recruitment_data.csv", 'csv')

-- COMMAND ----------

CREATE STREAMING LIVE TABLE bronze_training_data
  COMMENT "raw data capturing employee training activities"
AS 
SELECT 
  * 
  ,current_timestamp as load_timestamp
  ,current_timestamp as update_timestamp
  ,_metadata
FROM cloud_files("${volume_path}/training_and_development_data.csv", 'csv')
