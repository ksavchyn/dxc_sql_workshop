-- Databricks notebook source
-- MAGIC %md
-- MAGIC ###gold_retruitment_data

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE gold_recruitment_data;

APPLY CHANGES INTO
  live.gold_recruitment_data
FROM
  stream(live.silver_recruitment_data)
KEYS
  (applicant_ID)
SEQUENCE BY
  load_timestamp
COLUMNS
    applicant_ID
   ,first_name
   ,last_name
   ,date_of_birth
   ,address
   ,city
   ,state
   ,zip_code
   ,country
   ,phone_number
   ,email
   ,gender
   ,education_level
   ,status
   ,desired_salary
STORED AS
  SCD TYPE 1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###gold_training_data

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE gold_training_data;

APPLY CHANGES INTO
  live.gold_training_data
FROM
  stream(live.silver_training_data)
KEYS
  (employee_ID)
SEQUENCE BY
  load_timestamp
COLUMNS * EXCEPT
  (load_timestamp,bronze_load_timestamp)
STORED AS
  SCD TYPE 1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###gold_employee_data

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE gold_employee_data;

APPLY CHANGES INTO
  live.gold_employee_data
FROM
  stream(live.silver_employee_data)
KEYS
  (employee_ID)
SEQUENCE BY
  load_timestamp
COLUMNS
    employee_ID
   ,first_name
   ,last_name
   ,date_of_birth
   ,gender
   ,marital_status
   ,race
   ,state
   ,date_of_birth
   ,start_date
   ,exit_date
   ,employee_status
   ,performance_score
   ,current_rating
   ,title
STORED AS
  SCD TYPE 1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###gold_survey_data

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE gold_survey_data;

APPLY CHANGES INTO
  live.gold_survey_data
FROM
  stream(live.silver_survey_data)
KEYS
  (employee_ID)
SEQUENCE BY
  load_timestamp
COLUMNS * EXCEPT
  (load_timestamp,bronze_load_timestamp)
STORED AS
  SCD TYPE 1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## gold_employee_360

-- COMMAND ----------

CREATE LIVE TABLE gold_employee_360
TBLPROPERTIES ("pipelines.autoOptimize.zOrderCols"="employee_ID")
--PARTITIONED BY (file_name)
AS
SELECT
   * 
FROM live.gold_employee_data ed
WHERE NOT (ed.employee_status = 'Active' AND ed.exit_date IS NOT NULL)
  AND (ed.start_date <= ed.exit_date OR ed.exit_date IS NULL)
  AND year(ed.date_of_birth) BETWEEN 1950 AND 2010
  AND NOT EXISTS (
    SELECT 1
    FROM live.gold_recruitment_data rd
    WHERE rd.applicant_ID = ed.employee_ID
      AND rd.applicant_status = 'Rejected'
      AND ed.employee_status = 'Active'
  )
  AND year(current_date()) - year(ed.date_of_birth) >= (
    SELECT rd.years_of_experience
    FROM live.gold_recruitment_data rd
    WHERE rd.applicant_ID = ed.employee_ID
  )
LEFT JOIN live.gold_survey_data sd 
  ON ed.employee_ID = sd.employee_ID 
  AND sd.survey_date >= ed.start_date
  AND (ed.exit_date IS NULL OR sd.survey_date <= ed.exit_date)
LEFT JOIN live.gold_training_data td 
  ON ed.employee_ID = td.employee_ID
  AND (td.training_date <= ed.exit_date OR ed.exit_date IS NULL)
LEFT JOIN live.gold_recruitment_data rd
  ON ed.employee_ID = rd.applicant_ID
