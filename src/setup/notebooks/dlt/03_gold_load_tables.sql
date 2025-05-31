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
