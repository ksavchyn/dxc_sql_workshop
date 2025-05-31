-- Databricks notebook source
-- MAGIC %md
-- MAGIC ###Employee Data (Silver)

-- COMMAND ----------

CREATE STREAMING LIVE TABLE silver_employee_data (
CONSTRAINT `EmployeeID is not null`    EXPECT (employee_ID is not null)
)
AS
SELECT
   pde.EmpID as employee_ID
   ,pde.FirstName as first_name
   ,pde.LastName as last_name
   ,pde.GenderCode as gender
   ,pde.MaritalDesc as marital_status
   ,pde.RaceDesc as race
   ,pde.State as state
   ,to_date(pde.DOB,'yyyyMMdd') as date_of_birth
   ,to_date(pde.StartDate,'yyyyMMdd') as start_date
  ,to_date(pde.ExitDate,'yyyyMMdd') as exit_date
  , pde.Title as title
  ,pde.Supervisor as supervisor
  ,cast(pde.BusinessUnit as string) as business_unit
  ,pde.Department as department
  ,pde.Division as division
  ,pde.JobFunction as job_function
  ,pde.EmployeeStatus as employee_status
  ,pde.TerminationType as termination_type
  ,pde.EmployeeType as employment_type
  ,`pde`.`Performance Score` as performance_score
  ,`pde`.`Current Employee Rating` as current_rating
  ,current_timestamp as load_timestamp
FROM stream(live.bronze_employee_data) pde

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Employee Survey Data (Silver)

-- COMMAND ----------

CREATE STREAMING LIVE TABLE silver_employee_survey(
  CONSTRAINT `Employee ID is not null`    EXPECT (employee_ID is not null)
)
--PARTITIONED BY (file_name)
AS
SELECT
   `cc`.`Employee ID` as employee_ID
  ,to_date(`cc`.`Survey Date`,'yyyyMMdd') as survey_date
  ,cast(`cc`.`Enagement Score` as int) as engement_score
  ,cast(`cc`.`Satisfaction Score` as int) as satisfaction_score
  ,cast(`cc`.`Work-Life Balance Score` as int) as work_life_balance_score
  ,cc.update_timestamp as bronze_update_timestamp
  ,current_timestamp as load_timestamp
FROM stream(live.bronze_employee_survey) cc


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Employee Training Data (Silver)

-- COMMAND ----------

CREATE STREAMING LIVE TABLE silver_training_data (
  CONSTRAINT `Employee ID is not null`    EXPECT (employee_ID is not null)
)
AS
SELECT
   `ic`.`Employee ID` as employee_ID
  ,to_date(`ic`.`Training Date`,'yyyyMMdd') as training_date
  ,`ic`.`Training Program Name` as program_name
  ,`ic`.`Training Type` as training_type
  ,`ic`.`Training Outcome` as training_outcome
  ,ic.Trainer as trainer
  ,cast(`ic`.`Training Duration(Days)` as int) as training_duration
  ,coalesce(cast(`ic`.`Training Cost` as double),0.0) as training_cost
  ,ic.update_timestamp as bronze_update_timestamp
  ,current_timestamp as load_timestamp
FROM stream(live.bronze_training_data) ic

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Recruitment Data (Silver)

-- COMMAND ----------

CREATE STREAMING LIVE TABLE silver_recruitment_data (
  CONSTRAINT `Applicant ID is not null`    EXPECT (applicant_ID is not null)
)
AS
SELECT
   `oc`.`Applicant ID` as applicant_ID
  ,to_date(`oc`.`Date of Birth`,'yyyyMMdd') as date_of_birth
  ,to_date(`oc`.`Application Date`,'yyyyMMdd') as application_date
  ,`oc`.`First Name` as first_name
  ,`oc`.`Last Name` as last_name
  ,`oc`.`Gender` as gender
  ,`oc`.`Phone Number` as phone_number
  ,`oc`.`Email` as email
  ,`oc`.`Address` as address
  ,`oc`.`City` as city
  ,`oc`.`State` as state
  ,`oc`.`Zip Code` as zip_code
  ,`oc`.`Country` as country
  ,`oc`.`Education Level` as education_level
  ,`oc`.`Job Title` as job_title
  ,`oc`.`Status` as status
  ,cast(`oc`.`Years of Experience` as int) as years_of_experience
  ,coalesce(cast(`oc`.`Desired Salary` as double),0.0) as desired_salary
  ,oc.update_timestamp as bronze_update_timestamp
  ,current_timestamp as load_timestamp
FROM stream(live.bronze_recruitment_data) oc

