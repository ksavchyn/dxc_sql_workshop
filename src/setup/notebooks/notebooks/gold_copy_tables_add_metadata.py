# Databricks notebook source
# create widgets
dbutils.widgets.text('catalog', 'katsavchyn')
dbutils.widgets.text('schema', 'gold')

# assign widget value to variable
catalog = dbutils.widgets.get('catalog')
schema = dbutils.widgets.get('schema')

# specify catalog to use
spark.sql(f'USE CATALOG {catalog}')
spark.sql(f'USE SCHEMA {schema}')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold.gold_recruitment_data AS 
# MAGIC SELECT * 
# MAGIC FROM gold_recruitment_data;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE gold.gold_training_data
# MAGIC  AS SELECT * FROM gold_training_data;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE gold.gold_employee_data
# MAGIC AS SELECT * 
# MAGIC FROM gold_employee_data;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE gold.gold_survey_data AS 
# MAGIC SELECT * 
# MAGIC FROM gold_survey_data;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE gold.gold_employee_360 
# MAGIC  AS 
# MAGIC SELECT * 
# MAGIC FROM gold_employee_360;
# MAGIC
# MAGIC ------------------------
# MAGIC
# MAGIC ALTER TABLE gold.gold_training_data 
# MAGIC ALTER COLUMN employee_ID SET NOT NULL;
# MAGIC
# MAGIC ALTER TABLE gold.gold_recruitment_data 
# MAGIC ALTER COLUMN applicant_ID SET NOT NULL;
# MAGIC
# MAGIC ALTER TABLE gold.gold_employee_data 
# MAGIC ALTER COLUMN employee_ID SET NOT NULL;
# MAGIC
# MAGIC ALTER TABLE gold.gold_survey_data 
# MAGIC ALTER COLUMN employee_ID SET NOT NULL;
# MAGIC
# MAGIC ALTER TABLE gold.gold_employee_360 
# MAGIC ALTER COLUMN employee_ID SET NOT NULL;
# MAGIC
# MAGIC ALTER TABLE gold.gold_training_data 
# MAGIC ADD CONSTRAINT employee_pk PRIMARY KEY (employee_ID);
# MAGIC
# MAGIC ALTER TABLE gold.gold_recruitment_data 
# MAGIC ADD CONSTRAINT applicant_pk PRIMARY KEY (applicant_ID);
# MAGIC
# MAGIC ALTER TABLE gold.gold_employee_data 
# MAGIC ADD CONSTRAINT employee_pk PRIMARY KEY (employee_ID);
# MAGIC
# MAGIC ALTER TABLE gold.gold_survey_data 
# MAGIC ADD CONSTRAINT employee_pk PRIMARY KEY (employee_ID);
# MAGIC
# MAGIC ALTER TABLE gold.gold_employee_360 
# MAGIC ADD CONSTRAINT employee_pk FOREIGN KEY (employee_ID) REFERENCES gold.gold_employee_360;
# MAGIC
# MAGIC ALTER TABLE gold.fact_carrier_claims 
# MAGIC ADD CONSTRAINT claim_start_date_fk FOREIGN KEY (claim_start_date) REFERENCES gold.dim_date;
# MAGIC
# MAGIC -- ALTER TABLE gold.fact_carrier_claims 
# MAGIC -- ADD CONSTRAINT diagnosis_key_1_fk FOREIGN KEY (diagnosis_key_1) REFERENCES gold.dim_diagnosis;
# MAGIC
# MAGIC -- ALTER TABLE gold.fact_patient_claims 
# MAGIC -- ADD CONSTRAINT pc_beneficiary_fk FOREIGN KEY (beneficiary_key) REFERENCES gold.dim_beneficiary;
# MAGIC
# MAGIC -- ALTER TABLE gold.fact_patient_claims 
# MAGIC -- ADD CONSTRAINT pc_claim_start_date_fk FOREIGN KEY (claim_start_date) REFERENCES gold.dim_date;
# MAGIC
# MAGIC -- ALTER TABLE gold.fact_patient_claims 
# MAGIC -- ADD CONSTRAINT pc_diagnosis_key_1_fk FOREIGN KEY (diagnosis_key_1) REFERENCES gold.dim_diagnosis;
# MAGIC
# MAGIC -- ALTER TABLE gold.fact_patient_claims 
# MAGIC -- ADD CONSTRAINT pc_attending_physician_provider_key_fk FOREIGN KEY (attending_physician_provider_key) REFERENCES gold.dim_provider;
# MAGIC
# MAGIC -- ALTER TABLE gold.fact_prescription_drug_events 
# MAGIC -- ADD CONSTRAINT pde_beneficiary_fk FOREIGN KEY (beneficiary_key) REFERENCES gold.dim_beneficiary;
# MAGIC
# MAGIC -- ALTER TABLE gold.fact_prescription_drug_events 
# MAGIC -- ADD CONSTRAINT pde_rx_service_date_fk FOREIGN KEY (rx_service_date) REFERENCES gold.dim_date;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- create report table for ai/bi dashboard
# MAGIC -- CREATE OR REPLACE TABLE gold.rpt_patient_claims AS
# MAGIC -- SELECT
# MAGIC --   a.claim_type,
# MAGIC --   a.claim_start_date,
# MAGIC --   b.gender,
# MAGIC --   b.race,
# MAGIC --   b.deceased_flag,
# MAGIC --   b.state,
# MAGIC --   b.county_code,
# MAGIC --   b.esrd_flag,
# MAGIC --   b.cancer_flag,
# MAGIC --   b.heart_failure_flag,
# MAGIC --   b.copd_flag,
# MAGIC --   b.depression_flag,
# MAGIC --   b.diabetes_flag,
# MAGIC --   b.ischemic_heart_disease_flag,
# MAGIC --   b.osteoporosis_flag,
# MAGIC --   b.rheumatoid_arthritis_flag,
# MAGIC --   b.stroke_transient_ischemic_attack_flag,
# MAGIC --   c.diagnosis_short_description,
# MAGIC --   d.provider_organization_name,
# MAGIC --   d.entity_type,
# MAGIC --   sum(a.claim_payment_amount) as claim_payment_amount,
# MAGIC --   sum(a.primary_payer_claim_paid_amount) as primary_payer_claim_paid_amount
# MAGIC --  FROM gold.fact_patient_claims a
# MAGIC --  INNER JOIN gold.dim_beneficiary b ON a.beneficiary_key = b.beneficiary_key
# MAGIC --  INNER JOIN gold.dim_diagnosis c ON a.diagnosis_key_1 = c.diagnosis_key
# MAGIC --  INNER JOIN gold.dim_provider d on a.attending_physician_provider_key = d.provider_key
# MAGIC --  GROUP BY ALL;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION gold.get_age(start_date date) 
# MAGIC RETURNS INT
# MAGIC RETURN FLOOR(DATEDIFF('2015-12-31', start_date) / 365.25);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- add gold_recruitment_data comment
# MAGIC COMMENT ON TABLE
# MAGIC gold.gold_recruitment_data 
# MAGIC IS
# MAGIC "The 'gold_recruitment_data' table offers a comprehensive view of the recruitment process within an organization, encompassing key data points from initial job posting to candidate selection. This dataset aims to facilitate insights into the efficiency of recruitment efforts, candidate profiles, and the effectiveness of sourcing channels.";
# MAGIC
# MAGIC ALTER TABLE gold.gold_recruitment_data SET TAGS ('certified');
# MAGIC
# MAGIC -- add gold_training_data comment
# MAGIC COMMENT ON TABLE
# MAGIC gold.gold_training_data 
# MAGIC IS
# MAGIC "The 'gold_training_data' table provides a comprehensive overview of training activities and developmental programs within an organization. This dataset is designed to capture the details of employee participation in various training initiatives, helping to assess the impact of professional development efforts and guide strategic decision-making.";
# MAGIC
# MAGIC ALTER TABLE gold.gold_training_data SET TAGS ('certified');
# MAGIC
# MAGIC -- add gold_employee_data comments
# MAGIC COMMENT ON TABLE
# MAGIC gold.gold_employee_data
# MAGIC IS
# MAGIC "The 'gold_employee_data' table is a simulated dataset created to explore various data analysis and machine learning techniques in the context of human resources and employee management. This synthetic dataset mirrors the structure and characteristics of real employee data, while all the information contained within is entirely fictional and generated for illustrative purposes.";
# MAGIC
# MAGIC ALTER TABLE gold.gold_employee_data SET TAGS ('certified');
# MAGIC
# MAGIC -- add gold_survey_data comments
# MAGIC COMMENT ON TABLE
# MAGIC gold.gold_survey_data
# MAGIC IS
# MAGIC "The 'gold_survey_data' table presents a comprehensive collection of responses obtained through an organization-wide employee engagement survey. This dataset is intended to analyze the levels of employee engagement, satisfaction, and sentiment across various facets of the workplace, facilitating insights into workforce dynamics and guiding strategies for improvement.";
# MAGIC
# MAGIC ALTER TABLE gold.gold_survey_data SET TAGS ('certified');

# COMMAND ----------

# DBTITLE 1,add column comments
# MAGIC %sql
# MAGIC ALTER TABLE gold.gold_employee_data ALTER COLUMN employee_ID COMMENT 'Unique identifier for each employee in the organization.';
# MAGIC ALTER TABLE gold.gold_employee_data ALTER COLUMN first_name COMMENT 'The first name of the employee.';
# MAGIC ALTER TABLE gold.gold_employee_data ALTER COLUMN last_name COMMENT 'The last name of the employee.';
# MAGIC ALTER TABLE gold.gold_employee_data ALTER COLUMN gender COMMENT 'A code representing the gender of the employee (e.g., M for Male, F for Female, N for Non-binary).';
# MAGIC ALTER TABLE gold.gold_employee_data ALTER COLUMN marital_status COMMENT 'The marital status of the employee (e.g., Single, Married, Divorced).';
# MAGIC ALTER TABLE gold.gold_employee_data ALTER COLUMN race COMMENT 'A description of the employee\'s racial or ethnic background (if provided).';
# MAGIC ALTER TABLE gold.gold_employee_data ALTER COLUMN state COMMENT 'The state or region where the employee is located.';
# MAGIC ALTER TABLE gold.gold_employee_data ALTER COLUMN date_of_birth COMMENT 'The date of birth of the employee.';
# MAGIC ALTER TABLE gold.gold_employee_data ALTER COLUMN start_date COMMENT 'The date when the employee started working for the organization.';
# MAGIC ALTER TABLE gold.gold_employee_data ALTER COLUMN exit_date COMMENT 'The date when the employee left or exited the organization (if applicable).';
# MAGIC ALTER TABLE gold.gold_employee_data ALTER COLUMN title COMMENT 'The job title or position of the employee within the organization.';
# MAGIC ALTER TABLE gold.gold_employee_data ALTER COLUMN supervisor COMMENT 'The name of the employee\'s immediate supervisor or manager.';
# MAGIC ALTER TABLE gold.gold_employee_data ALTER COLUMN business_unit COMMENT 'The specific business unit or department to which the employee belongs.';
# MAGIC ALTER TABLE gold.gold_employee_data ALTER COLUMN department COMMENT 'The broader category or type of department the employee\'s work is associated with.';
# MAGIC ALTER TABLE gold.gold_employee_data ALTER COLUMN division COMMENT 'The division or branch of the organization where the employee works.';
# MAGIC ALTER TABLE gold.gold_employee_data ALTER COLUMN job_function COMMENT 'A brief description of the employee\'s primary job function or role.';
# MAGIC ALTER TABLE gold.gold_employee_data ALTER COLUMN employee_status COMMENT 'The current employment status of the employee (e.g., Active, On Leave, Terminated).';
# MAGIC ALTER TABLE gold.gold_employee_data ALTER COLUMN termination_type COMMENT 'The type of termination if the employee has left the organization (e.g., Resignation, Layoff, Retirement).';
# MAGIC ALTER TABLE gold.gold_employee_data ALTER COLUMN employment_type COMMENT 'The type of employment the employee has (e.g., Full-time, Part-time, Contract).';
# MAGIC ALTER TABLE gold.gold_employee_data ALTER COLUMN performance_score COMMENT 'A score indicating the employee\'s performance level (e.g., Excellent, Satisfactory, Needs Improvement).';
# MAGIC ALTER TABLE gold.gold_employee_data ALTER COLUMN current_rating COMMENT The current rating or evaluation of the employee's overall performance.';
# MAGIC ----------------------------------------------------
# MAGIC ALTER TABLE gold.gold_survey_data ALTER COLUMN employee_ID COMMENT 'A unique identifier assigned to each employee who participated in the employee engagement.';
# MAGIC ALTER TABLE gold.gold_survey_data ALTER COLUMN survey_date COMMENT 'The date on which the engagement survey was administered to employees.';
# MAGIC ALTER TABLE gold.gold_survey_data ALTER COLUMN engement_score COMMENT 'A calculated numerical score representing the level of employee engagement based on survey responses.';
# MAGIC ALTER TABLE gold.gold_survey_data ALTER COLUMN satisfaction_score COMMENT 'A numerical score indicating employee satisfaction with various aspects of their job and workplace.';
# MAGIC ALTER TABLE gold.gold_survey_data ALTER COLUMN work_life_balance_score COMMENT 'A numerical score reflecting employee perceptions of the balance between work and personal life.';
# MAGIC
# MAGIC -----------------------------------------------------
# MAGIC ALTER TABLE gold.gold_recruitment_data ALTER COLUMN applicant_ID COMMENT 'A unique identifier assigned to each applicant who has submitted their information for a job';
# MAGIC ALTER TABLE gold.gold_recruitment_data ALTER COLUMN date_of_birth COMMENT 'The birthdate of the applicant.';
# MAGIC ALTER TABLE gold.gold_recruitment_data ALTER COLUMN application_date COMMENT 'The date on which the applicant submitted their application for the job.';
# MAGIC ALTER TABLE gold.gold_recruitment_data ALTER COLUMN first_name COMMENT 'The first name of the applicant.';
# MAGIC ALTER TABLE gold.gold_recruitment_data ALTER COLUMN last_name COMMENT 'The last name of the applicant.';
# MAGIC ALTER TABLE gold.gold_recruitment_data ALTER COLUMN gender COMMENT 'The gender of the applicant.';
# MAGIC ALTER TABLE gold.gold_recruitment_data ALTER COLUMN phone_number COMMENT 'The contact phone number of the applicant.';
# MAGIC ALTER TABLE gold.gold_recruitment_data ALTER COLUMN email COMMENT 'The email address of the applicant for communication purposes.';
# MAGIC ALTER TABLE gold.gold_recruitment_data ALTER COLUMN address COMMENT 'The street address where the applicant resides.';
# MAGIC ALTER TABLE gold.gold_recruitment_data ALTER COLUMN city COMMENT 'The city where the applicant\s address is located.';
# MAGIC ALTER TABLE gold.gold_recruitment_data ALTER COLUMN state COMMENT 'The state or province where the applicant\'s address is situated.';
# MAGIC ALTER TABLE gold.gold_recruitment_data ALTER COLUMN zip_code COMMENT 'The postal or ZIP code associated with the applicant\'s address.';
# MAGIC ALTER TABLE gold.gold_recruitment_data ALTER COLUMN country COMMENT 'The country where the applicant\'s address is located.';
# MAGIC ALTER TABLE gold.gold_recruitment_data ALTER COLUMN education_level COMMENT 'The highest level of education attained by the applicant.';
# MAGIC ALTER TABLE gold.gold_recruitment_data ALTER COLUMN job_title COMMENT 'The title or designation of the job the applicant is applying for.';
# MAGIC ALTER TABLE gold.gold_recruitment_data ALTER COLUMN status COMMENT 'The status of the applicant\'s application (e.g., Submitted, Under Review, Rejected, Selected).';
# MAGIC ALTER TABLE gold.gold_recruitment_data ALTER COLUMN years_of_experience COMMENT 'The number of years of professional experience the applicant has.';
# MAGIC ALTER TABLE gold.gold_recruitment_data ALTER COLUMN desired_salary COMMENT 'The salary the applicant wishes to receive for the job.';
# MAGIC ---------------------------------------------------
# MAGIC
# MAGIC ALTER TABLE gold.gold_training_data ALTER COLUMN employee_ID COMMENT 'A unique identifier for each employee who participated in the training program.';
# MAGIC ALTER TABLE gold.gold_training_data ALTER COLUMN training_date COMMENT 'The date on which the training session took place.';
# MAGIC ALTER TABLE gold.gold_training_data ALTER COLUMN program_name COMMENT 'The name or title of the training program attended by the employee';
# MAGIC ALTER TABLE gold.gold_training_data ALTER COLUMN training_type COMMENT 'The categorization of the training, indicating its purpose or focus (e.g., Technical, Soft Skills, Safety).';
# MAGIC ALTER TABLE gold.gold_training_data ALTER COLUMN training_outcome COMMENT 'The observed outcome or result of the training for the employee (e.g., Completed, Partial Completion, Not';
# MAGIC ALTER TABLE gold.gold_training_data ALTER COLUMN training_duration COMMENT 'The duration of the training program in days.';
# MAGIC ALTER TABLE gold.gold_training_data ALTER COLUMN training_cost COMMENT 'The cost associated with organizing and conducting the training program.';
# MAGIC ALTER TABLE gold.gold_training_data ALTER COLUMN trainer COMMENT 'The name of the trainer or instructor who facilitated the training.';
