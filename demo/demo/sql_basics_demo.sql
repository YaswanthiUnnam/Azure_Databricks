-- Databricks notebook source
show databases;

-- COMMAND ----------

select current_databases()

-- COMMAND ----------

use f1_processed

-- COMMAND ----------

show tables;

-- COMMAND ----------

select * from drivers

-- COMMAND ----------

desc drivers;

-- COMMAND ----------

select name, dob, nationality
from drivers
where nationality = 'British'
AND dob >= '1990-01-01'
order by dob desc

-- COMMAND ----------

