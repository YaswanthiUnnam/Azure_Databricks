-- Databricks notebook source
use f1_processed

-- COMMAND ----------

select *, concat(driver_ref, '-', code) as new_driver_ref from drivers

-- COMMAND ----------

select *, SPLIT(name, ' ') from drivers

-- COMMAND ----------

select *, SPLIT(name, ' ') [0] forename, SPLIT(name, ' ') [1] surname from drivers

-- COMMAND ----------

select *, current_timestamp() from drivers

-- COMMAND ----------

select *, date_format(dob,'dd-MM-yyyy') from drivers

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Aggregate functions

-- COMMAND ----------

select count(1) from drivers

-- COMMAND ----------

select max(dob) from drivers

-- COMMAND ----------

select * from drivers where dob = '2000-05-11'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **GroupBy**

-- COMMAND ----------

select nationality, count(*) from drivers
group by nationality
order by nationality

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Having**

-- COMMAND ----------

select nationality, count(*) from drivers
group by nationality
having count(*) > 1
order by nationality

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Window functions

-- COMMAND ----------

select nationality, name, dob, RANK() over (partition by nationality order by dob desc) 
as age_rank from drivers
order by nationality, age_rank