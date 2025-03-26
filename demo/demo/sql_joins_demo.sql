-- Databricks notebook source
use f1_presentation

-- COMMAND ----------

desc driver_standings

-- COMMAND ----------

create or replace temp view v_driver_standings_2018 as select year,driver_name,team,total_points, wins, rank from driver_standings where year=2018

-- COMMAND ----------

select * from v_driver_standings_2018

-- COMMAND ----------

create or replace temp view v_driver_standings_2020 as select year,driver_name,team,total_points, wins, rank from driver_standings where year=2020

-- COMMAND ----------

select * from v_driver_standings_2020

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Inner Join

-- COMMAND ----------

select * from v_driver_standings_2018 d_2018
JOIN  v_driver_standings_2020 d_2020 on d_2018.driver_name=d_2020.driver_name

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Left JOIN

-- COMMAND ----------

select * from v_driver_standings_2018 d_2018
LEFT JOIN  v_driver_standings_2020 d_2020 on d_2018.driver_name=d_2020.driver_name

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Right JOIN

-- COMMAND ----------

select * from v_driver_standings_2018 d_2018
RIGHT JOIN  v_driver_standings_2020 d_2020 on d_2018.driver_name=d_2020.driver_name

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Full JOIN

-- COMMAND ----------

select * from v_driver_standings_2018 d_2018
FULL JOIN  v_driver_standings_2020 d_2020 on d_2018.driver_name=d_2020.driver_name

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Semi JOIN

-- COMMAND ----------

select * from v_driver_standings_2018 d_2018
SEMI JOIN  v_driver_standings_2020 d_2020 on d_2018.driver_name=d_2020.driver_name

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Anti Join

-- COMMAND ----------

select * from v_driver_standings_2018 d_2018
ANTI JOIN  v_driver_standings_2020 d_2020 on d_2018.driver_name=d_2020.driver_name

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Cross JOIN

-- COMMAND ----------

/* join very small dimension with few records to a fact table and multiply data*/


select * from v_driver_standings_2018 d_2018
CROSS JOIN  v_driver_standings_2020 d_2020