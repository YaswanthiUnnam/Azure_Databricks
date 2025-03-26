-- Databricks notebook source
-- MAGIC %python
-- MAGIC html = """<h1 style = 'color:Black;text-align:center;font-family:Ariel'> Report on Dominant Formula 1 Drivers</h1>"""
-- MAGIC displayHTML(html)

-- COMMAND ----------

create or replace temp view v_dominant_drivers
AS
select driver_name,count(1) as total_races, sum(calculated_points) as total_points,
avg(calculated_points) as avg_points,
RANK() OVER (ORDER BY avg(calculated_points) desc) driver_rank
 from f1_presentation.calculated_race_results
 where year between 2011 and 2020
 group by driver_name
 having count(1) >=50
 order by avg_points desc

-- COMMAND ----------

select year, driver_name,count(1) as total_races, sum(calculated_points) as total_points,
avg(calculated_points) as avg_points
 from f1_presentation.calculated_race_results
 where driver_name in (select driver_name from v_dominant_drivers where driver_rank <= 10)
 group by year, driver_name
 
 order by year desc

-- COMMAND ----------

select year, driver_name,count(1) as total_races, sum(calculated_points) as total_points,
avg(calculated_points) as avg_points
 from f1_presentation.calculated_race_results
 where driver_name in (select driver_name from v_dominant_drivers where driver_rank <= 10)
 group by year, driver_name
 
 order by year desc

-- COMMAND ----------

select year, driver_name,count(1) as total_races, sum(calculated_points) as total_points,
avg(calculated_points) as avg_points
 from f1_presentation.calculated_race_results
 where driver_name in (select driver_name from v_dominant_drivers where driver_rank <= 10)
 group by year, driver_name
 
 order by year desc

-- COMMAND ----------

