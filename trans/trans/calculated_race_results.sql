-- Databricks notebook source
use f1_processed;

-- COMMAND ----------

create table f1_presentation.calculated_race_results
USING parquet
AS
select races.year, constructors.name as team_name, drivers.name as driver_name, 
results.position, results.points, 
11 - results.position as calculated_points
 from results
 JOIN f1_processed.drivers on (results.driver_id = drivers.driver_id)
 JOIN f1_processed.constructors on (results.constructor_id = constructors.constructorid)
 JOIN f1_processed.races on (results.race_id = races.race_id)
 WHERE results.position <= 10

-- COMMAND ----------

select * from f1_presentation.calculated_race_results

-- COMMAND ----------

