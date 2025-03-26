-- Databricks notebook source
create database if not exists demo;


-- COMMAND ----------

show databases;

-- COMMAND ----------

desc database extended demo;

-- COMMAND ----------

select current_database();

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Managed tables in SPARK

-- COMMAND ----------

-- MAGIC
-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC race_results_df.write.format("parquet").saveAsTable("demo.race_results_python")

-- COMMAND ----------

use demo;
show tables;

-- COMMAND ----------

describe extended race_results_python

-- COMMAND ----------

select * from demo.race_results_python
where year = 2019

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create a managed table using SQL

-- COMMAND ----------

create table demo.race_results_sql
AS
select * from demo.race_results_python
where year = 2019

-- COMMAND ----------

select current_database();

-- COMMAND ----------

desc extended 
demo.race_results_sql

-- COMMAND ----------

drop table demo.race_results_sql

-- COMMAND ----------

show tables in demo


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create a External table using SQL

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC race_results_df.write.format("parquet").option("path",f"{presentation_folder_path}/race_results_ext_python").saveAsTable("demo.race_results_ext_python")

-- COMMAND ----------

describe extended demo.race_results_ext_python

-- COMMAND ----------

/*creating external table using SQL statements*/

DROP TABLE IF EXISTS demo.race_results_ext_sql;

create table demo.race_results_ext_sql
(
  year INT, race_name string, circuit_location string, driver_name string, driver_number INT, driver_nationality string, team string, grid INT, fastest_lap INT, race_time string, points FLOAT, position STRING, created_date timestamp
)
USING parquet
location '/mnt/formula1cardatalake/presentation/race_results_ext_sql'

-- COMMAND ----------

show tables in demo;

-- COMMAND ----------

DESCRIBE TABLE demo.race_results_ext_sql;
DESCRIBE TABLE demo.race_results_ext_python;


-- COMMAND ----------

insert into demo.race_results_ext_sql
select * from demo.race_results_ext_python
where year = 2020;

-- COMMAND ----------

select count(1) from demo.race_results_ext_sql

-- COMMAND ----------

show tables in demo;


-- COMMAND ----------

drop table demo.race_results_ext_sql

-- COMMAND ----------

show tables in demo;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Views on table

-- COMMAND ----------

/*local temp view*/

create or replace temp view v_race_results as select * from demo.race_results_python
where year = 2019;

-- COMMAND ----------

select * from v_race_results

-- COMMAND ----------

/*Global temp view*/

create or replace global temp view gv_race_results as select * from demo.race_results_python
where year = 2019;

-- COMMAND ----------

select * from global_temp.gv_race_results

-- COMMAND ----------

show tables in global_temp;

-- COMMAND ----------

drop table global_temp.v_race_results

-- COMMAND ----------

show tables in global_temp;

-- COMMAND ----------

