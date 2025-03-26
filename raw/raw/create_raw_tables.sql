-- Databricks notebook source
create database if not exists f1_raw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### create circuits table

-- COMMAND ----------

drop table if exists f1_raw.circuits;

create table if not exists f1_raw.circuits
(
circuitId INT, circuitRef STRING, name STRING, location STRING, country STRING, lat double, lng double, alt INT, url STRING
)
using csv
options (path "/mnt/formula1cardatalake/raw/circuits.csv", header true)

-- COMMAND ----------

select * from f1_raw.circuits

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create races table

-- COMMAND ----------

drop table if exists f1_raw.races;

create table if not exists f1_raw.races
(
raceId INT, year INT, round INT, circuitId INT, name STRING, date DATE, time STRING, url STRING
)
using csv
options (path "/mnt/formula1cardatalake/raw/races.csv", header true)

-- COMMAND ----------

select * from f1_raw.races

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create tables for JSON files

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### create constructors table
-- MAGIC - single line JSON
-- MAGIC - simple structure
-- MAGIC

-- COMMAND ----------

drop table if exists f1_raw.constructors;

create table if not exists f1_raw.constructors
(
constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING
)
using json
options (path "/mnt/formula1cardatalake/raw/constructors.json", header true)

-- COMMAND ----------

select * from f1_raw.constructors

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### create drivers table
-- MAGIC - single line JSON
-- MAGIC - complex structure
-- MAGIC

-- COMMAND ----------

drop table if exists f1_raw.drivers;

create table if not exists f1_raw.drivers
(
driverId INT, driverRef STRING,number INT, code STRING, name STRUCT<forename:STRING, surname:STRING>, dob DATE, nationality STRING, url STRING
)
using json
options (path "/mnt/formula1cardatalake/raw/drivers.json", header true)

-- COMMAND ----------

select * from f1_raw.drivers

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### create results table
-- MAGIC - single line JSON
-- MAGIC - simple structure
-- MAGIC

-- COMMAND ----------

drop table if exists f1_raw.results;

create table if not exists f1_raw.results
(
resultId INT, raceId INT, driverId INT,constructorId INT,number INT, grid INT, position INT, positonText STRING, positionOrder INT, points INT, laps INT, time STRING, milliseconds INT, fastestLap INT, rank INT, fastestLapTime STRING, fastestLapSpeed Float, statusId STRING
)
using json
options (path "/mnt/formula1cardatalake/raw/results.json", header true)

-- COMMAND ----------

select * from f1_raw.results

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### create pit stops table
-- MAGIC - multi line JSON
-- MAGIC - simple structure
-- MAGIC

-- COMMAND ----------

drop table if exists f1_raw.pit_stops;

create table if not exists f1_raw.pit_stops
(
driverId INT, duration STRING, lap INT, milliseconds INT, raceId INT, stop INT, time STRING
)
using json
options (path "/mnt/formula1cardatalake/raw/pit_stops.json",multiLine true, header true)

-- COMMAND ----------

select * from f1_raw.pit_stops;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Creating Table for list of files

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### Create lap times table
-- MAGIC - CSV file
-- MAGIC - Multiple Files

-- COMMAND ----------

drop table if exists f1_raw.lap_times;

create table if not exists f1_raw.lap_times
(
raceId INT, driverId INT, lap INT,position INT, time STRING, milliseconds INT
)
using csv
options (path "/mnt/formula1cardatalake/raw/lap_times")

-- COMMAND ----------

select * from f1_raw.lap_times

-- COMMAND ----------

select count(1) from f1_raw.lap_times

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### Create qualifying table
-- MAGIC - JSON file
-- MAGIC - Multiline JSON
-- MAGIC - Multiple Files

-- COMMAND ----------

drop table if exists f1_raw.qualifying;

create table if not exists f1_raw.qualifying
(
constructorId INT,driverId INT,number INT, position INT, q1 STRING, q2 STRING, q3 STRING, qualifyingId INT, raceId INT
)
using json
options (path "/mnt/formula1cardatalake/raw/qualifying",multiLine true, header true)

-- COMMAND ----------

select * from f1_raw.qualifying


-- COMMAND ----------

