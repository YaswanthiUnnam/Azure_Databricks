-- Databricks notebook source
-- MAGIC %python
-- MAGIC html = """<h1 style = 'color:Black;text-align:center;font-family:Ariel'> Report on Dominant Formula 1 Teams</h1>"""
-- MAGIC displayHTML(html)

-- COMMAND ----------

create or replace temp view v_dominant_teams
AS
select team_name,count(1) as total_races, sum(calculated_points) as total_points,
avg(calculated_points) as avg_points,
RANK() OVER (ORDER BY avg(calculated_points) desc) team_rank
 from f1_presentation.calculated_race_results
 where year between 2011 and 2020
 group by team_name
 having count(1) >=100
 order by avg_points desc

-- COMMAND ----------

select * from v_dominant_teams

-- COMMAND ----------

select year, team_name,count(1) as total_races, sum(calculated_points) as total_points,
avg(calculated_points) as avg_points
 from f1_presentation.calculated_race_results
 where team_name in (select team_name from v_dominant_teams where team_rank <= 5)
 group by year, team_name
 
 order by year desc

-- COMMAND ----------

select year, team_name,count(1) as total_races, sum(calculated_points) as total_points,
avg(calculated_points) as avg_points
 from f1_presentation.calculated_race_results
 where team_name in (select team_name from v_dominant_teams where team_rank <= 5)
 group by year, team_name
 
 order by year desc