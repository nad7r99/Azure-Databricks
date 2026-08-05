-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Build Constructor Standings
-- MAGIC
-- MAGIC #### Sources
-- MAGIC - fact_session_results
-- MAGIC -  dim_constructors
-- MAGIC
-- MAGIC #### Output Columns
-- MAGIC - season
-- MAGIC - constructor id
-- MAGIC - constructor name
-- MAGIC - nationality
-- MAGIC - race starts
-- MAGIC - total points
-- MAGIC - number of wins
-- MAGIC - number of podiums
-- MAGIC - standing position

-- COMMAND ----------

-- DBTITLE 1,Cell 2
create or replace view formula1_project.gold.Constructors_standing_vw 
as 
with Constructors_standing_view 
as (
select r.season,
       r.constructor_id,
       c.constructor_name,
       c.nationality,
       count(*) as race_starts,
       sum(r.points) as total_points,
       count_if(r.is_win) as number_of_wins,
       count_if(r.is_podium) as number_of_podiums

from formula1_project.gold.fact_session_result r
join formula1_project.gold.dim_constructors c
     on c.constructor_id = r.constructor_id


group by r.season,
         r.constructor_id,
         c.constructor_name,
         c.nationality

order by total_points desc, number_of_wins desc)
select season,
       constructor_id,
       constructor_name,
       nationality,
       rank() over(partition by season order by total_points desc, number_of_wins desc) as standing,
       race_starts,
       total_points,
       number_of_wins,
       number_of_podiums
from Constructors_standing_view

-- COMMAND ----------


select season,
       constructor_id,
       constructor_name,
       nationality,
       standing,
       race_starts,
       total_points,
       number_of_wins,
       number_of_podiums
from formula1_project.gold.Constructors_standing_vw         
where season = 2025