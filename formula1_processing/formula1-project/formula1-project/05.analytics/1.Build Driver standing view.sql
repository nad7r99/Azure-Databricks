-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Build Driver Standings
-- MAGIC
-- MAGIC #### Sources
-- MAGIC - fact_session_results
-- MAGIC - dim_drivers
-- MAGIC
-- MAGIC #### Output Columns
-- MAGIC - season
-- MAGIC - driver id
-- MAGIC - driver name
-- MAGIC - nationality
-- MAGIC - race starts
-- MAGIC - total points
-- MAGIC - number of wins
-- MAGIC - number of podiums
-- MAGIC - standing position

-- COMMAND ----------

CREATE OR REPLACE VIEW formula1_project.gold.vw_driver_standing
AS
WITH driver_session_summary
AS
  (SELECT r.season,
        d.driver_id,
        d.driver_name,
        d.nationality,
        COUNT(*) AS race_starts,
        SUM(r.points) AS total_points,
        COUNT_IF(r.is_win) AS number_of_wins,
        COUNT_IF(r.is_podium) AS number_of_podiums
    FROM formula1_project.gold.fact_session_result r
    JOIN formula1_project.gold.dim_drivers d
      ON r.driver_id = d.driver_id 
  GROUP BY r.season,
        d.driver_id,
        d.driver_name,
        d.nationality)    
SELECT season,
       driver_id,
       driver_name,
       nationality,
       RANK() OVER (PARTITION BY season ORDER BY total_points DESC, number_of_wins DESC) AS standing,
       race_starts,
       total_points,
       number_of_wins,
       number_of_podiums
  FROM driver_session_summary;



-- COMMAND ----------

select * from formula1_project.gold.vw_driver_standing
where season = 2025