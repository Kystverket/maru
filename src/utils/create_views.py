# Databricks notebook source
import os

ENV: str = os.getenv("ENVIRONMENT")

# COMMAND ----------

spark.sql(
    f"""
CREATE OR REPLACE VIEW gold_{ENV}.maru.v_municipality_voyage_route_top_100 AS
WITH tmp_sails AS (
SELECT 
    sail_id,
    municipality_voyage_route,
    vessel_type,
    gt_group,
    MIN(year_month) AS year_month_start,
    SUM(sum_seconds) AS sum_seconds,
    SUM(sum_kwh) AS sum_kwh,
    SUM(sum_fuel) AS sum_fuel,
    SUM(sum_co2) AS sum_co2,
    SUM(sum_nmvoc) AS sum_nmvoc,
    SUM(sum_co) AS sum_co,
    SUM(sum_ch4) AS sum_ch4,
    SUM(sum_n2o) AS sum_n2o,
    SUM(sum_sox) AS sum_sox,
    SUM(sum_pm10) AS sum_pm10,
    SUM(sum_pm2_5) AS sum_pm2_5,
    SUM(sum_nox) AS sum_nox,
    SUM(sum_bc) AS sum_bc,
    SUM(sum_co2e) AS sum_co2e,
    SUM(distance_kilometers) AS distance_kilometers
FROM gold_{ENV}.maru.maru_report
WHERE municipality_voyage_route IS NOT NULL
GROUP BY ALL
)

,tmp_ranked_routes AS (
  SELECT
    municipality_voyage_route,
    sum(sum_co2e) as co2e_route_total,
    row_number() OVER (ORDER BY sum(sum_co2e) DESC) as rank
  FROM tmp_sails
  GROUP BY municipality_voyage_route
  ORDER BY co2e_route_total DESC
  LIMIT 100
)

SELECT
  sails.municipality_voyage_route,
  rank,
  co2e_route_total,
  vessel_type,
  gt_group,
  LEFT(year_month_start, 4) AS year,
  year_month_start AS year_month,
  COUNT(DISTINCT sail_id) AS count_sails,
  SUM(sum_seconds) AS sum_seconds,
  SUM(sum_kwh) AS sum_kwh,
  SUM(sum_fuel) AS sum_fuel,
  SUM(sum_co2) AS sum_co2,
  SUM(sum_nmvoc) AS sum_nmvoc,
  SUM(sum_co) AS sum_co,
  SUM(sum_ch4) AS sum_ch4,
  SUM(sum_n2o) AS sum_n2o,
  SUM(sum_sox) AS sum_sox,
  SUM(sum_pm10) AS sum_pm10,
  SUM(sum_pm2_5) AS sum_pm2_5,
  SUM(sum_nox) AS sum_nox,
  SUM(sum_bc) AS sum_bc,
  SUM(sum_co2e) AS sum_co2e,
  SUM(distance_kilometers) AS distance_kilometers
FROM tmp_sails sails
INNER JOIN tmp_ranked_routes routes ON sails.municipality_voyage_route = routes.municipality_voyage_route
GROUP BY ALL
"""
)

# COMMAND ----------

spark.sql(
    f"""
CREATE OR REPLACE VIEW gold_{ENV}.maru.v_maru_report AS
SELECT 
      mmsi,
      vessel_id,
      year_month,
      gt_group,
      gt,
      vessel_type,
      degree_of_electrification,
      main_engine_fueltype,
      phase,
      voyage_type,
      maritime_borders_norwegian_economic_zone_id,
      maritime_borders_norwegian_economic_zone_area_name,
      management_plan_marine_areas_area_id,
      management_plan_marine_areas_area_name_norwegian,
      municipality_id,
      municipality_name,
      county_id,
      county_name,
      municipality_voyage_type,
      unlocode_country_code,
      unlocode_location_code,
      in_coast_and_sea_area,
      in_norwegian_continental_shelf,
      version,
      SUM(sum_seconds) as sum_seconds,
      SUM(sum_kwh) as sum_kwh,
      SUM(sum_fuel) as sum_fuel,
      SUM(sum_co2) as sum_co2,
      SUM(sum_nmvoc) as sum_nmvoc,
      SUM(sum_co) as sum_co,
      SUM(sum_ch4) as sum_ch4,
      SUM(sum_n2o) as sum_n2o,
      SUM(sum_sox) as sum_sox,
      SUM(sum_pm10) as sum_pm10,
      SUM(sum_pm2_5) as sum_pm2_5,
      SUM(sum_nox) as sum_nox,
      SUM(sum_bc) as sum_bc,
      SUM(sum_co2e) as sum_co2e,
      SUM(distance_kilometers) as distance_kilometers
FROM gold_{ENV}.maru.maru_report
WHERE version = "v1.3.0"
    AND in_coast_and_sea_area = true
    AND year_month < date_format(current_date(), "yyyy-MM")
GROUP BY ALL
"""
)
