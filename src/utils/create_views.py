# Databricks notebook source
import os
import sys

sys.path.append(os.path.abspath(".."))
from utils.config import VERSION

dbutils.widgets.text("ENVIRONMENT", "dev")
ENV: str = dbutils.widgets.get("ENVIRONMENT")
os.environ["ENVIRONMENT"] = ENV
spark.conf.set("spark.sql.ansi.enabled", "false")
OLD_VERSION = "v1_8"

# COMMAND ----------

spark.sql(
    f"""
CREATE OR REPLACE VIEW gold_{ENV}.maru.v_municipality_voyage_route_top_100_{VERSION} AS
WITH tmp_sails AS (
SELECT
    sail_id,
    municipality_voyage_route,
    vessel_type,
    gt_group,
    MIN(year_month) AS year_month_start,
    SUM(sum_seconds) AS sum_seconds,
    SUM(sum_kwh) AS sum_kwh,
    SUM(sum_fuel_main_aux_boiler_engines_tonnes) AS sum_fuel_main_aux_boiler_engines_tonnes,
    SUM(sum_fuel_mdo_equivalent_tonnes) AS sum_fuel_mdo_equivalent_tonnes,
    SUM(sum_co2_tonnes) AS sum_co2_tonnes,
    SUM(sum_nmvoc_tonnes) AS sum_nmvoc_tonnes,
    SUM(sum_co_tonnes) AS sum_co_tonnes,
    SUM(sum_ch4_tonnes) AS sum_ch4_tonnes,
    SUM(sum_n2o_tonnes) AS sum_n2o_tonnes,
    SUM(sum_sox_tonnes) AS sum_sox_tonnes,
    SUM(sum_pm10_tonnes) AS sum_pm10_tonnes,
    SUM(sum_pm2_5_tonnes) AS sum_pm2_5_tonnes,
    SUM(sum_nox_tonnes) AS sum_nox_tonnes,
    SUM(sum_bc_tonnes) AS sum_bc_tonnes,
    SUM(sum_co2e_tonnes) AS sum_co2e_tonnes,
    SUM(distance_kilometers) AS distance_kilometers
FROM gold_{ENV}.maru.dm_maru_report_{VERSION}
WHERE municipality_voyage_route IS NOT NULL
  AND in_coast_and_sea_area = true
  AND year_month < date_format(current_date(), "yyyy-MM")
GROUP BY ALL
)

,tmp_ranked_routes AS (
  SELECT
    municipality_voyage_route,
    SUM(sum_co2e_tonnes) as co2e_route_total,
    ROW_NUMBER() OVER (ORDER BY SUM(sum_co2e_tonnes) DESC) as rank
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
  SUM(sum_fuel_main_aux_boiler_engines_tonnes) AS sum_fuel_main_aux_boiler_engines_tonnes,
  SUM(sum_fuel_mdo_equivalent_tonnes) AS sum_fuel_mdo_equivalent_tonnes,
  ROUND(SUM(sum_co2_tonnes), 4) as sum_co2_tonnes,
  ROUND(SUM(sum_nmvoc_tonnes), 4) as sum_nmvoc_tonnes,
  ROUND(SUM(sum_co_tonnes), 4) as sum_co_tonnes,
  ROUND(SUM(sum_ch4_tonnes), 4) as sum_ch4_tonnes,
  ROUND(SUM(sum_n2o_tonnes), 4) as sum_n2o_tonnes,
  ROUND(SUM(sum_sox_tonnes), 4) as sum_sox_tonnes,
  ROUND(SUM(sum_pm10_tonnes), 4) as sum_pm10_tonnes,
  ROUND(SUM(sum_pm2_5_tonnes), 4) as sum_pm2_5_tonnes,
  ROUND(SUM(sum_nox_tonnes), 4) as sum_nox_tonnes,
  ROUND(SUM(sum_bc_tonnes), 4) as sum_bc_tonnes,
  ROUND(SUM(sum_co2e_tonnes), 4) as sum_co2e_tonnes,
  SUM(distance_kilometers) AS distance_kilometers
FROM tmp_sails sails
INNER JOIN tmp_ranked_routes routes ON sails.municipality_voyage_route = routes.municipality_voyage_route
GROUP BY ALL
"""
)

# COMMAND ----------

spark.sql(
    f"""
CREATE OR REPLACE VIEW gold_{ENV}.maru.v_maru_report_{VERSION} AS
SELECT
      mmsi,
      vessel_id,
      year,
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
      in_coast_and_sea_area,
      in_norwegian_continental_shelf,
      version,
      SUM(sum_seconds) as sum_seconds,
      ROUND(SUM(sum_kwh), 4) as sum_kwh,
      ROUND(SUM(sum_kwh_battery), 4) as sum_kwh_battery,
      ROUND(SUM(sum_aux_kwh_shore_power), 4) as sum_aux_kwh_shore_power,
      ROUND(SUM(sum_boiler_kwh_shore_power), 4) as sum_boiler_kwh_shore_power,
      ROUND(SUM(sum_fuel_main_aux_boiler_engines_tonnes), 4) as sum_fuel_main_aux_boiler_engines_tonnes,
      ROUND(SUM(sum_fuel_mdo_equivalent_tonnes), 4) as sum_fuel_mdo_equivalent_tonnes,
      ROUND(SUM(sum_co2_tonnes), 4) as sum_co2_tonnes,
      ROUND(SUM(sum_nmvoc_tonnes), 4) as sum_nmvoc_tonnes,
      ROUND(SUM(sum_co_tonnes), 4) as sum_co_tonnes,
      ROUND(SUM(sum_ch4_tonnes), 4) as sum_ch4_tonnes,
      ROUND(SUM(sum_n2o_tonnes), 4) as sum_n2o_tonnes,
      ROUND(SUM(sum_sox_tonnes), 4) as sum_sox_tonnes,
      ROUND(SUM(sum_pm10_tonnes), 4) as sum_pm10_tonnes,
      ROUND(SUM(sum_pm2_5_tonnes), 4) as sum_pm2_5_tonnes,
      ROUND(SUM(sum_nox_tonnes), 4) as sum_nox_tonnes,
      ROUND(SUM(sum_bc_tonnes), 4) as sum_bc_tonnes,
      ROUND(SUM(sum_co2e_tonnes), 4) as sum_co2e_tonnes,
      SUM(distance_kilometers) as distance_kilometers
FROM gold_{ENV}.maru.dm_maru_report_{VERSION}
WHERE in_coast_and_sea_area = true
    AND year_month < date_format(current_date(), "yyyy-MM")
GROUP BY ALL
"""
)

# COMMAND ----------

spark.sql(
    f"""
CREATE OR REPLACE VIEW gold_{ENV}.maru.v_municipality_voyage_route_top_100_{OLD_VERSION} AS
WITH tmp_sails AS (
SELECT
    sail_id,
    municipality_voyage_route,
    vessel_type,
    gt_group,
    MIN(year_month) AS year_month_start,
    SUM(sum_seconds) AS sum_seconds,
    SUM(sum_kwh) AS sum_kwh,
    SUM(sum_fuel_main_aux_boiler_engines_tonnes) AS sum_fuel_tonnes,
    SUM(sum_co2_tonnes) AS sum_co2_tonnes,
    SUM(sum_nmvoc_tonnes) AS sum_nmvoc_tonnes,
    SUM(sum_co_tonnes) AS sum_co_tonnes,
    SUM(sum_ch4_tonnes) AS sum_ch4_tonnes,
    SUM(sum_n2o_tonnes) AS sum_n2o_tonnes,
    SUM(sum_sox_tonnes) AS sum_sox_tonnes,
    SUM(sum_pm10_tonnes) AS sum_pm10_tonnes,
    SUM(sum_pm2_5_tonnes) AS sum_pm2_5_tonnes,
    SUM(sum_nox_tonnes) AS sum_nox_tonnes,
    SUM(sum_bc_tonnes) AS sum_bc_tonnes,
    SUM(sum_co2e_tonnes) AS sum_co2e_tonnes,
    SUM(distance_kilometers) AS distance_kilometers
FROM gold_{ENV}.maru.dm_maru_report_{OLD_VERSION}
WHERE municipality_voyage_route IS NOT NULL
  AND in_coast_and_sea_area = true
  AND year_month < date_format(current_date(), "yyyy-MM")
GROUP BY ALL
)

,tmp_ranked_routes AS (
  SELECT
    municipality_voyage_route,
    SUM(sum_co2e_tonnes) as co2e_route_total,
    ROW_NUMBER() OVER (ORDER BY SUM(sum_co2e_tonnes) DESC) as rank
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
  SUM(sum_fuel_tonnes) AS sum_fuel_tonnes,
  ROUND(SUM(sum_co2_tonnes), 4) as sum_co2_tonnes,
  ROUND(SUM(sum_nmvoc_tonnes), 4) as sum_nmvoc_tonnes,
  ROUND(SUM(sum_co_tonnes), 4) as sum_co_tonnes,
  ROUND(SUM(sum_ch4_tonnes), 4) as sum_ch4_tonnes,
  ROUND(SUM(sum_n2o_tonnes), 4) as sum_n2o_tonnes,
  ROUND(SUM(sum_sox_tonnes), 4) as sum_sox_tonnes,
  ROUND(SUM(sum_pm10_tonnes), 4) as sum_pm10_tonnes,
  ROUND(SUM(sum_pm2_5_tonnes), 4) as sum_pm2_5_tonnes,
  ROUND(SUM(sum_nox_tonnes), 4) as sum_nox_tonnes,
  ROUND(SUM(sum_bc_tonnes), 4) as sum_bc_tonnes,
  ROUND(SUM(sum_co2e_tonnes), 4) as sum_co2e_tonnes,
  SUM(distance_kilometers) AS distance_kilometers
FROM tmp_sails sails
INNER JOIN tmp_ranked_routes routes
  ON sails.municipality_voyage_route = routes.municipality_voyage_route
GROUP BY ALL
"""
)


# COMMAND ----------

spark.sql(
    f"""
CREATE OR REPLACE VIEW gold_{ENV}.maru.v_maru_report_{OLD_VERSION} AS
SELECT
      mmsi,
      vessel_id,
      year,
      month,
      year_month,
      imo,
      ship_name,
      call_sign,
      statcode5,
      shiptype_level5,
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
      in_coast_and_sea_area,
      in_norwegian_continental_shelf,
      sail_id,
      municipality_voyage_route,
      version,
      SUM(sum_seconds) as sum_seconds,
      ROUND(SUM(sum_kwh), 4) as sum_kwh,
      ROUND(SUM(sum_kwh_battery), 4) as sum_kwh_battery,
      ROUND(SUM(sum_aux_kwh_shore_power), 4) as sum_aux_kwh_shore_power,
      ROUND(SUM(sum_boiler_kwh_shore_power), 4) as sum_boiler_kwh_shore_power,
      ROUND(SUM(sum_fuel_main_aux_boiler_engines_tonnes), 4) as sum_fuel_tonnes,
      ROUND(SUM(sum_co2_tonnes), 4) as sum_co2_tonnes,
      ROUND(SUM(sum_nmvoc_tonnes), 4) as sum_nmvoc_tonnes,
      ROUND(SUM(sum_co_tonnes), 4) as sum_co_tonnes,
      ROUND(SUM(sum_ch4_tonnes), 4) as sum_ch4_tonnes,
      ROUND(SUM(sum_n2o_tonnes), 4) as sum_n2o_tonnes,
      ROUND(SUM(sum_sox_tonnes), 4) as sum_sox_tonnes,
      ROUND(SUM(sum_pm10_tonnes), 4) as sum_pm10_tonnes,
      ROUND(SUM(sum_pm2_5_tonnes), 4) as sum_pm2_5_tonnes,
      ROUND(SUM(sum_nox_tonnes), 4) as sum_nox_tonnes,
      ROUND(SUM(sum_bc_tonnes), 4) as sum_bc_tonnes,
      ROUND(SUM(sum_co2e_tonnes), 4) as sum_co2e_tonnes,
      SUM(distance_kilometers) as distance_kilometers,
      MAX(bi_processing_time) as bi_processing_time,
      MAX(shore_power_is_connected) as shore_power_is_connected
FROM gold_{ENV}.maru.dm_maru_report_{OLD_VERSION}
WHERE in_coast_and_sea_area = true
  AND year_month < date_format(current_date(), "yyyy-MM")
GROUP BY ALL
"""
)
