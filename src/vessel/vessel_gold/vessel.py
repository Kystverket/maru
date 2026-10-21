# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
import os

# COMMAND ----------
# MAGIC %md #Parameters
# COMMAND ----------
import sys

import pyspark.sql.functions as F
from pyspark.sql.functions import (
    array,
    coalesce,
    col,
    lit,
    row_number,
    to_date,
)
from pyspark.sql.window import Window

sys.path.append(os.path.abspath("../../"))
sys.path.append(os.path.abspath("../../../../"))
from transformations import (
    add_dnv_engine_nox_rating,
    add_enriched_columns,
    create_vessel_current_table,
    create_vessel_historical_table,
)
from utils.config import VERSION
from utils.read_data import read_dnv_engine_nox_rating

dbutils.widgets.text("ENVIRONMENT", "dev")
ENV: str = dbutils.widgets.get("ENVIRONMENT")
os.environ["ENVIRONMENT"] = ENV
spark.conf.set("spark.sql.ansi.enabled", "false")

SOURCE_SHIPTYPE_MAPPINGS_TABLE_FULL_PATH = (
    "silver_prod.shiptype_mappings.shiptype_mappings"
)

SOURCE_VESSEL_COMBINED_TABLE_FULL_PATH = "gold_prod.ais.qsr_shipdata_combined_scd2_v3"
SOURCE_AUX_BOILER_KWH_TABLE_FULL_PATH = (
    f"gold_{ENV}.maru.vessel_aux_boiler_kw_{VERSION}"
)
SOURCE_VESSEL_BATTERY_TABLE_FULL_PATH = f"gold_{ENV}.maru.vessel_battery_{VERSION}"
SOURCE_VESSEL_TYPE_TABLE_FULL_PATH = f"gold_{ENV}.maru.vessel_type_{VERSION}"

TARGET_MARU_VESSEL_HISTORICAL_TABLE_FULL_PATH = (
    f"gold_{ENV}.maru.dim_vessel_historical_{VERSION}"
)
TARGET_MARU_VESSEL_CURRENT_TABLE_FULL_PATH = (
    f"gold_{ENV}.maru.dim_vessel_current_{VERSION}"
)

VESSEL_UNIT_AUX_BOILER_KW = ["gt", "dwt", "teu", "cbm", "m"]

# COMMAND ----------

# MAGIC %md #Create schema and table

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS gold_{ENV}.maru")

# COMMAND ----------

create_vessel_historical_table(spark, TARGET_MARU_VESSEL_HISTORICAL_TABLE_FULL_PATH)

# COMMAND ----------

create_vessel_current_table(spark, TARGET_MARU_VESSEL_CURRENT_TABLE_FULL_PATH)

# COMMAND ----------

# MAGIC %md #Read data

# COMMAND ----------

# MAGIC %md ##Vessel data

# COMMAND ----------

df_shipdata_combined_imputated = (
    spark.table(SOURCE_VESSEL_COMBINED_TABLE_FULL_PATH)
    .where("!(mmsi=257657900 and ship_name='TARGA') and !(mmsi=235112231)")
    .withColumn(
        "imo",
        F.coalesce(
            F.when(F.col("imo_ais") != 0, F.col("imo_ais")),
            F.col("imo_spglobal"),
        ),
    )
    .drop("imo_ais", "imo_spglobal")
    .withColumnRenamed("vessel_id_hash", "vessel_id")
)  # (DATA-1499), (DATA-1722)

df_shipdata_combined_imo_mmsi = (
    spark.table(SOURCE_VESSEL_COMBINED_TABLE_FULL_PATH)
    .where("mmsi < 776000000 and mmsi != 235112231")
    .withColumnRenamed(  # (DATA-1722)
        "vessel_id_hash", "vessel_id"
    )
)

df_shiptype_mappings = (
    spark.table(SOURCE_SHIPTYPE_MAPPINGS_TABLE_FULL_PATH)
    .where("target_vocabulary ='maru' AND source_vocabulary='statcode5'")
    .select("source_id", "target_id")
)

df_battery_electric = spark.table(SOURCE_VESSEL_BATTERY_TABLE_FULL_PATH)

df_vessel_type = spark.table(SOURCE_VESSEL_TYPE_TABLE_FULL_PATH)

# COMMAND ----------

# MAGIC %md ###QSR shipdata

# COMMAND ----------

# MAGIC %md ### Aux and boiler kwh

# COMMAND ----------

# Window for removing duplicates (if any)
window = Window.partitionBy("vessel_type", "size_bin").orderBy("low_limit")

# Read Aux and Boiler kw
df_aux_boiler_in = (
    spark.table(SOURCE_AUX_BOILER_KWH_TABLE_FULL_PATH)
    .withColumn("row_number", row_number().over(window))
    .where("row_number == 1")
    .drop("row_number")
)

# COMMAND ----------

# MAGIC %md #Transform

# COMMAND ----------

# MAGIC %md ##Merge data from ship registers

# COMMAND ----------

com_imp = df_shipdata_combined_imputated.alias("com_imp")
com_filtered = (
    df_shipdata_combined_imo_mmsi.select("vessel_id").distinct().alias("com_filtered")
)
ele = df_battery_electric.alias("ele")
vty = df_vessel_type.alias("vty")
stm = df_shiptype_mappings.alias("stm")

df_vessel_joined = (
    com_imp.join(com_filtered, on="vessel_id", how="inner")
    .join(
        ele,
        on=(
            (com_imp["vessel_id"] == ele["vessel_id"])
            & (
                ele["start_date"]
                < coalesce(to_date(com_imp["__END_AT"]), to_date(lit("2999-12-31")))
            )
            & (
                coalesce(ele["end_date"], to_date(lit("2999-12-31")))
                > to_date(com_imp["__START_AT"])
            )
        ),
        how="left",
    )
    .withColumn(
        "_battery_rn",
        row_number().over(
            Window.partitionBy(com_imp["vessel_id"], com_imp["__START_AT"]).orderBy(
                ele["start_date"].desc_nulls_last()
            )
        ),
    )
    .where(col("_battery_rn") == 1)
    .drop("_battery_rn")
    .join(vty, on="statcode5", how="left")
    .join(stm, com_imp.statcode5 == stm.source_id, how="left")
    .select(
        com_imp.vessel_id,
        array(com_imp.mmsi.cast("int")).alias("mmsi"),
        array(coalesce(com_imp.imo, lit(0)).cast("int")).alias("imo"),
        array(com_imp.call_sign).alias("call_sign"),
        coalesce(com_imp.ship_name, lit("Unknown")).alias("ship_name"),
        coalesce(com_imp.flag_code, lit("Unknown")).alias("flag_code"),
        coalesce(stm.target_id, lit("Unknown")).alias("vessel_type_imo_ghg"),
        coalesce(vty.vessel_type, lit("Unknown")).alias("vessel_type_maru"),
        coalesce(com_imp.statcode5, lit("Unknown")).alias("statcode5"),
        coalesce(com_imp.year_of_build, lit(-99)).alias("year_of_build"),
        coalesce(com_imp.gross_tonnage, lit(-99)).cast("double").alias("gt"),
        coalesce(com_imp.deadweight_tonnage, lit(-99)).alias("dwt"),
        coalesce(com_imp.twentyfoot_equivalent_unit, lit(-99)).alias("teu"),
        coalesce(com_imp.liquid_capacity, com_imp.gas_capacity, lit(-99)).alias("cbm"),
        coalesce(com_imp.length_overall, lit(-99)).alias("m"),
        coalesce(com_imp.breadth_moulded, lit(-99)).alias("breadth_moulded"),
        coalesce(com_imp.draught, lit(-99)).alias("draught"),
        coalesce(com_imp.ship_type_text, lit("Unknown")).alias("ship_type"),
        coalesce(com_imp.main_engine_rpm, lit(-99)).alias("main_engine_rpm"),
        coalesce(com_imp.power_kw_max, lit(0)).alias("main_engine_kw"),
        coalesce(com_imp.stroke_type, lit(-99)).cast("int").alias("stroketype"),
        coalesce(com_imp.speed_service, lit(-99)).alias("speed_service"),
        coalesce(com_imp.speed_max, lit(-99)).alias("speed_max"),
        coalesce(com_imp.power_kw_max, lit(0)).alias("power_kw_max"),
        coalesce(com_imp.fuel_type1_code, lit("UU")).alias("fp_fueltype1code"),
        coalesce(com_imp.fuel_type2_code, lit("UU")).alias("fp_fueltype2code"),
        coalesce(com_imp.main_engine_type, lit("Unknown")).alias("fp_me_enginetype"),
        coalesce(com_imp.propulsion_type, lit("Unknown")).alias("fp_propulsiontype"),
        coalesce(com_imp.main_engine_builder, lit("Unknown")).alias("engine_builder"),
        coalesce(com_imp.main_engine_model, lit("Unknown")).alias("engine_model"),
        coalesce(com_imp.length_between_perpendiculars_lbp, lit(-99)).alias(
            "fp_lengthbetweenperpendicularslbp"
        ),
        coalesce(com_imp.breadth_extreme, lit(-99)).alias("fp_breadthextreme"),
        coalesce(com_imp.displacement, lit(-99)).cast("int").alias("fp_displacement"),
        ele.degree_of_electrification,
        to_date(com_imp["__START_AT"]).alias("start_date"),
        coalesce(to_date(com_imp["__END_AT"]), to_date(lit("2999-12-31"))).alias(
            "end_date"
        ),
        coalesce(ele.battery_pack_kwh, lit(-99)).alias("battery_pack_kwh"),
        coalesce(com_imp.propulsion_type_code, lit("UU")).alias(
            "fp_propulsiontypecode"
        ),
        com_imp.main_engine_fueltype,
        com_imp.main_engine_fueltype_original,
        com_imp.main_engine_enginetype,
        com_imp.msg_type.cast("smallint").alias("msg_type"),
    )
)

# COMMAND ----------

# MAGIC %md ## Add aux and boiler kwh

# COMMAND ----------

# Merge Vessel dataframe with Aux and boiler kw

unpivot_id_columns = ["vessel_id", "vessel_type_imo_ghg", "start_date"]
unpivot_value_columns = VESSEL_UNIT_AUX_BOILER_KW
unpivot_dataframe_columns = unpivot_id_columns + unpivot_value_columns

# # Unpivot df_vessel to prepare for join with aux and boiler kw dataframe
df_vessel_joined_selected = df_vessel_joined.select(unpivot_dataframe_columns)
df_vessel_unpivot_unit = df_vessel_joined_selected.unpivot(
    ids=unpivot_id_columns,
    values=unpivot_value_columns,
    variableColumnName="unit",
    valueColumnName="unit_value",
)

# Join Vessel dataframe with Aux and Boiler kw dataframe. Each vessel_type_imo_ghg has one unit
df_vessel_aux_boiler_kwh = (
    df_vessel_unpivot_unit.join(
        df_aux_boiler_in,
        (df_vessel_unpivot_unit.vessel_type_imo_ghg == df_aux_boiler_in.vessel_type)
        & (df_vessel_unpivot_unit.unit == df_aux_boiler_in.unit)
        & (df_vessel_unpivot_unit.unit_value >= df_aux_boiler_in.low_limit)
        & (df_vessel_unpivot_unit.unit_value < df_aux_boiler_in.high_limit),
        how="inner",
    )
    .select(
        df_vessel_unpivot_unit["*"],
        df_aux_boiler_in["aux_berth_kw"],
        df_aux_boiler_in["aux_anchor_kw"],
        df_aux_boiler_in["aux_maneuver_kw"],
        df_aux_boiler_in["aux_cruise_kw"],
        df_aux_boiler_in["boiler_berth_kw"],
        df_aux_boiler_in["boiler_anchor_kw"],
        df_aux_boiler_in["boiler_maneuver_kw"],
        df_aux_boiler_in["boiler_cruise_kw"],
    )
    .drop("unit", "unit_value", "vessel_type_imo_ghg")
)


# COMMAND ----------

df_vessel = df_vessel_joined.join(
    df_vessel_aux_boiler_kwh, on=["vessel_id", "start_date"], how="inner"
)
df_vessel = df_vessel.withColumnRenamed("m", "length_overall")

# COMMAND ----------

# MAGIC %md ##Cleaning and enrichment

# COMMAND ----------

df_dnv_engine_nox_rating = read_dnv_engine_nox_rating(spark, ENV)
df_vessel = add_dnv_engine_nox_rating(df_vessel, df_dnv_engine_nox_rating)
df_vessel_enriched = add_enriched_columns(df_vessel)

# COMMAND ----------

# MAGIC %md ###Select columns

# COMMAND ----------

df_vessel_selected = df_vessel_enriched.select(
    "vessel_id",
    "start_date",
    "end_date",
    "mmsi",
    "imo",
    "call_sign",
    "ship_name",
    "flag_code",
    "vessel_type_imo_ghg",
    "vessel_type_maru",
    "statcode5",
    "year_of_build",
    "gt",
    "dwt",
    "teu",
    "cbm",
    "length_overall",
    "breadth_moulded",
    "draught",
    "ship_type",
    "main_engine_rpm",
    "main_engine_kw",
    "stroketype",
    "speed_service",
    "speed_max",
    "power_kw_max",
    "fp_fueltype1code",
    "fp_fueltype2code",
    "fp_me_enginetype",
    "fp_propulsiontype",
    "fp_breadthextreme",
    "fp_displacement",
    "fp_lengthbetweenperpendicularslbp",
    "engine_builder",
    "engine_model",
    "degree_of_electrification",
    "aux_berth_kw",
    "aux_anchor_kw",
    "aux_maneuver_kw",
    "aux_cruise_kw",
    "boiler_berth_kw",
    "boiler_anchor_kw",
    "boiler_maneuver_kw",
    "boiler_cruise_kw",
    "length_group",
    "tier_group",
    "tier_group_nox",
    "engine_nox_rating",
    "year_group",
    "gt_group",
    "dwt_group",
    "fp_propulsiontypecode",
    "main_engine_fueltype",
    "main_engine_fueltype_original",
    "main_engine_enginetype",
    "msg_type",
).orderBy("vessel_id", "start_date")

# COMMAND ----------

# MAGIC
# MAGIC %md #QC

# COMMAND ----------

# Check that input and output dataframe of unpivot and join have equal length (current records only)
count_expected = (
    com_imp.where(col("__END_AT").isNull())
    .join(com_filtered, on="vessel_id", how="inner")
    .join(
        stm, com_imp.statcode5 == stm.source_id, how="inner"
    )  # vessel_type_imo_ghg is not null"
    .count()
)
count_actual = df_vessel_selected.where("end_date = '2999-12-31'").count()
assert (  # noqa: S101
    count_expected == count_actual
), f"Expected: {count_expected}, actual: {count_actual}"

# COMMAND ----------

# MAGIC %md #Write data

# COMMAND ----------


# Write historical table
(
    df_vessel_selected.write.clusterBy("vessel_id", "start_date", "end_date")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(name=TARGET_MARU_VESSEL_HISTORICAL_TABLE_FULL_PATH)
)

# COMMAND ----------


# Write current table
(
    spark.table(TARGET_MARU_VESSEL_HISTORICAL_TABLE_FULL_PATH)
    .where("end_date = '2999-12-31'")
    .drop("start_date", "end_date")
    .write.clusterBy("vessel_id")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(name=TARGET_MARU_VESSEL_CURRENT_TABLE_FULL_PATH)
)
