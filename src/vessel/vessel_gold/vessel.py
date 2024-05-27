# Databricks notebook source
import os

from pyspark.sql import DataFrame
from pyspark.sql.functions import coalesce, col, lit, lower, mode, row_number, when
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md #Parameters

# COMMAND ----------

ENV: str = os.getenv("ENVIRONMENT")

FULL_TABLE_NAME_AUX_BOILER_KWH = f"silver_{ENV}.maru.vessel_aux_boiler_kw"
FULL_TABLE_NAME_FAIRPLAY_MAINENGINE = f"gold_{ENV}.fairplay.tblmainengines"
FULL_TABLE_NAME_FAIRPLAY_SHIPDATA = f"gold_{ENV}.fairplay.shipdata"
FULL_TABLE_NAME_MARU_VESSEL = f"gold_{ENV}.maru.vessel"
FULL_TABLE_NAME_VESSEL_BATTERY = f"silver_{ENV}.maru.vessel_battery"
FULL_TABLE_NAME_VESSEL_COMBINED_IMPUTATED = f"gold_{ENV}.shipdata.combined_imputated"
FULL_TABLE_NAME_VESSEL_COMBINED_IMPUTATED_IMO_MMSI = (
    f"gold_{ENV}.shipdata.combined_imputated_imo_mmsi"
)
FULL_TABLE_NAME_VESSEL_TYPE = f"silver_{ENV}.maru.vessel_type"

VESSEL_UNIT_AUX_BOILER_KW = ["gt", "dwt", "teu", "cbm"]

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS gold_{ENV}.maru")

# COMMAND ----------

# MAGIC %md #Read data

# COMMAND ----------

# MAGIC %md ##Vessel data

# COMMAND ----------

df_shipdata_combined = spark.table(FULL_TABLE_NAME_VESSEL_COMBINED_IMPUTATED)
df_shipdata_combined_imo_mmsi = spark.table(
    FULL_TABLE_NAME_VESSEL_COMBINED_IMPUTATED_IMO_MMSI
).where("row_number_mmsi_imo = 1 AND mmsi < 776000000")

df_battery_electric = (
    spark.table(FULL_TABLE_NAME_VESSEL_BATTERY)
    .fillna("0", subset="imo")
    .withColumn("mmsi", col("mmsi").cast("long"))
    .withColumn("imo", col("imo").cast("long"))
    .dropDuplicates(subset=["mmsi", "imo"])
)

df_battery_electric_vessel_id = (
    df_battery_electric.join(
        df_shipdata_combined_imo_mmsi, on=["imo", "mmsi"], how="inner"
    )
    .select(df_battery_electric["*"], df_shipdata_combined_imo_mmsi["vessel_id"])
    .dropDuplicates(subset=["vessel_id"])
)

df_vessel_type = spark.table(FULL_TABLE_NAME_VESSEL_TYPE)

# COMMAND ----------

# MAGIC %md ###Fairplay

# COMMAND ----------

df_fairplay = spark.sql(
    f"""
WITH tmp_mainengine AS (
SELECT
	lrno AS fp_imo, 
	FIRST_VALUE(enginebuilder) OVER (PARTITION BY lrno ORDER BY Position) as fp_me_enginebuilder,
	FIRST_VALUE(enginemodel) OVER (PARTITION BY lrno ORDER BY Position) as fp_me_enginemodel
FROM {FULL_TABLE_NAME_FAIRPLAY_MAINENGINE}
)

,tmp_mainengine_grouped AS (
SELECT
	fp_imo,
	min(fp_me_enginebuilder) as fp_me_enginebuilder,
	min(fp_me_enginemodel) as fp_me_enginemodel
FROM tmp_mainengine
GROUP BY fp_imo
)

SELECT 
	lrimoshipno as fp_imo,
	fueltype1code as fp_fueltype1code,
 	fueltype2code as fp_fueltype2code,
	MainEngineType as fp_me_enginetype,
	me.fp_me_enginebuilder,
	me.fp_me_enginemodel,
	propulsiontype as fp_propulsiontype
	FROM {FULL_TABLE_NAME_FAIRPLAY_SHIPDATA} fp
	LEFT JOIN tmp_mainengine_grouped me ON me.fp_imo = fp.lrimoshipno 
    WHERE `__END_AT` IS NULL
 """
)

# COMMAND ----------

# MAGIC %md ### Aux and boiler kwh

# COMMAND ----------

# Window for removing duplicates (if any)
window = Window.partitionBy("vessel_type", "size_bin").orderBy("low_limit")

# Read Aux and Boiler kw
df_aux_boiler_in = (
    spark.table(FULL_TABLE_NAME_AUX_BOILER_KWH)
    .withColumn("row_number", row_number().over(window))
    .filter("row_number == 1")
    .drop("row_number")
)

# COMMAND ----------

# MAGIC %md #Transform

# COMMAND ----------

# MAGIC %md ##Merge data from ship registers

# COMMAND ----------

com = df_shipdata_combined.alias("com")
com_filtered = (
    df_shipdata_combined_imo_mmsi.select("vessel_id").distinct().alias("com_filtered")
)
fpl = df_fairplay.alias("fpl")
ele = df_battery_electric_vessel_id.alias("ele")
vty = df_vessel_type.alias("vty")

df_vessel_joined = (
    com.join(com_filtered, on="vessel_id", how="inner")
    .join(fpl, fpl.fp_imo == com.fairplay_id, how="left")
    .join(ele, on="vessel_id", how="left")
    .join(vty, on="statcode5", how="left")
    .select(
        com.vessel_id,
        com.mmsi,
        com.imo,
        coalesce(com.call_sign, lit("Unknown")).alias("call_sign"),
        coalesce(com.ship_name, lit("Unknown")).alias("ship_name"),
        coalesce(com.flag_code, lit("Unknown")).alias("flag_code"),
        coalesce(com.vessel_type_imo_ghg, lit("Unknown")).alias("vessel_type_imo_ghg"),
        coalesce(vty.vessel_type, lit("Unknown")).alias("vessel_type_maru"),
        coalesce(com.statcode5, lit("Unknown")).alias("statcode5"),
        coalesce(com.year_of_build, lit(-99)).alias("year_of_build"),
        coalesce(com.gt, lit(-99)).alias("gt"),
        coalesce(com.dwt, lit(-99)).alias("dwt"),
        coalesce(com.teu, lit(-99)).alias("teu"),
        coalesce(com.liquid_capacity, com.gas_capacity, lit(-99)).alias("cbm"),
        coalesce(com.length_overall, lit(-99)).alias("length_overall"),
        coalesce(com.breadth_moulded, lit(-99)).alias("breadth_moulded"),
        coalesce(com.draught, lit(-99)).alias("draught"),
        coalesce(com.ship_type, lit("Unknown")).alias("ship_type"),
        coalesce(com.main_engine_rpm, lit(-99)).alias("main_engine_rpm"),
        coalesce(com.power_kw_max, lit(-99)).alias("main_engine_kw"),
        coalesce(com.main_engine_stroke_type, lit(-99)).alias("stroketype"),
        coalesce(com.speed_service, lit(-99)).alias("speed_service"),
        coalesce(com.speed_max, lit(-99)).alias("speed_max"),
        coalesce(com.power_kw_max, lit(-99)).alias("power_kw_max"),
        coalesce(fpl.fp_fueltype1code, lit("UU")).alias("fp_fueltype1code"),
        coalesce(fpl.fp_fueltype2code, lit("UU")).alias("fp_fueltype2code"),
        coalesce(fpl.fp_me_enginetype, lit("Unknown")).alias("fp_me_enginetype"),
        coalesce(fpl.fp_propulsiontype, lit("Unknown")).alias("fp_propulsiontype"),
        coalesce(fpl.fp_me_enginebuilder, lit("Unknown")).alias("engine_builder"),
        coalesce(fpl.fp_me_enginemodel, lit("Unknown")).alias("engine_model"),
        coalesce(ele.degree_of_electrification, lit(0)).alias(
            "degree_of_electrification"
        ),
        ele.battery_installation_year,
        ele.route,
        ele.battery_pack_kwh,
    )
)

# COMMAND ----------

# MAGIC %md ## Add aux and boiler kwh

# COMMAND ----------

# Merge Vessel dataframe with Aux and boiler kw

unpivot_id_columns = ["vessel_id", "vessel_type_imo_ghg"]
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
        & ((df_vessel_unpivot_unit.unit_value < df_aux_boiler_in.high_limit)),
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

df_vessel = df_vessel_joined.join(df_vessel_aux_boiler_kwh, on="vessel_id", how="inner")

# COMMAND ----------

# MAGIC %md ##Cleaning and enrichment

# COMMAND ----------

# MAGIC %md ### set_propulsiontypecode

# COMMAND ----------


def set_propulsiontypecode(df: DataFrame) -> DataFrame:
    """
    Sets propsulsiontype code from propulsiontype.

    Parameters:
    -----------
    df: pyspark.sql.DataFrame
        Spark DataFrame containing necessary columns for the calculation: fp_propulsiontype

    Returns:
    --------
    pyspark.sql.DataFrame
    Spark DataFrame with an additional column `fp_propulsiontypecode`.

    """

    df = df.withColumn(
        "fp_propulsiontypecode",
        when(df["fp_propulsiontype"] == "Battery-Electric", "BE")
        .when(df["fp_propulsiontype"] == "Gas Turbine(s) & Diesel Elec.", "GGDE")
        .when(df["fp_propulsiontype"] == "Gas Turbine(s) Or Diesel Elec.", "GGDX")
        .when(df["fp_propulsiontype"] == "Gas Turbine(s), Electric Drive", "GE")
        .when(df["fp_propulsiontype"] == "Gas Turbine(s), Geared Drive", "GG")
        .when(df["fp_propulsiontype"] == "Oil Eng(s) & Gas Turb(s) El.Dr", "DEGE")
        .when(df["fp_propulsiontype"] == "Oil Eng(s) Direct Dr, Aux Sail", "DDWS")
        .when(df["fp_propulsiontype"] == "Oil Eng(s), Elec-Dr, Aux Sail", "DEWS")
        .when(df["fp_propulsiontype"] == "Oil Eng(s), Geared, Aux Sail", "DGWS")
        .when(df["fp_propulsiontype"] == "Oil Engine(s), Direct Drive", "DD")
        .when(df["fp_propulsiontype"] == "Oil Engine(s), Drive Unknown", "DU")
        .when(df["fp_propulsiontype"] == "Oil Engine(s), Electric Drive", "DE")
        .when(df["fp_propulsiontype"] == "Oil Engine(s), Geared Drive", "DG")
        .when(df["fp_propulsiontype"] == "Oil Engines, Direct & Elec. Dr", "DDDE")
        .when(df["fp_propulsiontype"] == "Oil Engines, Elec. & Direct Dr", "DEDD")
        .when(df["fp_propulsiontype"] == "Oil Engines, Elec. & Geared Dr", "DEDG")
        .when(df["fp_propulsiontype"] == "Oil Engines, F&S, Geared Drive", "DGDG")
        .when(df["fp_propulsiontype"] == "Oil Engines, Geared & Elec. Dr", "DGDE")
        .when(df["fp_propulsiontype"] == "Oil Engs & Fuel Cell-Electric", "AE")
        .when(df["fp_propulsiontype"] == "Oil Engs & Gas Turb(s), Geared", "DGGG")
        .when(df["fp_propulsiontype"] == "Petrol Engine(s), Direct Drive", "PD")
        .when(df["fp_propulsiontype"] == "S.Turb, Gear & Oil Eng(s), Ele", "SGDE")
        .when(df["fp_propulsiontype"] == "Sail, Aux Oil Eng(s) Direct-Dr", "WSDD")
        .when(df["fp_propulsiontype"] == "Sail, Aux Oil Eng(s), Elec Dr.", "WSDE")
        .when(df["fp_propulsiontype"] == "Sail, Aux Oil Eng(s), Geared", "WSDG")
        .when(df["fp_propulsiontype"] == "St. Turb(s) Elec Dr. Or D.E.", "SEDX")
        .when(df["fp_propulsiontype"] == "Steam Recip(s), Direct Drive", "RD")
        .when(df["fp_propulsiontype"] == "Steam Turbine(s), Elec.Drive", "SE")
        .when(df["fp_propulsiontype"] == "Steam Turbine(s), Geared Drive", "SG")
        .otherwise(lit("UU")),
    )

    return df


# COMMAND ----------

# MAGIC %md ###set_enginetype

# COMMAND ----------


def set_enginetype(df: DataFrame) -> DataFrame:
    """
    Set main engine type based on enigne data from ship registers.
    Reference. Fourth IMO GHG study pp. 48 (76)

    Engine types:
    - Nuclear
    - Methanol
    - ST (Steam Turbin)
    - GT (Gas Turbin)
    - SSD (Slow Speed Diesel)
    - MSD (Medium Speed Diesel)
    - HSD (High Speed Diesel)
    - LNG-Otto SS (slow speed)
    - LNG-Otto MS (medium speed)
    - LNG-Diesel
    - LBSI (Lean Burn Spark-Ignited)

    Parameters:
    -----------
    df: pyspark.sql.DataFrame
        Spark DataFrame containing necessary columns for the calculation: fp_fueltype1first, fp_fueltype2second, fp_propulsiontypecode, fp_fueltype1code, fp_fueltype2code, engine_model, engine_builder, stroketype, main_engine_rpm

    Returns:
    --------
    pyspark.sql.DataFrame
    Spark DataFrame with an additional column `main_engine_enginetype`.

    """

    df = df.withColumn(
        "main_engine_enginetype",
        when(
            (df["fp_fueltype1code"] == "NU") | (df["fp_fueltype2code"] == "NU"),
            "Nuclear",
        ).when(df["fp_fueltype1code"] == "ME", "Methanol")
        # Steam Turbine(s), Geared Drive or S.Turb, Gear & Oil Eng(s), Ele or Steam Turbine(s), Elec.Drive or St. Turb(s) Elec Dr. Or D.E.
        .when(df["fp_propulsiontypecode"].isin(["SG", "SGDE", "SE", "SEDX"]), "ST")
        # Gas Turbine(s), Geared Drive or Oil Eng(s) & Gas Turb(s) El.Dr or Gas Turbine(s), Electric Drive or Gas Turbine(s) & Diesel Elec. or Oil Engs & Gas Turb(s), Geared
        .when(
            df["fp_propulsiontypecode"].isin(["GG", "DEGE", "GE", "GGDE", "DGGG"]), "GT"
        )
        # Oil Engs & Fuel Cell-Electric
        .when(df["fp_propulsiontypecode"] == "AE", "Hydrogen")
        # Battery-Electric and DNV eletric vessel overview
        .when(df["fp_propulsiontypecode"] == "BE", "Electric")
        # Sail, Aux Oil Eng(s), Geared or Sail, Aux Oil Eng(s) Direct-Dr or Sail, Aux Oil Eng(s), Elec Dr.
        .when(df["fp_propulsiontypecode"].isin(["WSDG", "WSDD", "WSDE"]), "Sail")
        .when(df["fp_me_enginetype"] == "Gas", "GT")
        .when(df["fp_me_enginetype"] == "Turbine", "ST")
        .when(
            (df["fp_me_enginetype"] == "Reciprocatng")
            | (df["fp_fueltype1code"] == "CU"),
            "Reciprocating",
        )
        # Set based on fueltype:
        # LNG (LG)
        .when(
            (df["fp_fueltype1code"] == "LG") | (df["fp_fueltype2code"] == "LG"),
            "LNG",
        )
        # Gas Boil Off (GB)
        .when(
            (df["fp_fueltype1code"] == "GB") | (df["fp_fueltype2code"] == "GB"),
            "LNG",
        )
        # Lpg (LP)
        .when(
            (df["fp_fueltype1code"] == "LP") | (df["fp_fueltype2code"] == "LP"),
            "LNG",
        )
        # LNG fueltype
        .when(df["main_engine_fueltype"] == "LNG", "LNG")
        # Oil engines
        .when(
            (
                df["fp_propulsiontypecode"].isin(
                    [
                        "DEGE",
                        "DDWS",
                        "DEWS",
                        "DGWS",
                        "DD",
                        "DU",
                        "DE",
                        "DG",
                        "DDDE",
                        "DEDD",
                        "DEDG",
                        "DGDG",
                        "DGDE",
                        "DGGG",
                    ]
                )
            )
            | (df["fp_propulsiontypecode"].isNull()),
            # Set engine type based on rpm if larger than 0
            when((df["main_engine_rpm"] > 0) & (df["main_engine_rpm"] <= 300), "SSD")
            .when((df["main_engine_rpm"] > 300) & (df["main_engine_rpm"] <= 900), "MSD")
            .when(df["main_engine_rpm"] > 900, "HSD")
            .otherwise(lit("SSD")),
        )
        # Fallback values for Oil engines:
        .when(
            df["main_engine_fueltype"].isin("Distillate Fuel", "Residual Fuel"), "SSD"
        ),
    )

    # LNG

    df = df.withColumn(
        "main_engine_enginetype",
        when(
            df["main_engine_enginetype"] == "LNG",
            when(
                (lower(df["engine_model"]).like("%df%"))
                | (lower(df["engine_model"]).like("%x%"))
                | (lower(df["engine_model"]).like("%me-ga%")),
                "LNG-Otto SS",
            )
            .when(
                (df["stroketype"] == 4) & (df["main_engine_rpm"] > 300), "LNG-Otto MS"
            )
            .when((lower(df["engine_model"]).like("%me%")), "LNG-Diesel")
            .when(
                (
                    df["fp_fueltype1code"].isin(["LG", "LP", "GB"])
                )  # LNG (LG), Lpg (LP), Gas boiler (GB)
                & (df["fp_fueltype2code"] != "DF")  # Distillate Fuel (DF)
                & (df["fp_fueltype2code"] != "RF"),  # Residual Fuel (RF)
                "LBSI",
            )
            .when(
                df["engine_builder"].isin(
                    ["Rolls-Royce Marine AS - Norway", "Bergen Engines AS - Norway"]
                ),
                "LBSI",
            )  # According to IMO GHG 4.
            .otherwise(lit("LNG-Otto MS")),
        ).otherwise(df["main_engine_enginetype"]),
    )

    return df


# COMMAND ----------

# MAGIC %md ###set_fueltype

# COMMAND ----------


def set_fueltype(df: DataFrame) -> DataFrame:
    """
    Sets main_engine_fueltype based on input from ship registers.

    Fueltypes:
    - Distillate Fuel
    - Residual Fuel
    - Methanol
    - LNG
    - Unknown
    - Electric
    - Nuclear
    - Hydrogen

    Parameters:
    -----------
    df: pyspark.sql.DataFrame
        Spark DataFrame containing necessary columns for the calculation:  fp_fueltype1code, fp_fueltype2code, fp_propulsiontypecode, vessel_type_imo_ghg

    Returns:
    --------
    pyspark.sql.DataFrame
    Spark DataFrame with an additional column .

    """
    df = df.withColumn(
        "main_engine_fueltype",
        # LNG
        when(df["fp_fueltype1code"].isin("LG", "GB", "LP"), "LNG")
        .when(df["fp_fueltype2code"].isin("LG", "GB", "LP"), "LNG")
        .when(
            ((df["fp_fueltype1code"] == "RF") | (df["fp_fueltype2code"] == "RF"))
            & (df["fp_propulsiontypecode"].isin(["SGDE", "SEDX", "SE", "SG"]))
            & (df["vessel_type_imo_ghg"] == "Liquefied gas tanker"),
            "LNG",
        )
        # Coal
        .when(
            (df["fp_fueltype1code"] == "CU")
            & (df["fp_fueltype2code"].isin(["NN", "UU", "YY"])),
            "Coal",
        )
        # Nuclear
        .when(df["fp_fueltype1code"] == "NU", "Nuclear").when(
            df["fp_fueltype2code"] == "NU", "Nuclear"
        )
        # Hydrogen
        .when(df["fp_fueltype1code"] == "HY", "Hydrogen")
        # Methanol
        .when(df["fp_fueltype1code"] == "ME", "Methanol")
        # Electric
        .when(df["fp_propulsiontypecode"] == "BE", "Electric")
        # Residuial Fuel
        .when(df["fp_fueltype1code"] == "RF", "Residual Fuel").when(
            df["fp_fueltype2code"] == "RF", "Residual Fuel"
        )
        # Distillate Fuel
        .when(
            (df["fp_fueltype1code"] == "DF") & (df["fp_fueltype2code"] == "DF"),
            "Distillate Fuel",
        )
        .when(
            (
                (df["fp_fueltype1code"] == "DF")
                & (df["fp_fueltype2code"].isin(["NN", "UU", "YY"]))
            )
            | (
                (df["fp_fueltype2code"] == "DF")
                & (df["fp_fueltype1code"].isin(["NN", "UU", "YY"]))
            ),
            "Distillate Fuel",
        )
        .when(
            ((df["fp_fueltype1code"] == "CU") & (df["fp_fueltype2code"] == "DF")),
            "Distillate Fuel",
        )
        .when(
            df["vessel_type_imo_ghg"] == "Miscellaneous - fishing", "Distillate Fuel"
        ),
    )

    return df


# COMMAND ----------

# MAGIC
# MAGIC %md ###set_year

# COMMAND ----------


def set_year(df: DataFrame) -> DataFrame:
    """
    Set year and assign 0 for missing values.

    Parameters:
    -----------
    df: pyspark.sql.DataFrame
        Spark DataFrame containing necessary column for the calculation: year

    Returns:
    --------
    pyspark.sql.DataFrame
        Spark DataFrame with the same columns.
    """

    df = df.withColumn("year_of_build", coalesce(df["year_of_build"], lit(0)))

    return df


# COMMAND ----------

# MAGIC %md ###set_year_tier

# COMMAND ----------


def set_year_tier(df: DataFrame) -> DataFrame:
    """
    Set tier group from year.

    Parameters:
    -----------
    df: pyspark.sql.DataFrame
        Spark DataFrame containing necessary column for the calculation: year

    Returns:
    --------
    pyspark.sql.DataFrame
        Spark DataFrame with a new column: tier_group.
    """

    df = df.withColumn(
        # Set year tier
        "tier_group",
        when((df["year_of_build"] >= 1900) & (df["year_of_build"] < 2000), lit(0))
        .when((df["year_of_build"] >= 2000) & (df["year_of_build"] < 2010), lit(1))
        .when((df["year_of_build"] >= 2010) & (df["year_of_build"] < 2020), lit(2))
        .when(df["year_of_build"] >= 2020, lit(3))
        .otherwise(lit(0)),
    )

    return df


# COMMAND ----------

# MAGIC %md ###set_year_tier_nox

# COMMAND ----------


def set_year_tier_nox(df: DataFrame) -> DataFrame:
    """
    nox tier according to IMO GHG fourth study, table 11.

    Parameters:
    -----------
    df: pyspark.sql.DataFrame
        Spark DataFrame containing necessary column for the calculation: year

    Returns:
    --------
    pyspark.sql.DataFrame
        Spark DataFrame with a new column: tier_group.
    """

    df = df.withColumn(
        # Set year tier
        "tier_group_nox",
        when((df["year_of_build"] >= 1900) & (df["year_of_build"] < 2000), lit(0))
        .when((df["year_of_build"] >= 2000) & (df["year_of_build"] < 2011), lit(1))
        .when((df["year_of_build"] >= 2011) & (df["year_of_build"] < 2021), lit(2))
        .when(df["year_of_build"] >= 2021, lit(3))
        .otherwise(lit(-99)),
    )

    return df


# COMMAND ----------

# MAGIC %md ###set_year_group

# COMMAND ----------


def set_year_group(df: DataFrame) -> DataFrame:
    """
    Set year group from year.

    Parameters:
    -----------
    df: pyspark.sql.DataFrame
        Spark DataFrame containing necessary column for the calculation: year

    Returns:
    --------
    pyspark.sql.DataFrame
        Spark DataFrame with a new column: year_group.
    """

    df = df.withColumn(
        "year_group",
        when((df["year_of_build"] > 0) & (df["year_of_build"] < 1960), lit("y1, <1959"))
        .when(
            (df["year_of_build"] >= 1960) & (df["year_of_build"] < 1980),
            lit("y2, 1960-1979"),
        )
        .when(
            (df["year_of_build"] >= 1980) & (df["year_of_build"] < 1990),
            lit("y3, 1980-1989"),
        )
        .when(
            (df["year_of_build"] >= 1990) & (df["year_of_build"] < 2000),
            lit("y4, 1990-1999"),
        )
        .when(
            (df["year_of_build"] >= 2000) & (df["year_of_build"] < 2005),
            lit("y5, 2000-2004"),
        )
        .when(
            (df["year_of_build"] >= 2005) & (df["year_of_build"] < 2010),
            lit("y6, 2005-2009"),
        )
        .when(
            (df["year_of_build"] >= 2010) & (df["year_of_build"] < 2015),
            lit("y7, 2010-2014"),
        )
        .when(
            (df["year_of_build"] >= 2015) & (df["year_of_build"] < 2020),
            lit("y8, 2015-2019"),
        )
        .when(df["year_of_build"] >= 2020, lit("y9, >=2020"))
        .otherwise(lit("Unknown")),
    )

    return df


# COMMAND ----------

# MAGIC %md ###set_length_group

# COMMAND ----------


def set_length_group(df: DataFrame) -> DataFrame:
    """
    Set length group from length.

    Parameters:
    -----------
    df: pyspark.sql.DataFrame
        Spark DataFrame containing necessary column for the calculation: length_overall

    Returns:
    --------
    pyspark.sql.DataFrame
        Spark DataFrame with a new column: length_group.
    """

    df = df.withColumn(
        "length_group",
        when((df["length_overall"] > 0) & (df["length_overall"] < 24), "l1, <24 m")
        .when((df["length_overall"] >= 24) & (df["length_overall"] < 50), "l2, 24-50 m")
        .when((df["length_overall"] >= 50) & (df["length_overall"] < 70), "l3, 50-70 m")
        .when(
            (df["length_overall"] >= 70) & (df["length_overall"] < 100), "l4, 70-100 m"
        )
        .when(
            (df["length_overall"] >= 100) & (df["length_overall"] < 150),
            "l5, 100-150 m",
        )
        .when(
            (df["length_overall"] >= 150) & (df["length_overall"] < 225),
            "l6, 150-225 m",
        )
        .when(df["length_overall"] >= 225, "l7, >=225 m")
        .otherwise("Unknown"),
    )

    return df


# COMMAND ----------

# MAGIC
# MAGIC %md ###set_gt_group

# COMMAND ----------


def set_gt_group(df: DataFrame) -> DataFrame:
    """
    Set gt group (gross tonnage group) from gt (gross tonnage).

    Parameters:
    -----------
    df: pyspark.sql.DataFrame
        Spark DataFrame containing necessary column for the calculation: gt

    Returns:
    --------
    pyspark.sql.DataFrame
        Spark DataFrame with a new column: gt_group.
    """

    df = df.withColumn(
        "gt_group",
        when((df["gt"] > 0) & (df["gt"] < 400), "gt1, 0-399")
        .when((df["gt"] >= 400) & (df["gt"] < 1000), "gt2, 400-999")
        .when((df["gt"] >= 1000) & (df["gt"] < 3000), "gt3, 1000-2999")
        .when((df["gt"] >= 3000) & (df["gt"] < 5000), "gt4, 3000-4999")
        .when((df["gt"] >= 5000) & (df["gt"] < 10000), "gt5, 5000-9999")
        .when((df["gt"] >= 10000) & (df["gt"] < 25000), "gt6, 10000-24999")
        .when((df["gt"] >= 25000) & (df["gt"] < 50000), "gt7, 25000-49999")
        .when((df["gt"] >= 50000) & (df["gt"] < 100000), "gt8, 50000-99999")
        .when(df["gt"] >= 100000, "gt9, >=100 000")
        .otherwise("Unknown"),
    )

    return df


# COMMAND ----------

# MAGIC %md ###set_dwt_group

# COMMAND ----------


def set_dwt_group(df: DataFrame) -> DataFrame:
    """
    Set dwt group (deadweight tonnage group) from dwt (deadweight tonnage)

    Parameters:
    -----------
    df: pyspark.sql.DataFrame
        Spark DataFrame containing necessary column for the calculation: dwt

    Returns:
    --------
    pyspark.sql.DataFrame
        Spark DataFrame with a new column: dwt_group.
    """

    df = df.withColumn(
        "dwt_group",
        when((df["dwt"] > 0) & (df["dwt"] < 5000), "dwt1, <5000")
        .when(
            (df["dwt"] >= 5000) & (df["dwt"] < 10000),
            "dwt2, 5 000-10 000",
        )
        .when(
            (df["dwt"] >= 10000) & (df["dwt"] < 25000),
            "dwt3, 10 000-25 000",
        )
        .when(
            (df["dwt"] >= 25000) & (df["dwt"] < 50000),
            "dwt4, 25 000-50 000",
        )
        .when(
            (df["dwt"] >= 50000) & (df["dwt"] < 75000),
            "dwt5, 50 000-75 000",
        )
        .when(
            (df["dwt"] >= 75000) & (df["dwt"] < 100000),
            "dwt6, 75 000-100 000",
        )
        .when(
            (df["dwt"] >= 100000) & (df["dwt"] < 150000),
            "dwt7, 100 000-150 000",
        )
        .when(
            (df["dwt"] >= 150000) & (df["dwt"] < 200000),
            "dwt8, 150 000-200 000",
        )
        .when(df["dwt"] >= 200000, "dwt9, >=200 000")
        .otherwise("Unknown"),
    )

    return df


# COMMAND ----------

# MAGIC %md ### update_degree_of_electrification

# COMMAND ----------


def update_degree_of_electrification(df: DataFrame) -> DataFrame:
    """
    Sets degree_of_electrification to 0.95 for vessels with fuel_type = Electric

    Parameters:
    -----------
    df: pyspark.sql.DataFrame
        Spark DataFrame containing necessary columns for the calculation: fuel_type and degree_of_electrification

    Returns:
    --------
    pyspark.sql.DataFrame
    Spark DataFrame with updated values.
    """

    df = df.withColumn(
        "degree_of_electrification",
        when(
            df["main_engine_fueltype"] == "Electric", lit("0.95").cast("float")
        ).otherwise(df["degree_of_electrification"]),
    )

    return df


# COMMAND ----------

# MAGIC %md ### fill_missing_values_by_vessel_gt_group

# COMMAND ----------


def fill_missing_values_fueltype(df: DataFrame) -> DataFrame:
    """
    Fill missing values based on most frequent record (mode) in the group (vessel_type and gt).

    Parameters:
    -----------
    df: pyspark.sql.DataFrame
        Spark DataFrame containing necessary columns for the calculation:  main_engine_fueltype

    Returns:
    --------
    pyspark.sql.DataFrame
    Spark DataFrame with an additional column .

    """
    window_vesseltype_gtgroup = Window.partitionBy("vessel_type_maru", "gt_group")
    window_vesseltype = Window.partitionBy("vessel_type_maru")
    window_gtgroup = Window.partitionBy("gt_group")

    df = df.withColumn(
        "main_engine_fueltype_original", col("main_engine_fueltype")
    ).withColumn(
        "main_engine_fueltype",
        coalesce(
            col("main_engine_fueltype"),
            mode("main_engine_fueltype").over(window_vesseltype_gtgroup),
            mode("main_engine_fueltype").over(window_gtgroup),
            mode("main_engine_fueltype").over(window_vesseltype),
        ),
    )

    return df


# COMMAND ----------

# MAGIC %md ### update_main_engine_kw_with_speed_correction_factor

# COMMAND ----------


def update_main_engine_kw_with_speed_correction_factor(df: DataFrame) -> DataFrame:
    """
    Applies a power correction factor to the 'main_engine_kw' column based on the vessel type.

    Parameters:
    -----------
    df : pyspark.sql.DataFrame
        Spark DataFrame containing the necessary columns for the calculation: 'vessel_type_maru' and 'main_engine_kw'.

    Returns:
    --------
    pyspark.sql.DataFrame
        Spark DataFrame with the 'main_engine_kw' column updated with the power correction factor.
    """

    df = df.withColumn(
        "main_engine_kw",
        when(df["vessel_type_maru"] == "Cruise", df["main_engine_kw"] * 0.7).otherwise(
            df["main_engine_kw"]
        ),
    )
    return df


# COMMAND ----------

# MAGIC %md ###Add enriched columns

# COMMAND ----------


def add_enriched_columns(df: DataFrame) -> DataFrame:
    """
    Select and add new columns to vessel dataset.

    Parameters:
    -----------
    df: pyspark.sql.DataFrame
        Spark DataFrame containing necessary columns (see specific columns in docstring for each function)

    Returns:
    --------
    pyspark.sql.DataFrame
        Spark DataFrame with new columns (see specific columns in docstring for each function).
    """

    df = set_length_group(df)
    df = set_year_tier(df)
    df = set_year_tier_nox(df)
    df = set_year_group(df)
    df = set_gt_group(df)
    df = set_dwt_group(df)
    df = set_propulsiontypecode(df)
    df = set_fueltype(df)
    df = fill_missing_values_fueltype(df)
    df = set_enginetype(df)
    df = update_degree_of_electrification(df)
    df = update_main_engine_kw_with_speed_correction_factor(df)

    return df


# COMMAND ----------

df_vessel_total = add_enriched_columns(df_vessel)

# COMMAND ----------

# MAGIC %md #QC

# COMMAND ----------

# Check that input and output dataframe of unpivot and join have equal length
count_expected = (
    com.join(com_filtered, on="vessel_id", how="inner")
    .where("vessel_type_imo_ghg is not null")
    .count()
)
count_actual = df_vessel_total.count()
assert (
    count_expected == count_actual
), f"Expected: {count_expected}, actual: {count_actual}"

# COMMAND ----------

# MAGIC %md #Write data

# COMMAND ----------

df_vessel_total.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    name=FULL_TABLE_NAME_MARU_VESSEL
)
