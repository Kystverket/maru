# Databricks notebook source
# DBTITLE 1,Untitled
spark.conf.set("spark.sql.ansi.enabled", "false")

from collections.abc import Iterable, Sequence
from functools import reduce

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import NumericType

# COMMAND ----------

# DBTITLE 1, Configuration
FACT_OLD = "gold_prod.maru.fact_emission_v1_8"
FACT_NEW = "gold_prod.maru.fact_emission_1_10"

REPORT_OLD = "gold_prod.maru.dm_maru_report_v1_8"
REPORT_NEW = "gold_prod.maru.dm_maru_report_1_10"

START_DATE = "2024-01-01"
END_DATE = "2024-04-01"

MONTHS: list[str] = [
    "2024-01",
    "2024-02",
    "2024-03",
]

METRICS: list[str] = [
    "main_engine_kwh",
    "aux_kwh",
    "boiler_kwh",
    "fuel_main_aux_boiler_engines_tonnes",
    "fuel_mdo_equivalent_tonnes",
    "bc_tonnes",
    "ch4_tonnes",
    "co_tonnes",
    "co2_tonnes",
    "n2o_tonnes",
    "nmvoc_tonnes",
    "nox_tonnes",
    "pm10_tonnes",
    "pm2_5_tonnes",
    "sox_tonnes",
    "co2e_tonnes",
]


# COMMAND ----------

# DBTITLE 1, Load fact tables
df_fact_old = spark.table(FACT_OLD).filter(
    (F.col("date_time_utc") >= START_DATE)
    & (F.col("date_time_utc") < END_DATE)
    & (F.col("in_coast_and_sea_area") == True)
)

df_fact_new = spark.table(FACT_NEW).filter(
    (F.col("date_time_utc") >= START_DATE)
    & (F.col("date_time_utc") < END_DATE)
    & (F.col("in_coast_and_sea_area") == True)
)


# COMMAND ----------

# DBTITLE 1, Load report tables
df_report_old = spark.table(REPORT_OLD).filter(
    F.col("year_month").isin(MONTHS) & (F.col("in_coast_and_sea_area") == True)
)

df_report_new = spark.table(REPORT_NEW).filter(
    F.col("year_month").isin(MONTHS) & (F.col("in_coast_and_sea_area") == True)
)


# COMMAND ----------

# DBTITLE 1, Helper functions


def get_vessel_id_col(df: DataFrame) -> str:
    """Return the vessel identifier column name present in the DataFrame.

    Checks for 'vessel_id_hash' first, then falls back to 'vessel_id'.

    Args:
        df: Input DataFrame.

    Returns:
        The name of the vessel identifier column ('vessel_id_hash' or 'vessel_id').

    Raises:
        ValueError: If neither column is found in the DataFrame.
    """
    if "vessel_id_hash" in df.columns:
        return "vessel_id_hash"
    if "vessel_id" in df.columns:
        return "vessel_id"
    msg = "DataFrame has neither 'vessel_id_hash' nor 'vessel_id' column."
    raise ValueError(msg)


def get_metrics_from_schema(df: DataFrame) -> list[str]:
    """Return numeric metric columns whose names match.

    - sum_*
    - *_tonnes
    - *_kwh.
    """
    return sorted(
        field.name
        for field in df.schema.fields
        if isinstance(field.dataType, NumericType)
        and (field.name.startswith("sum_") or field.name.endswith(("_tonnes", "_kwh")))
    )


def with_percentage_difference(
    df: DataFrame,
    new_col: str = "new",
    old_col: str = "old",
    pct_col: str = "percentage_difference",
) -> DataFrame:
    """Add a percentage-difference column: (new - old) / old * 100.

    Yields null when old is zero.
    """
    return df.withColumn(
        pct_col,
        F.when(F.col(old_col) == 0, F.lit(None)).otherwise(
            (F.col(new_col) - F.col(old_col)) / F.col(old_col) * 100
        ),
    )


def _label_total_row(
    total_row: DataFrame,
    result_schema: dict[str, str],
    group_cols: list[str],
) -> DataFrame:
    """Set group columns to 'TOTAL' (strings) or null (other types)."""
    return total_row.withColumns(
        {
            g: F.lit("TOTAL")
            if result_schema[g] == "string"
            else F.lit(None).cast(result_schema[g])
            for g in group_cols
        }
    )


# COMMAND ----------

# DBTITLE 1, Count comparison


def count_by(df: DataFrame, group_cols: Iterable[str], col_name: str) -> DataFrame:
    """Count rows grouped by one or more columns."""
    return df.groupBy(list(group_cols)).agg(F.count("*").alias(col_name))


def compare_counts(
    df_old: DataFrame,
    df_new: DataFrame,
    group_cols: Iterable[str],
    label: str,
) -> None:
    """Compare row counts between two DataFrames grouped by given columns.

    Prints a message when counts are identical, otherwise displays a
    DataFrame ordered by descending percentage difference.
    """
    group_cols = list(group_cols)
    old = count_by(df_old, group_cols, "old")
    new = count_by(df_new, group_cols, "new")

    compared = (
        new.join(old, group_cols, "full_outer")
        .fillna(0)
        .transform(with_percentage_difference)
        .orderBy(F.desc("percentage_difference"))
    )

    top = compared.select(F.max("percentage_difference")).first()[0]
    if top == 0:
        print(f"{label}: No differences detected.")
    else:
        display(compared)


def count_profile(df: DataFrame, label: str) -> DataFrame:
    """Return a single-row summary of row and distinct-key counts."""
    vessel_id_col = get_vessel_id_col(df)
    return df.agg(
        F.count("*").alias(f"{label}_rows"),
        F.countDistinct("sail_id").alias(f"{label}_distinct_sail_id"),
        F.countDistinct(vessel_id_col).alias(f"{label}_distinct_vessel_id"),
        F.countDistinct("mmsi").alias(f"{label}_distinct_mmsi"),
        F.countDistinct("imo").alias(f"{label}_distinct_imo"),
        F.countDistinct("year_month").alias(f"{label}_distinct_months"),
    )


# COMMAND ----------

# DBTITLE 1, Metric sum comparisons


def compare_metric_sums(df_old: DataFrame, df_new: DataFrame) -> None:
    """Compare total summed metrics between two DataFrames.

    Displays percentage change per metric and the largest absolute change.
    """
    old = df_old.agg(*[F.sum(c).alias(f"{c}_old") for c in METRICS])
    new = df_new.agg(*[F.sum(c).alias(f"{c}_new") for c in METRICS])

    result = old.crossJoin(new)

    for col in sorted(METRICS):
        result = (
            result.withColumns({"old": F.col(f"{col}_old"), "new": F.col(f"{col}_new")})
            .transform(
                lambda d, col=col: with_percentage_difference(
                    d, pct_col=f"{col}_pct_change"
                )
            )
            .drop("old", "new")
        )

    pct_cols = [f"{c}_pct_change" for c in sorted(METRICS)]
    result = result.select(*pct_cols).withColumn(
        "max_change", F.greatest(*[F.abs(F.col(c)) for c in pct_cols])
    )

    display(result)


def compare_metric_sums_grouped_by(
    df_old: DataFrame,
    df_new: DataFrame,
    group_cols: str | Sequence[str],
) -> None:
    """Compare summed metrics between two DataFrames grouped by given columns.

    For each metric, calculates the percentage change per group. Also includes
    a combined total column and a grand-total row.
    """
    group_cols = [group_cols] if isinstance(group_cols, str) else list(group_cols)

    metrics = sorted(
        set(get_metrics_from_schema(df_old)) & set(get_metrics_from_schema(df_new))
    )
    if not metrics:
        msg = "No matching metrics found in both DataFrames."
        raise ValueError(msg)

    old = df_old.groupBy(group_cols).agg(*[F.sum(c).alias(f"{c}_old") for c in metrics])
    new = df_new.groupBy(group_cols).agg(*[F.sum(c).alias(f"{c}_new") for c in metrics])
    joined = old.join(new, group_cols, "full_outer")

    for c in metrics:
        joined = with_percentage_difference(
            joined, old_col=f"{c}_old", new_col=f"{c}_new", pct_col=f"{c}_pct_change"
        )

    total_expr = lambda suffix: reduce(  # noqa: E731
        lambda a, b: a + b,
        [F.coalesce(F.col(f"{c}_{suffix}"), F.lit(0)) for c in metrics],
    )

    joined = (
        joined.withColumn("total_old", total_expr("old"))
        .withColumn("total_new", total_expr("new"))
        .transform(
            lambda d: with_percentage_difference(
                d, old_col="total_old", new_col="total_new", pct_col="total_pct_change"
            )
        )
    )

    pct_cols = [f"{c}_pct_change" for c in metrics] + ["total_pct_change"]

    result = joined.select(*group_cols, *pct_cols, "total_old", "total_new").withColumn(
        "max_change", F.greatest(*[F.abs(F.col(c)) for c in pct_cols])
    )

    # --- grand total row ---
    total_row = joined.agg(
        *[
            F.sum(F.coalesce(F.col(f"{c}_old"), F.lit(0))).alias(f"{c}_old")
            for c in metrics
        ],
        *[
            F.sum(F.coalesce(F.col(f"{c}_new"), F.lit(0))).alias(f"{c}_new")
            for c in metrics
        ],
        F.sum("total_old").alias("total_old"),
        F.sum("total_new").alias("total_new"),
    )

    for c in metrics:
        total_row = with_percentage_difference(
            total_row, old_col=f"{c}_old", new_col=f"{c}_new", pct_col=f"{c}_pct_change"
        )

    total_row = (
        total_row.transform(
            lambda d: with_percentage_difference(
                d, old_col="total_old", new_col="total_new", pct_col="total_pct_change"
            )
        )
        .transform(lambda d: _label_total_row(d, dict(result.dtypes), group_cols))
        .withColumn("max_change", F.greatest(*[F.abs(F.col(c)) for c in pct_cols]))
    )

    display(
        result.unionByName(
            total_row.select(result.columns), allowMissingColumns=True
        ).orderBy(F.desc("max_change"))
    )


def compare_metric_values_with_delta_and_pct(
    df_old: DataFrame,
    df_new: DataFrame,
    group_cols: str | Sequence[str],
) -> None:
    """Compare metrics between two DataFrames with absolute delta and percentage change.

    Groups by specified column(s), computes metric sums, deltas, and percentage
    changes side-by-side per metric. Appends a grand-total row.

    Raises:
        ValueError: If no matching metrics are found in both DataFrames.
    """
    group_cols = [group_cols] if isinstance(group_cols, str) else list(group_cols)

    metrics = sorted(
        set(get_metrics_from_schema(df_old)) & set(get_metrics_from_schema(df_new))
    )
    if not metrics:
        msg = "No matching metrics found in both DataFrames."
        raise ValueError(msg)

    old = df_old.groupBy(group_cols).agg(*[F.sum(c).alias(f"{c}_old") for c in metrics])
    new = df_new.groupBy(group_cols).agg(*[F.sum(c).alias(f"{c}_new") for c in metrics])
    joined = old.join(new, group_cols, "full_outer")

    for c in metrics:
        joined = (
            joined.withColumns(
                {
                    f"{c}_old": F.coalesce(F.col(f"{c}_old"), F.lit(0.0)),
                    f"{c}_new": F.coalesce(F.col(f"{c}_new"), F.lit(0.0)),
                    f"{c}_delta": F.col(f"{c}_new") - F.col(f"{c}_old"),
                }
            )
            .transform(
                lambda d, c=c: with_percentage_difference(
                    d, old_col=f"{c}_old", new_col=f"{c}_new", pct_col=f"{c}_pct_change"
                )
            )
            .withColumns(
                {
                    f"{c}_old": F.round(f"{c}_old", 1),
                    f"{c}_new": F.round(f"{c}_new", 1),
                    f"{c}_delta": F.round(f"{c}_delta", 1),
                    f"{c}_pct_change": F.round(f"{c}_pct_change", 1),
                }
            )
        )

    ordered_metric_cols = [
        col
        for c in metrics
        for col in (f"{c}_old", f"{c}_new", f"{c}_delta", f"{c}_pct_change")
    ]

    result = joined.select(*group_cols, *ordered_metric_cols)

    # --- grand total row ---
    total_row = result.agg(
        *[F.sum(f"{c}_old").alias(f"{c}_old") for c in metrics],
        *[F.sum(f"{c}_new").alias(f"{c}_new") for c in metrics],
    )

    for c in metrics:
        total_row = (
            total_row.withColumn(f"{c}_delta", F.col(f"{c}_new") - F.col(f"{c}_old"))
            .transform(
                lambda d, c=c: with_percentage_difference(
                    d, old_col=f"{c}_old", new_col=f"{c}_new", pct_col=f"{c}_pct_change"
                )
            )
            .withColumns(
                {
                    f"{c}_old": F.round(f"{c}_old", 1),
                    f"{c}_new": F.round(f"{c}_new", 1),
                    f"{c}_delta": F.round(f"{c}_delta", 1),
                    f"{c}_pct_change": F.round(f"{c}_pct_change", 1),
                }
            )
        )

    total_row = total_row.transform(
        lambda d: _label_total_row(d, dict(result.dtypes), group_cols)
    ).select(*result.columns)

    display(result.unionByName(total_row))


# COMMAND ----------

# DBTITLE 1, Vessel comparisons


def compare_unique_vessels_by_variable(
    df_old: DataFrame,
    df_new: DataFrame,
    variable: str,
) -> None:
    """Compare the number of unique vessels grouped by a variable.

    The vessel identifier column is detected automatically per DataFrame
    by checking for 'vessel_id_hash' first, then falling back to 'vessel_id'.

    Args:
        df_old: DataFrame for the old version.
        df_new: DataFrame for the new version.
        variable: Column name to group by.
    """
    vessel_id_col_old = get_vessel_id_col(df_old)
    vessel_id_col_new = get_vessel_id_col(df_new)
    old = df_old.groupBy(variable).agg(F.countDistinct(vessel_id_col_old).alias("old"))
    new = df_new.groupBy(variable).agg(F.countDistinct(vessel_id_col_new).alias("new"))

    compared = (
        new.join(old, variable, "full_outer")
        .fillna(0)
        .transform(with_percentage_difference)
        .orderBy(F.desc("percentage_difference"))
    )

    top = compared.select(F.max("percentage_difference")).first()[0]
    if top == 0:
        print(f"Grouped by {variable}: No differences detected.")
    else:
        display(compared)


# COMMAND ----------

# DBTITLE 1, Sum comparison by variable


def compare_sum_by_variable(
    df_old: DataFrame,
    df_new: DataFrame,
    variable: str,
    value_col: str,
) -> None:
    """Compare summed values for a single column grouped by a variable.

    Displays groups with non-zero percentage differences.
    """
    old = df_old.groupBy(variable).agg(F.sum(value_col).alias("old"))
    new = df_new.groupBy(variable).agg(F.sum(value_col).alias("new"))

    compared = (
        new.join(old, variable, "full_outer")
        .fillna(0)
        .transform(with_percentage_difference)
        .orderBy(F.desc("percentage_difference"))
    )

    top = compared.select(F.max("percentage_difference")).first()[0]
    if top == 0:
        print(f"Grouped by {variable}: No differences detected.")
    else:
        display(compared)


# COMMAND ----------

# DBTITLE 1, Monthly metric breakdown (per vessel type)


def monthly_metric_summary(
    df: DataFrame,
    label: str,
    metric_cols: list[str],
) -> DataFrame:
    """Return monthly sums for the given metric columns."""
    return df.groupBy("year_month").agg(
        *[F.sum(c).alias(f"{label}_{c}") for c in metric_cols]
    )


# COMMAND ----------

# DBTITLE 1, Fact table comparisons
compare_counts(df_fact_old, df_fact_new, ["phase"], "Row count per phase")

# COMMAND ----------

compare_metric_sums(df_fact_old, df_fact_new)

# COMMAND ----------

compare_metric_sums_grouped_by(df_fact_old, df_fact_new, ["phase"])

# COMMAND ----------

compare_unique_vessels_by_variable(df_fact_old, df_fact_new, "municipality_id")
compare_unique_vessels_by_variable(df_fact_old, df_fact_new, "phase")

# COMMAND ----------

compare_sum_by_variable(
    df_fact_old,
    df_fact_new,
    variable="phase",
    value_col="delta_previous_point_seconds",
)


# COMMAND ----------

# DBTITLE 1, Report table comparisons
compare_counts(df_report_old, df_report_new, ["vessel_type"], "Vessels by type")
compare_counts(df_report_old, df_report_new, ["voyage_type"], "Voyage types")
compare_counts(
    df_report_old, df_report_new, ["vessel_type", "phase"], "Vessel type + phase"
)
compare_counts(
    df_report_old, df_report_new, ["main_engine_fueltype"], "Engine fuel types"
)

# COMMAND ----------

compare_metric_sums_grouped_by(df_report_old, df_report_new, ["vessel_type"])

# COMMAND ----------

compare_metric_sums_grouped_by(
    df_report_old, df_report_new, ["vessel_type", "voyage_type"]
)

# COMMAND ----------

compare_metric_sums_grouped_by(
    df_report_old, df_report_new, ["vessel_type", "gt_group"]
)

# COMMAND ----------

compare_metric_values_with_delta_and_pct(
    df_report_old, df_report_new, ["vessel_type", "gt_group"]
)


# COMMAND ----------

# DBTITLE 1, Test: max sum_seconds per vessel (Jan 2024)
# January 2024 has 2,678,400 seconds (31 days).
# Allow +2.5 days = 2,894,400 seconds (~1.0833 months total).
SECONDS_IN_JANUARY = 31 * 86_400  # 2,678,400
MAX_ALLOWED_SECONDS = SECONDS_IN_JANUARY + int(2.5 * 86_400)  # 2,894,400

vessel_id_col_old = get_vessel_id_col(df_report_old)
vessel_id_col_new = get_vessel_id_col(df_report_new)

for label, df, vid_col in [
    ("OLD", df_report_old, vessel_id_col_old),
    ("NEW", df_report_new, vessel_id_col_new),
]:
    df_jan = df.filter(F.col("year_month") == "2024-01")
    max_val = (
        df_jan.groupBy(vid_col)
        .agg(F.sum("sum_seconds").alias("total_seconds"))
        .agg(F.max("total_seconds"))
        .first()[0]
    )
    status = "PASS" if (max_val or 0) <= MAX_ALLOWED_SECONDS else "FAIL"
    print(
        f"{label} - Max sum_seconds per vessel (Jan 2024): {max_val:,.0f} | "
        f"Threshold: {MAX_ALLOWED_SECONDS:,} | {status}"
    )

# Show vessels exceeding threshold in NEW table (Jan 2024)
df_jan_new = (
    df_report_new.filter(F.col("year_month") == "2024-01")
    .groupBy(vessel_id_col_new)
    .agg(F.sum("sum_seconds").alias("total_seconds"))
    .filter(F.col("total_seconds") > MAX_ALLOWED_SECONDS)
)

exceeding_count = df_jan_new.count()
if exceeding_count > 0:
    print(
        f"\n⚠ {exceeding_count} vessel(s) EXCEED threshold ({MAX_ALLOWED_SECONDS:,}s) in NEW (Jan 2024):"
    )
    display(df_jan_new.orderBy(F.desc("total_seconds")))

# COMMAND ----------


# DBTITLE 1, Outlier detection (kwh_per_hour, MAD z-score)
def find_outlier_vessels(
    df: DataFrame,
    label: str,
    year_month_filter: str = "2024-01",
    z_threshold: float = 3.5,
) -> None:
    """Detect energy-anomalous vessels using residual-based anomaly detection.

    Uses kwh_per_hour (energy intensity over time) which is robust to AIS gaps
    and does not depend on distance accuracy.

    Approach:
    1. Aggregates metrics per vessel + shiptype_level5 + voyage_type + gt_group for the target month.
    2. Computes kwh_per_hour = sum_kwh / (sum_seconds / 3600).
    3. Computes expected kwh_per_hour per (shiptype_level5, voyage_type, gt_group) group.
    4. Calculates residuals (actual - expected).
    5. Uses MAD-based modified z-scores per group to flag outliers.

    Outlier threshold: |modified_z_score| > z_threshold (default 3.5).
    """
    vid_col = get_vessel_id_col(df)

    # --- Step 1: Aggregate per vessel, voyage_type and gt_group for the target month ---
    df_month = df.filter(F.col("year_month") == year_month_filter)
    vessel_agg = df_month.groupBy(
        vid_col, "shiptype_level5", "voyage_type", "gt_group"
    ).agg(
        F.sum("sum_kwh").alias("sum_kwh"),
        F.sum("distance_kilometers").alias("distance_kilometers"),
        F.sum("sum_seconds").alias("sum_seconds"),
    )

    # --- Step 2: Compute kwh_per_hour (safe divide-by-zero) ---
    vessel_agg = vessel_agg.withColumn(
        "kwh_per_hour",
        F.when(
            F.col("sum_seconds") > 0,
            F.col("sum_kwh") / (F.col("sum_seconds") / 3600.0),
        ),
    )

    # Filter to vessels with valid kwh_per_hour (need sum_seconds > 0)
    # Exclude vessels hitting discrete model rate levels (4800, 9000 kwh/h)
    excluded_model_rate_values = [4800, 9000]
    vessel_valid = vessel_agg.filter(
        F.col("kwh_per_hour").isNotNull()
        & ~F.round("kwh_per_hour", 0).isin(excluded_model_rate_values)
    )

    # --- Step 3: Expected kwh_per_hour per ship type + voyage type + gt_group ---
    group_cols = ["shiptype_level5", "voyage_type", "gt_group"]
    expected = vessel_valid.groupBy(*group_cols).agg(
        F.mean("kwh_per_hour").alias("expected_kwh_per_hour"),
        F.count("*").alias("group_size"),
    )

    # --- Step 4: Join expected back and compute residuals ---
    vessel_scored = vessel_valid.join(expected, group_cols, "left").withColumn(
        "residual_kwh_per_hour", F.col("kwh_per_hour") - F.col("expected_kwh_per_hour")
    )

    # --- Step 5: MAD-based modified z-score per (shiptype_level5, voyage_type, gt_group) ---
    w = Window.partitionBy(*group_cols)
    vessel_scored = (
        vessel_scored.withColumns(
            {
                "median_residual": F.percentile_approx(
                    "residual_kwh_per_hour", 0.5
                ).over(w),
            }
        )
        .withColumns(
            {
                "abs_deviation": F.abs(
                    F.col("residual_kwh_per_hour") - F.col("median_residual")
                ),
            }
        )
        .withColumns(
            {
                "mad": F.percentile_approx("abs_deviation", 0.5).over(w),
            }
        )
        .withColumns(
            {
                "modified_z_score": F.when(
                    F.col("mad") > 0,
                    0.6745
                    * (F.col("residual_kwh_per_hour") - F.col("median_residual"))
                    / F.col("mad"),
                ).otherwise(F.lit(0.0)),
            }
        )
        .withColumns(
            {
                "is_outlier": F.abs(F.col("modified_z_score")) > z_threshold,
            }
        )
    )

    # --- Step 6: Output ---
    outliers = vessel_scored.filter(F.col("is_outlier")).orderBy(
        F.desc(F.abs(F.col("modified_z_score")))
    )

    counts = vessel_scored.agg(
        F.count("*").alias("total_vessels"),
        F.coalesce(F.sum(F.col("is_outlier").cast("int")), F.lit(0)).alias(
            "outlier_count"
        ),
    ).first()
    total_vessels = counts["total_vessels"]
    outlier_count = counts["outlier_count"]

    print(f"\n{'=' * 60}")
    print(f" Outlier detection ({year_month_filter}): {label}")
    print(" Method: MAD-based modified z-score on kwh_per_hour")
    print(" Grouping: shiptype_level5 + voyage_type + gt_group")
    print(f" Threshold: |z| > {z_threshold}")
    print(f" Vessels analysed: {total_vessels:,} (with sum_seconds > 0)")
    print(f"{'=' * 60}")

    if outlier_count == 0:
        print("\n\u2713 No statistical outliers detected.")
        return

    # Summary per ship type + voyage type
    print(f"\n\u26a0 {outlier_count} outlier vessel(s) detected:")
    summary = (
        outliers.groupBy("shiptype_level5", "voyage_type")
        .agg(
            F.count("*").alias("outlier_count"),
            F.round(F.min("modified_z_score"), 2).alias("min_z"),
            F.round(F.max("modified_z_score"), 2).alias("max_z"),
            F.round(F.mean("kwh_per_hour"), 1).alias("avg_kwh_per_hour"),
            F.round(F.mean("expected_kwh_per_hour"), 1).alias("expected_kwh_per_hour"),
        )
        .orderBy(F.desc("outlier_count"))
    )
    display(summary)

    # Detailed outlier table
    print("\nTop outlier vessels (up to 30):")
    display(
        outliers.select(
            F.col(vid_col).alias("vessel_id"),
            "shiptype_level5",
            "voyage_type",
            "gt_group",
            F.round("sum_kwh", 1).alias("sum_kwh"),
            F.round(F.col("sum_seconds") / 3600, 1).alias("hours"),
            F.round("distance_kilometers", 1).alias("distance_km"),
            F.round("kwh_per_hour", 2).alias("kwh_per_hour"),
            F.round("expected_kwh_per_hour", 2).alias("expected_kwh_per_hour"),
            F.round("residual_kwh_per_hour", 2).alias("residual_kwh_per_hour"),
            F.round("modified_z_score", 2).alias("modified_z_score"),
            "is_outlier",
            "group_size",
        ).limit(30)
    )


find_outlier_vessels(df_report_old, "OLD report", "2024-01")
find_outlier_vessels(df_report_new, "NEW report", "2024-01")

# COMMAND ----------

# DBTITLE 1, Cruise vessel deep-dive (berthed, selected municipalities)
CRUISE_MUNICIPALITIES = ["Oslo", "Bergen", "Kristiansand", "Haugesund", "Karmøy"]


def filter_cruise_berthed(df: DataFrame, municipalities: list[str]) -> DataFrame:
    """Filter DataFrame to cruise vessels berthed in specified municipalities."""
    return df.withColumn(
        "municipality_name_first",
        F.split(F.col("municipality_name"), r" \(").getItem(0),
    ).filter(
        F.col("municipality_name_first").isin(municipalities)
        & (F.col("vessel_type") == "Cruise")
        & (F.col("voyage_type") == "berthed")
    )


df_cruise_old = filter_cruise_berthed(df_report_old, CRUISE_MUNICIPALITIES)
df_cruise_new = filter_cruise_berthed(df_report_new, CRUISE_MUNICIPALITIES)

# COMMAND ----------

compare_metric_sums_grouped_by(
    df_cruise_old,
    df_cruise_new,
    ["vessel_type", "voyage_type", "municipality_name_first"],
)

# COMMAND ----------

compare_metric_sums_grouped_by(
    df_cruise_old, df_cruise_new, ["vessel_type", "gt_group"]
)


# COMMAND ----------

# DBTITLE 1, Container/RoRo vessel deep-dive
VESSEL_TYPE = "Container/RoRo"
RELATED_METRICS = [
    "sum_kwh",
    "sum_fuel_main_aux_boiler_engines_tonnes",
    "sum_fuel_mdo_equivalent_tonnes",
    "sum_co2_tonnes",
]

df_roro_old = df_report_old.filter(F.col("vessel_type") == VESSEL_TYPE)
df_roro_new = df_report_new.filter(F.col("vessel_type") == VESSEL_TYPE)

# COMMAND ----------

# DBTITLE 1,Cell 25
display(count_profile(df_roro_old, "old").crossJoin(count_profile(df_roro_new, "new")))

# COMMAND ----------

# DBTITLE 1,Cell 26
display(
    monthly_metric_summary(df_roro_old, "old", RELATED_METRICS)
    .join(
        monthly_metric_summary(df_roro_new, "new", RELATED_METRICS),
        "year_month",
        "outer",
    )
    .orderBy("year_month")
)
