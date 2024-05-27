import pyspark.sql.functions as F
from pyspark.sql.types import FloatType, IntegerType


def change_data_types(df):
    """
    Change data types from StringType to Floattype and IntegerType
    """

    df = df.withColumn(
        "degree_of_electrification",
        F.regexp_replace("degree_of_electrification", ",", ".").cast(FloatType()),
    )
    df = df.withColumn(
        "battery_pack_kwh",
        F.regexp_replace("battery_pack_kwh", " ", "").cast(IntegerType()),
    )

    return df
