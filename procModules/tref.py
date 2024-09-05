from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from procModules import mapper, constants

def Tref(spark: "SparkSession", dataFrame: "F.DataFrame") -> F.DataFrame:
    """
    Generate a DataFrame from tref.csv

    Parameters
    ----------
    spark: SparkSession
    dataFrame: Input raw DataFrame

    Returns
    -------
    DataFrame with Tref channels mapped.
    """
    mapdf = mapper.ReadMapCSV(spark, "tref.csv")
    df_tref = mapper.Map(dataFrame, mapdf, ["event_id","value","dev","fp","det","geo"]).filter("edge == 0")
    
    return df_tref

def SubtractTref(mappedDF: F.DataFrame, trefDF: F.DataFrame) -> F.DataFrame:
    """
    Subtract tref value from "value" column in the imput DataFrame

    Parameters
    ----------
    mappedDF: Input DataFrame after mapper.Map() with dev, fp, det, geo columns
    trefDF: Tref DataFrame from tref.Tref()

    Returns
    -------
    DataFrame with the "value" column replaced with tref subtracted.
    """

    tref_df = trefDF.withColumnRenamed("value","tref_value")
    df = mappedDF.join(tref_df, on=["event_id", "dev","fp","det","geo"], how="left") \
           .withColumn("value", F.col("value") - F.col("tref_value")) \
           .drop("tref_value")

    return df

