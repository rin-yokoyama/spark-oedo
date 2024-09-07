from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from procModules import mapper, constants

def ReadCSV(spark: SparkSession) -> F.DataFrame:
    """
    Read tref.csv file
    
    Parameters
    ----------
    spark: SparkSession

    Returns
    -------
    DataFrame from tref.csv
    """
    return mapper.ReadMapCSV(spark, "tref.csv")

def Tref(dataFrame: "F.DataFrame", mapdf: F.DataFrame) -> F.DataFrame:
    """
    Generate a DataFrame from tref.csv

    Parameters
    ----------
    dataFrame: Input raw DataFrame
    mapdf: Tref map dataframe

    Returns
    -------
    DataFrame with Tref channels mapped.
    """
    df_tref = mapper.Map(dataFrame, mapdf, [constants.ID_COLNAME,"value","dev","fp","det","geo"]).filter("edge == 0")
    
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
    df = mappedDF.join(tref_df, on=[constants.ID_COLNAME, "dev","fp","det","geo"], how="left") \
           .withColumn("value", F.col("value") - F.col("tref_value")) \
           .drop("tref_value")

    return df

