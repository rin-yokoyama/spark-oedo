from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, DataFrame, broadcast
from procModules import constants
from functools import reduce

def ReadMapCSV(spark: SparkSession, fileName: str, detNames: list = []) -> DataFrame:
    """
    Read map csv files from a list of category names.
    Lines starting with "#" will be skipped as comments.

    Parameters
    ----------
    spark: SparkSession
    fileName: File name of the mapping CSV.
    detNames: List of detector names to load

    Return
    ------
    Dict of category names and mapping DataFrames
    """

    df = spark.read.option("comment","#").csv(constants.MAPFILE_PATH+f"/{fileName}", header=True, inferSchema=True).cache()
    if len(detNames)>0:
        filter_condition = reduce(lambda cond, value: cond | col("cat").startswith(value), detNames, col("cat").startswith(detNames[0]))
        df = df.filter(filter_condition)

    return df 

def ExplodeRawData(dataFrame: DataFrame) -> DataFrame:
    """
    Explodes raw data by segdata arrays and hit arrays and returns the exploded DF

    Parameters
    ----------
    dataFrame : input dataframe

    Returns
    -------
    output dataframe
    """
    # Explode the data column to get a flat structure
    exploded_df = dataFrame.withColumn("ex_segs", explode("segdata"))
    exploded_df = exploded_df.select("event_id", "ex_segs.dev", "ex_segs.fp", "ex_segs.det", "ex_segs.hits")
    exploded_df = exploded_df.withColumn("ex_hits", explode("hits"))
    exploded_df = exploded_df.select("event_id", "dev", "fp", "det", "ex_hits.geo", "ex_hits.ch", "ex_hits.value", "ex_hits.edge")
    return exploded_df

def Map(dataFrame: "DataFrame", mapping_df: "DataFrame", cols: list = ["event_id", "cat", "value", "id"]) -> DataFrame:
    """
    Returns a mapped dataframe.

    Parameters
    ----------
    dataFrame : input dataframe (Call ExplodeRawData() before when you want to map raw data read from a parquet file.)
    mapping_df: DataFrame from the mapping CSV file.
    cols: list of column names to keep in the output DataFrame.

    Returns
    -------
    output dataframe

    """
 
    # Read the mapping file
    #mapping_df = broadcast(spark.read.csv("./map_files/"+catName+".csv", header=True, inferSchema=True))
    mapping_df = mapping_df.withColumn("id", col("id").cast("integer")) \
                           .withColumn("dev", col("dev").cast("integer")) \
                           .withColumn("fp", col("fp").cast("integer")) \
                           .withColumn("det", col("det").cast("integer")) \
                           .withColumn("geo", col("geo").cast("integer")) \
                           .withColumn("ch", col("ch").cast("integer")) \
   
    joined_df = dataFrame.join(mapping_df, on=["dev","fp","det","geo","ch"], how="inner") 

    # Create the new columns mapped_value and mapped_id
    mapped_df = joined_df.select(cols)

    return mapped_df