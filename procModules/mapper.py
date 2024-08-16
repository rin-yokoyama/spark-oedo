from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, collect_list, DataFrame, broadcast

def Map(spark: "SparkSession", dataFrame: "DataFrame", catName: "str", mapping_df: "DataFrame") -> DataFrame:
    """
    Returns a mapped dataframe.

    Parameters
    ----------
    spark : spark session
    dataFrame : input dataframe
    catName : Category name. It needs to match the map
        file name, [catName].csv

    Returns
    -------
    output dataframe

    """
    # Explode the data column to get a flat structure
    exploded_df = dataFrame.withColumn("ex_segs", explode("segdata"))
    exploded_df = exploded_df.select("event_id", "ex_segs.dev", "ex_segs.fp", "ex_segs.det", "ex_segs.hits")
    exploded_df = exploded_df.withColumn("ex_hits", explode("hits"))
    exploded_df = exploded_df.select("event_id", "dev", "fp", "det", "ex_hits.geo", "ex_hits.ch", "ex_hits.value", "ex_hits.edge")
 
    #combined_df = exploded_df.select("event_id").distinct()

    # Read the mapping file
    #mapping_df = broadcast(spark.read.csv("./map_files/"+catName+".csv", header=True, inferSchema=True))
    mapping_df = mapping_df.withColumn("id", col("id").cast("integer")) \
                           .withColumn("dev", col("dev").cast("integer")) \
                           .withColumn("fp", col("fp").cast("integer")) \
                           .withColumn("det", col("det").cast("integer")) \
                           .withColumn("geo", col("geo").cast("integer")) \
                           .withColumn("ch", col("ch").cast("integer")) \
   
    # Join with the mapping DataFrame to filter based on the criteria
    joined_df = exploded_df.join(broadcast(mapping_df), 
                                 (exploded_df.dev == mapping_df.dev) & 
                                 (exploded_df.fp == mapping_df.fp) & 
                                 (exploded_df.det == mapping_df.det) & 
                                 (exploded_df.geo == mapping_df.geo) & 
                                 (exploded_df.ch == mapping_df.ch), 
                                 "inner")
    
    # Create the new columns mapped_value and mapped_id
    mapped_df = joined_df.select(col("event_id"),
                                 col("value"),
                                 col("id"),
                                 col("edge"))

    return mapped_df