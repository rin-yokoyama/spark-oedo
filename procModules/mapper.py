from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, collect_list, DataFrame

def Map(spark: "SparkSession", dataFrame: "DataFrame", catName: "str") -> DataFrame:
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
    mapping_df = spark.read.csv("./map_files/"+catName+".csv", header=True, inferSchema=True)
    mapping_df = mapping_df.withColumn("id", col("id").cast("integer")) \
                           .withColumn("dev", col("dev").cast("integer")) \
                           .withColumn("fp", col("fp").cast("integer")) \
                           .withColumn("det", col("det").cast("integer")) \
                           .withColumn("geo", col("geo").cast("integer")) \
                           .withColumn("ch", col("ch").cast("integer")) \
   
    # Join with the mapping DataFrame to filter based on the criteria
    joined_df = exploded_df.join(mapping_df, 
                                 (exploded_df.dev == mapping_df.dev) & 
                                 (exploded_df.fp == mapping_df.fp) & 
                                 (exploded_df.det == mapping_df.det) & 
                                 (exploded_df.geo == mapping_df.geo) & 
                                 (exploded_df.ch == mapping_df.ch), 
                                 "inner")
    
    # Create the new columns mapped_value and mapped_id
    mapped_df = joined_df.select(col("event_id"),
                                 col("value").alias("mapped_value"),
                                 col("id"),
                                 col("edge"))
    
    # Group by device_id and detector_id and collect the lists
    result_df = mapped_df.groupBy("event_id").agg(
        collect_list("mapped_value").alias(catName + "_value"),
        collect_list("id").alias(catName+"_id"),
        collect_list("edge").alias(catName+"_edge")
    )

    # Join the result_df with the combined_df
    #combined_df = combined_df.join(result_df, on=["event_id"], how='left').drop(result_df.event_id)
    return result_df