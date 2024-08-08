from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, arrays_zip, collect_list, DataFrame
from pyspark.sql.window import Window

def Validate(dataFrame: "DataFrame", catName: "str", valWindow: "list[2]") -> DataFrame:
    # Explode the lists
    df_result = dataFrame.withColumn("zipped", arrays_zip(catName+"_value", catName+"_id", catName+"_edge")) \
                    .withColumn("exploded", explode("zipped")) \
                    .select("event_id", 
                            col("exploded."+catName+"_value").alias("value"), 
                            col("exploded."+catName+"_id").alias("id"), 
                            col("exploded."+catName+"_edge").alias("edge")) \
                    .filter((col("value") > valWindow[0]) & (col("value") < valWindow[1])) \
                    .groupBy("event_id") \
                    .agg(
                        collect_list("id").alias(catName+"_id"),
                        collect_list("value").alias(catName+"_value"),
                        collect_list("edge").alias(catName+"_edge")
                    )
    return df_result