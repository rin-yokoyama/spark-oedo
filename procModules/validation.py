from pyspark.sql.functions import explode, col, arrays_zip, collect_list, DataFrame

def Validate(dataFrame: "DataFrame", valWindow: "list[2]") -> DataFrame:
    # Explode the lists
    df_result = dataFrame.withColumn("zipped", arrays_zip("value", "id", "edge")) \
                    .withColumn("exploded", explode("zipped")) \
                    .select("event_id", 
                            col("exploded.value").alias("value"), 
                            col("exploded.id").alias("id"), 
                            col("exploded.edge").alias("edge")) \
                    .filter((col("value") > valWindow[0]) & (col("value") < valWindow[1])) \
                    .groupBy("event_id") \
                    .agg(
                        collect_list("value").alias("value"),
                        collect_list("id").alias("id"),
                        collect_list("edge").alias("edge")
                    )
    return df_result