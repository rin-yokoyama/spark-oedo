from pyspark.sql import functions as F

def Subtract(dataFrame: "F.DataFrame", dfSub: "F.DataFrame", id: int) -> F.DataFrame:
    df_sub = dfSub.withColumn(
        "value",
        F.expr("filter(value, (x, i) -> id[i] == {})".format(id))
    ).withColumn(
        "edge",
        F.expr("filter(edge, (x, i) -> id[i] == {})".format(id))
    ).withColumn(
        "id",
        F.expr("filter(id, (x, i) -> x == {})".format(id))
    ).withColumn(
        "zipped", F.arrays_zip("value","id","edge")
    ).withColumn(
        "exploded", F.explode("zipped")
    ).select(
        "event_id",
        F.col("exploded.value").alias("sub_value"), 
    )

    # Explode the lists
    df_result = dataFrame.withColumn("zipped", F.arrays_zip("value", "id", "edge")) \
                    .withColumn("exploded", F.explode("zipped")) \
                    .select("event_id", 
                            F.col("exploded.value").alias("orig_value"), 
                            F.col("exploded.id").alias("orig_id"), 
                            F.col("exploded.edge").alias("orig_edge")) \
                    .join(df_sub, "event_id") \
                    .withColumn("new_value", F.col("orig_value") - F.col("sub_value")) \
                    .groupBy("event_id") \
                    .agg(
                        F.collect_list("orig_id").alias("id"),
                        F.collect_list("new_value").alias("value"),
                        F.collect_list("orig_edge").alias("edge")
                    ) 
    return df_result
