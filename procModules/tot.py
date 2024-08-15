from pyspark.sql.functions import explode, col, arrays_zip, collect_list, row_number, DataFrame, broadcast
from pyspark.sql.window import Window

def Tot(dataFrame: "DataFrame") -> DataFrame:
    # Explode the lists
    df_exploded = dataFrame.withColumn("zipped", arrays_zip("value", "id", "edge")) \
                    .withColumn("exploded", explode("zipped")) \
                    .select("event_id", 
                            col("exploded.value").alias("value"), 
                            col("exploded.id").alias("id"), 
                            col("exploded.edge").alias("edge"))

    # Separate DataFrames for edge=1 and edge=0
    df_edge_1 = broadcast(df_exploded.filter(col("edge") == 1).select("event_id", "id", "value").alias("edge_1"))
    df_edge_0 = broadcast(df_exploded.filter(col("edge") == 0).select("event_id", "id", "value").alias("edge_0"))

    # Add row numbers to maintain order
    windowSpec = Window.partitionBy("event_id", "id").orderBy("value")
    df_edge_1 = df_edge_1.withColumn("row_num", row_number().over(windowSpec))
    df_edge_0 = df_edge_0.withColumn("row_num", row_number().over(windowSpec))

    # Join the DataFrames on event_id and id, and ensure edge=1 has the smallest value
    df_joined = df_edge_1.join(df_edge_0, ["event_id", "id"], "left_outer") \
        .filter((col("edge_0.value") > col("edge_1.value")) | col("edge_0.value").isNull())

    # Calculate the new values
    df_result = df_joined.withColumn("charge", col("edge_0.value") - col("edge_1.value")) \
                         .withColumn("timing", col("edge_1.value")) \
                         .select(col("event_id"), col("id"), col("charge"), col("timing")) \
                         .na.fill(0)  # Fill zeros if no corresponding edge=0 entry

    # Group by event_id and collect lists
    df_final = df_result.groupBy("event_id") \
                        .agg(
                            collect_list("id").alias("id"),
                            collect_list("charge").alias("charge"),
                            collect_list("timing").alias("timing")
                        )
    return broadcast(df_final)