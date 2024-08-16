from pyspark.sql.functions import col, when, row_number, DataFrame, broadcast
from pyspark.sql.window import Window

def Tot(dataFrame: "DataFrame", trailingComesFirst: bool = True) -> DataFrame:
    """
    Generating timing charge data from ToT
    dataFrame: Input data frame with event_id, id, value columns
    trailingComesFirst: bit for inverted logic signal
    return: dataframe with id, charge, timing data
    """
    if trailingComesFirst:
        edges = [1,0]
    else:
        edges = [0,1]
    # Separate DataFrames for edge=1 and edge=0
    df_edge_1 = dataFrame.filter(col("edge") == edges[0]).select("event_id", "id", "value")
    df_edge_0 = dataFrame.filter(col("edge") == edges[1]).select(col("event_id").alias("event_id0"), col("id").alias("id0"), col("value").alias("value0"))

    # Join the DataFrames on event_id and id, calculate charge but fill zero if value0 is null, and ensure edge=1 has the smaller value
    df_joined = df_edge_1.join(broadcast(df_edge_0), on=[df_edge_1.id == df_edge_0.id0, df_edge_1.event_id == df_edge_0.event_id0], how="outer")  \
                         .withColumn("charge", when(col("value0").isNull(), 0).otherwise(col("value0") - col("value"))) \
                         .filter(col("charge")>0)

    # Use a window function to find the smallest positive difference per (event_id, id, value)
    window_spec = Window.partitionBy("event_id", "id", "value").orderBy("charge")
    df_result = df_joined.withColumn("rank", row_number().over(window_spec)).filter(col("rank") == 1).drop("rank") \
                         .select("event_id","id","charge",col("value").alias("timing"))

    return df_result