from pyspark.sql import functions as F

def Validate(dataFrame: "F.DataFrame", valWindow: "list[2]") -> F.DataFrame:

    df_result = dataFrame.filter((F.col("value") > valWindow[0]) & (F.col("value") < valWindow[1]))
    return df_result

def Subtract(dataFrame: "F.DataFrame", dfSub: "F.DataFrame", id: int) -> F.DataFrame:
    """
    Subtracts the value with id==id in the dfSub dataframe from the value column in the dataFrame
    This is primary for tref subtraction
    dataFrame: input dataframe
    dfSub: mapped dataframe for subtraction (Tref data)
    id: id for the subtraction (tref id for the corresponding TDC module)
    return: dataframe with updated value
    """
    df_sub = dfSub.select("event_id", F.col("id").alias("sub_id"), F.col("value").alias("sub_value")) \
                  .filter(F.col("sub_id") == id)

    df_result = dataFrame.select("event_id", 
                            F.col("value").alias("orig_value"), 
                            F.col("id").alias("orig_id"), 
                            F.col("edge").alias("orig_edge")) \
                    .join(df_sub, "event_id", "left") \
                    .withColumn("new_value", F.col("orig_value") - F.col("sub_value")) \
                    .select("event_id",
                            F.col("orig_id").alias("id"),
                            F.col("new_value").alias("value"),
                            F.col("orig_edge").alias("edge"))
    return df_result
