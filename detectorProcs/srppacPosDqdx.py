from pyspark.sql import functions as F
from pyspark.sql.window import Window

def srppacPosDqdx(dataFrame: F.DataFrame, center: float, stripWidth: float, detOffset: float = 0, turned: bool = False):
    """
    Calculate position from SRPPAC raw data
    dataFrame: input dataframe that contains event_id, id, charge, timing
    center: Center strip position
    stripWidth: strip width of the PPAC
    detOffset: Offset of the detector position (default: 0)
    turned: A flag for flipping the axis (default: False)
    return: dataframe with two columns: event_id and position
    """
    # Step 1: Identify c0, c1, id0, id1 for each event_id
    # Define a window partition by event_id and order by charge descending
    window_spec = Window.partitionBy("event_id").orderBy(F.desc("charge"))

    # Find the largest and second largest value within each event_id
    df_with_c0_c1 = dataFrame.withColumn("rank", F.row_number().over(window_spec)) \
        .withColumn("c0", F.when(F.col("rank") == 1, F.col("charge")).otherwise(None)) \
        .withColumn("t0", F.when(F.col("rank") == 1, F.col("timing")).otherwise(None)) \
        .withColumn("id0", F.when(F.col("rank") == 1, F.col("id")).otherwise(None)) \
        .withColumn("c1", F.when(F.col("rank") == 2, F.col("charge")).otherwise(None)) \
        .withColumn("t1", F.when(F.col("rank") == 2, F.col("timing")).otherwise(None)) \
        .withColumn("id1", F.when(F.col("rank") == 2, F.col("id")).otherwise(None))

    # Use groupBy and agg to collect the largest and second-largest values for each event_id
    df_with_c0_c1 = df_with_c0_c1.groupBy("event_id").agg(
        F.max("c0").alias("c0"),
        F.max("c1").alias("c1"),
        F.max("id0").alias("id0"),
        F.max("id1").alias("id1")
    )

    # Join back with the original DataFrame on event_id
    #df_joined = df.join(df_with_c0_c1, on="event_id")

    # Step 2: Implement the calculation for "pos"
    df_with_pos = df_with_c0_c1.withColumn(
        "pos",
        (F.when(F.lit(turned), -1).otherwise(1)) *
        ((F.col("id0") - F.lit(center) + (1 - (F.col("c0") - F.col("c1")) / (F.col("c0") + F.col("c1")))) *
        (F.when(F.col("id0") < F.col("id1"), 1).otherwise(-1)) * 0.5) *
        F.lit(stripWidth) - F.lit(detOffset)
    )

    # Select the desired columns along with the new "pos" column
    df_final = df_with_pos.select("event_id", "pos")

    return df_final