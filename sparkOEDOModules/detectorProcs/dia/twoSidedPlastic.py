from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from sparkOEDOModules.procModules import constants

def twoSidedPlastic(dfL: DataFrame, dfR: DataFrame, detName: str, timeWindow: tuple[float, float]) -> DataFrame:
    """
    Calculate position from SRPPAC raw data

    Parameters
    ----------
    dfL: input dataframe for the left plastic that contains event_id, id, charge, timing
    dfR: input dataframe for the right plastic that contains event_id, id, charge, timing
    detName: detector name which will be prepended to the output column names
    timeWindow: list of [min, max] for the timewindow correlating L and R by (L.time - R.time)

    Returns
    -------
    Dataframe with columns: event_id and 
    """
    # Join dfL and dfR on event_id
    df_joined = dfL.alias("dfL").join(dfR.alias("dfR"), on=[constants.ID_COLNAME,"id"], how="inner")

    # Calculate the time difference
    df_with_diff = df_joined.withColumn("tdiff", F.col("dfL.timing") - F.col("dfR.timing")) \
                            .withColumn("tavg", (F.col("dfL.timing") + F.col("dfR.timing") / 2.)) \
                            .withColumn("qdiff", F.col("dfL.charge") - F.col("dfR.charge")) \
                            .withColumn("qavg", (F.col("dfL.charge") + F.col("dfR.charge") / 2.)) \
                            .withColumn("qlog", F.log(F.col("dfL.charge") / F.col("dfR.charge"))) \
                            .withColumn("qsqsum", F.sqrt(F.col("dfL.charge") * F.col("dfR.charge")))

    # Filter rows based on the time difference within the window
    df_filtered = df_with_diff.filter((F.col("tdiff") >= timeWindow[0]) & (F.col("tdiff") <= timeWindow[1]))

    # Group by event_id and collect the time differences as an array
    df_result = df_filtered.groupBy(constants.ID_COLNAME,"id").agg(
                                F.collect_list("tdiff").alias(detName + "_tdiff"),
                                F.collect_list("tavg").alias(detName + "_tavg"),
                                F.collect_list("qdiff").alias(detName + "_qdiff"),
                                F.collect_list("qavg").alias(detName + "_qavg"),
                                F.collect_list("qlog").alias(detName + "_qlog"),
                                F.collect_list("qsqsum").alias(detName + "_qsqsum")
                            ) \
                            .withColumnRenamed("id", detName + "_id")

    return df_result