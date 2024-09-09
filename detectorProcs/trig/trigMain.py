from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from procModules import tref, manipulation, mapper, tot, calibrator, constants
from detectorProcs.dia import twoSidedPlastic
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--input", help="input file name", required=True)
parser.add_argument("--partitions", help="number of partitions", nargs='?', const=1, type=int)
parser.add_argument("--full", help="output full time charge data", action="store_true")
args = parser.parse_args()

mapping_df = None
tref_mapping_df = None

def LoadCSVFiles(spark: SparkSession):
    """
    Load CSV Files to DataFrames. This function should be called once before calling Process()

    Parameters
    ----------
    spark: SparkSession
    """
    global mapping_df, tref_mapping_df
    if mapping_df is None:
        mapping_df = mapper.ReadMapCSV(spark, "trig.csv", "trig")
        tref_mapping_df = tref.ReadCSV(spark)

def Process(rawDF: F.DataFrame, full: bool) -> F.DataFrame:
    """
    Main processor function for trigger selection

    Parameters
    ----------
    rawDF: Input rawdata DataFrame
    full: Flag for full outputs

    Returns
    -------
    Output DataFrame
    """
    if mapping_df is None:
        print("Call LoadCSVFiles() before calling Process()")

    # Map tref channels first
    tref_df = tref.Tref(rawDF, tref_mapping_df)

    # Generate timecharge dataframes for each category
    df = mapper.Map(rawDF, mapping_df, [constants.ID_COLNAME, "cat", "value", "id", "edge", "dev", "fp", "det", "geo"])
    df = tref.SubtractTref(df, tref_df)
    df = manipulation.Validate(df,[-100000,100000])
    df = calibrator.ToFloat(df,"value", 0, 0.0244140625)

    df = df.groupBy(constants.ID_COLNAME).agg(
        F.collect_list("id").alias("trig_id"),
        F.collect_list("value").alias("trig_timing")
    )

    # Add a new column with the first element of the array
    df = df.withColumn(
        "trig",
        F.when(F.size(F.col("trig_id")) > 0, F.col("trig_id")[0])  # Check if the array has elements
         .otherwise(None)  # Return None (or some default value) if the array is empty
    )

    detector_df = detector_df.join(df,constants.ID_COLNAME,"left")
    if full:
        return detector_df
    else:
        return detector_df.select(constants.ID_COLNAME, "trig")
 
if __name__ == '__main__':
    # Initialize Spark session
    spark = SparkSession.builder \
            .master("spark://"+constants.CLUSTER_NAME+":7077") \
            .appName("DIA") \
            .config("spark.driver.memory","8g") \
            .config("spark.executor.memory","8g") \
            .getOrCreate()

    # Read the parquet file
    raw_df = spark.read.parquet(constants.DATA_PATH+"/"+args.input+".parquet")

    if args.partitions != None:
        raw_df = raw_df.repartition(args.partitions)
    exploded_df = mapper.ExplodeRawData(raw_df)
    LoadCSVFiles(spark)
    detector_df = Process(exploded_df, args.full)
    detector_df.write.mode("overwrite").parquet(constants.DATA_PATH+"/"+args.input+f"_dia.parquet")
    