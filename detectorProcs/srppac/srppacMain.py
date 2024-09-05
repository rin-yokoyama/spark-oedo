from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from procModules import tref, manipulation, mapper, tot, calibrator, constants, monotoneTableConverter
from detectorProcs.srppac import srppacPosDqdx
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--input", help="input file name", required=True)
parser.add_argument("--partitions", help="number of partitions", nargs='?', const=1, type=int)
parser.add_argument("--full", help="output full time charge data", action="store_true")
parser.add_argument("--require", help="required ppac name. Rows without this ppac column will be deleted")
args = parser.parse_args()

#DETECTOR_NAMES = ["sr91","sr92","src1","src2","sr11","sr12"]
DETECTOR_NAMES = ["sr0"]

def Process(spark: SparkSession, rawDF: F.DataFrame, full: bool, require: str) -> F.DataFrame:

    # Read mapping file and create a DataFrame
    mapping_df = mapper.ReadMapCSV(spark, "srppac.csv", DETECTOR_NAMES)

    # Map tref channels first
    tref_df = tref.Tref(spark, rawDF)

    # Generate timecharge dataframes
    df = mapper.Map(rawDF, mapping_df, ["event_id","cat","value","id","edge","dev","fp","det","geo"])
    df = tref.SubtractTref(df,tref_df) # Tref subtraction
    df = manipulation.Validate(df,[-100000,100000])
         # Flip edge for cathode signals (trailingComesFirst=True)
    df = df.withColumn("edge", F.when(F.col("cat").endswith("a"), F.col("edge")).otherwise(1-F.col("edge")))
    df = tot.Tot(df)
    df = calibrator.ToFloat(df,"charge", 0, 0.09765627)
    df = calibrator.ToFloat(df,"timing", 0, 0.09765627)

    # process for each ppac
    ppacList = DETECTOR_NAMES
    srppac_df = rawDF.select("event_id").dropDuplicates(["event_id"])
    for ppac in ppacList:
        df_a = df.filter(F.col("cat") == ppac+"a")
        df_x = df.filter(F.col("cat") == ppac+"x")
        df_y = df.filter(F.col("cat") == ppac+"y")
        xc0_udf = monotoneTableConverter.getConverterUDF(spark, ppac+"_xc0.csv")
        yc0_udf = monotoneTableConverter.getConverterUDF(spark, ppac+"_yc0.csv")
        pos_x = srppacPosDqdx.srppacPosDqdx(df_x,xc0_udf,46.5,2.55,48,True)
        pos_y = srppacPosDqdx.srppacPosDqdx(df_y,yc0_udf,28.5,2.58,49.28,True)
        pos_x = pos_x.select("event_id",F.col("pos").alias(ppac+"x_pos"))
        pos_y = pos_y.select("event_id",F.col("pos").alias(ppac+"y_pos"))

        # Fill event data into an array per event and add the ppac name to the column name
        if full:
            # Full output
            df_a = df_a.groupBy("event_id").agg(
                F.collect_list("timing").alias(ppac+"a_timing"),
                F.collect_list("charge").alias(ppac +"a_charge")
            )
            df_x = df_x.groupBy("event_id").agg(
                F.collect_list("id").alias(ppac+"x_id"),
                F.collect_list("timing").alias(ppac+"x_timing"),
                F.collect_list("charge").alias(ppac +"x_charge")
            )
            df_x = df_x.join(pos_x, ["event_id"], how="left")
            df_y = df_y.groupBy("event_id").agg(
                F.collect_list("id").alias(ppac+"y_id"),
                F.collect_list("timing").alias(ppac+"y_timing"),
                F.collect_list("charge").alias(ppac +"y_charge")
            )
            df_y = df_y.join(pos_y, ["event_id"], how="left")
            # join to the final output data frame
            srppac_df = srppac_df.join(df_a, on=["event_id"], how="left")
            srppac_df = srppac_df.join(df_x, on=["event_id"], how="left")
            srppac_df = srppac_df.join(df_y, on=["event_id"], how="left")
        else:
            # Short output
            df_a = df_a.groupBy("event_id").agg(
                F.collect_list("timing").alias(ppac+"a_timing"),
                F.collect_list("charge").alias(ppac +"a_charge")
            )
            pos_x = pos_x.filter(F.col(ppac+"x_pos").isNotNull())
            result_df = pos_x.join(pos_y, ["event_id"], how="left")
            result_df = result_df.join(df_a, ["event_id"], how="left")
            if ppac == require:
                srppac_df = srppac_df.join(result_df, ["event_id"], how="inner")
            else:
                srppac_df = srppac_df.join(result_df, ["event_id"], how="left")

    return srppac_df
 
if __name__ == '__main__':
    # Initialize Spark session
            #.master("spark://"+constants.CLUSTER_NAME+":7077") \
    spark = SparkSession.builder \
            .master("local[*]") \
            .appName("SRPPAC") \
            .config("spark.driver.memory","20g") \
            .config("spark.executor.memory","20g") \
            .getOrCreate()

    # Read the parquet file
    raw_df = spark.read.parquet(constants.DATA_PATH+"/"+args.input+".parquet")

    if args.partitions != None:
        raw_df = raw_df.repartition(args.partitions)
    exploded_df = mapper.ExplodeRawData(raw_df)
    srppac_df = Process(spark, exploded_df, args.full, args.require)
    srppac_df.write.mode("overwrite").parquet(constants.DATA_PATH+"/"+args.input+f"_srppac.parquet")
    