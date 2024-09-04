from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from procModules import tref, manipulation, mapper, tot, calibrator, constants
from detectorProcs.dia import twoSidedPlastic
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--input", help="input file name", required=True)
parser.add_argument("--partitions", help="number of partitions", nargs='?', const=1, type=int)
parser.add_argument("--full", help="output full time charge data", action="store_true")
parser.add_argument("--require", help="required ppac name. Rows without this ppac column will be deleted")
args = parser.parse_args()

DETECTOR_NAMES = ["dia3"]
#DETECTOR_NAMES = ["dia3","dias2"]

def Process(spark: SparkSession, rawDF: F.DataFrame, full: bool, require: str) -> F.DataFrame:

    # Mapper list
    mapList = [
        {"name": "dia3_pad","tref_id": 3},
        {"name": "dia3_stripL","tref_id": 3},
        {"name": "dia3_stripR","tref_id": 3}
    ]
    #mapList = [
    #    {"name": "dia3_pad","tref_id": 3},
    #    {"name": "dia3_stripL","tref_id": 3},
    #    {"name": "dia3_stripR","tref_id": 3},
    #    {"name": "dias2_pad","tref_id": 3},
    #    {"name": "dias2_stripL","tref_id": 3},
    #    {"name": "dias2_stripR","tref_id": 3}
    #]

    # Read all mapping files into a dictionary
    mapping_dfs = {}
    for cat in mapList:
        cat_name = cat["name"]
        mapping_dfs[cat_name] = spark.read.csv("hdfs://"+constants.CLUSTER_NAME+":9000"+constants.MAPFILE_PATH+f"/{cat_name}.csv", header=True, inferSchema=True).cache()

    # Map tref channels first
    tref_df = tref.Tref(spark, rawDF)

    # Generate timecharge dataframes for each category
    time_charge_dfs = {}
    for cat in mapList:
        df = mapper.Map(spark, rawDF, cat["name"], mapping_dfs[cat["name"]])
        df = manipulation.Subtract(df,tref_df,cat["tref_id"]) # Tref subtraction
        #df = manipulation.Validate(df,[-100000,100000])
        df = tot.Tot(df,trailingComesFirst=False)
        df = calibrator.ToFloat(df,"charge", 0, 0.0244140625)
        df = calibrator.ToFloat(df,"timing", 0, 0.0244140625)
        time_charge_dfs[cat["name"]] = df

    # process for each ppac
    detList = DETECTOR_NAMES
    detector_df = rawDF.select("event_id").dropDuplicates(["event_id"])
    for det in detList:
        df_p = time_charge_dfs[det+"_pad"]
        df_l = time_charge_dfs[det+"_stripL"]
        df_r = time_charge_dfs[det+"_stripR"]
        df_s = twoSidedPlastic.twoSidedPlastic(df_l, df_r, det+"strip", [-100,100])
        
        # Aggrigate by events
        df_p = df_p.groupBy("event_id").agg(
            F.collect_list("timing").alias(det+"pad_timing"),
            F.collect_list("charge").alias(det +"pad_charge")
        )
        df_l = df_l.groupBy("event_id").agg(
            F.collect_list("id").alias(det+"stripL_id"),
            F.collect_list("timing").alias(det+"stripL_timing"),
            F.collect_list("charge").alias(det +"stripL_charge")
        )
        df_r = df_r.groupBy("event_id").agg(
            F.collect_list("id").alias(det+"stripR_id"),
            F.collect_list("timing").alias(det+"stripR_timing"),
            F.collect_list("charge").alias(det +"stripR_charge")
        )

        if full:
            # Full output
            detector_df = detector_df.join(df_p,"event_id","left")
            detector_df = detector_df.join(df_l,"event_id","left")
            detector_df = detector_df.join(df_r,"event_id","left")
            detector_df = detector_df.join(df_s,"event_id","left")
        else:
            # Reduced output
            if det == require:
                how = "inner"
            else:
                how = "left"
            detector_df = detector_df.join(df_p,"event_id",how) 
            df_s = df_s.select("event_id", det+"strip_id", det+"strip_tdiff", det+"strip_tavg", det+"strip_qsqsum")
            detector_df = detector_df.join(df_s,"event_id",how)

    return detector_df
 
if __name__ == '__main__':
    # Initialize Spark session
    spark = SparkSession.builder \
            .master("spark://"+constants.CLUSTER_NAME+":7077") \
            .appName("DIA") \
            .config("spark.driver.memory","8g") \
            .config("spark.executor.memory","8g") \
            .getOrCreate()

    # Read the parquet file
    raw_df = spark.read.parquet("hdfs://"+ constants.CLUSTER_NAME + ":9000"+constants.DATA_PATH+"/"+args.input+".parquet")

    if args.partitions != None:
        raw_df = raw_df.repartition(args.partitions)
    exploded_df = mapper.ExplodeRawData(raw_df)
    detector_df = Process(spark, exploded_df, args.full, args.require)
    detector_df.write.mode("overwrite").parquet("hdfs://"+constants.CLUSTER_NAME+":9000"+constants.DATA_PATH+"/"+args.input+f"_dia.parquet")
    