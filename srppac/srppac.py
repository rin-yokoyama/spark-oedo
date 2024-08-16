from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from procModules import tref, manipulation, mapper, tot, calibrator
from detectorProcs import srppacPosDqdx
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--full", help="output full time charge data", action="store_true")
parser.add_argument("--require", help="required ppac name. Rows without this ppac column will be deleted")
args = parser.parse_args()

# Initialize Spark session
spark = SparkSession.builder.appName("MapperOedo") \
        .getOrCreate()

# Read the parquet file
raw_df = spark.read.parquet("/home/ryokoyam/spark-oedo/rawdata/calib1029.parquet")

# Mapper list
mapList = [
    {"name": "sr91_a","tref_id": 6},
    {"name": "sr91_x","tref_id": 6},
    {"name": "sr91_y","tref_id": 7},
    {"name": "sr92_a","tref_id": 8},
    {"name": "sr92_x","tref_id": 8},
    {"name": "sr92_y","tref_id": 9},
    {"name": "src1_a","tref_id": 12},
    {"name": "src1_x","tref_id": 12},
    {"name": "src1_y","tref_id": 13},
    {"name": "src2_a","tref_id": 14},
    {"name": "src2_x","tref_id": 14},
    {"name": "src2_y","tref_id": 15},
    {"name": "sr11_a","tref_id": 16},
    {"name": "sr11_x","tref_id": 16},
    {"name": "sr11_y","tref_id": 17},
    {"name": "sr12_a","tref_id": 18},
    {"name": "sr12_x","tref_id": 18},
    {"name": "sr12_y","tref_id": 19}
]

# Read all mapping files into a dictionary
mapping_dfs = {}
for cat in mapList:
    cat_name = cat["name"]
    mapping_dfs[cat_name] = spark.read.csv(f"./map_files/{cat_name}.csv", header=True, inferSchema=True).cache()

# Map tref channels first
tref_df = tref.Tref(spark, raw_df)

# Generate timecharge dataframes for each category
time_charge_dfs = {}
for cat in mapList:
    df = mapper.Map(spark, raw_df, cat["name"], mapping_dfs[cat["name"]])
    df = manipulation.Subtract(df,tref_df,cat["tref_id"]) # Tref subtraction
    df = manipulation.Validate(df,[-100000,100000])
    if cat["name"][-1:] == "a":
        df = tot.Tot(df,trailingComesFirst=False)
    else:
        df = tot.Tot(df,trailingComesFirst=True)
    df = calibrator.ToFloat(df,"charge", 0, 0.09765627)
    df = calibrator.ToFloat(df,"timing", 0, 0.09765627)
    time_charge_dfs[cat["name"]] = df

# process for each ppac
ppacList = ["sr91","sr92","src1","src2","sr11","sr12"]
srppac_df = raw_df.select("event_id")
for ppac in ppacList:
    df_a = time_charge_dfs[ppac+"_a"]
    df_x = time_charge_dfs[ppac+"_x"]
    df_y = time_charge_dfs[ppac+"_y"]
    pos_x = srppacPosDqdx.srppacPosDqdx(df_x,46.5,2.55,0,True)
    pos_y = srppacPosDqdx.srppacPosDqdx(df_y,46.5,2.55,0,True)
    pos_x = pos_x.select("event_id",F.col("pos").alias(ppac+"_x_pos"))
    pos_y = pos_y.select("event_id",F.col("pos").alias(ppac+"_y_pos"))

    # Fill event data into an array per event and add the ppac name to the column name
    if args.full:
        # Full output
        df_a = df_a.groupBy("event_id").agg(
            F.collect_list("timing").alias(ppac+"_a_timing"),
            F.collect_list("charge").alias(ppac +"_a_charge")
        )
        df_x = df_x.groupBy("event_id").agg(
            F.collect_list("id").alias(ppac+"_x_id"),
            F.collect_list("timing").alias(ppac+"_x_timing"),
            F.collect_list("charge").alias(ppac +"_x_charge")
        )
        df_x = df_x.join(pos_x, "event_id", "left")
        df_y = df_y.groupBy("event_id").agg(
            F.collect_list("id").alias(ppac+"_y_id"),
            F.collect_list("timing").alias(ppac+"_y_timing"),
            F.collect_list("charge").alias(ppac +"_y_charge")
        )
        df_y = df_y.join(pos_y, "event_id", "left")
        # join to the final output data frame
        srppac_df = srppac_df.join(df_a, on=["event_id"],how="fullouter")
        srppac_df = srppac_df.join(df_x, on=["event_id"],how="fullouter")
        srppac_df = srppac_df.join(df_y, on=["event_id"],how="fullouter")
    else:
        # Short output
        df_a = df_a.groupBy("event_id").agg(
            F.collect_list("timing").alias(ppac+"_a_timing"),
            F.collect_list("charge").alias(ppac +"_a_charge")
        )
        pos_x = pos_x.filter(F.col(ppac+"_x_pos").isNotNull())
        result_df = pos_x.join(pos_y, on=["event_id"], how="left")
        result_df = result_df.join(df_a, on=["event_id"], how="left")
        if ppac == args.require:
            srppac_df = srppac_df.join(result_df, on=["event_id"], how="inner")
        else:
            srppac_df = srppac_df.join(result_df, on=["event_id"], how="fullouter")
 
srppac_df.write.mode("overwrite").parquet("/home/ryokoyam/spark-oedo/rawdata/calib1029_srppac.parquet")
