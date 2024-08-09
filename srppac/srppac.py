from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from procModules import tref, manipulation, mapper, validation, tot, calibrator

# Initialize Spark session
spark = SparkSession.builder.appName("MapperOedo") \
        .config("spark.driver.memory","16g") \
        .config("spark.executor.memory","16g") \
        .config("spark.sql.shuffle.partitions","200") \
        .config("spark.sql.parquet.columnarReaderBatchSize", "1024") \
        .config("spark.sql.parquet.enableVectorizedReader", "false") \
        .config("spark.sql.sources.bucketing.enabled", "false") \
        .getOrCreate()

# Read the parquet file
raw_df = spark.read.parquet("/home/ryokoyam/spark-oedo/rawdata/calib1029_short.parquet")

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

# Map tref channels first
tref_df = tref.Tref(spark, raw_df)

srppac_df = raw_df.select("event_id")

for cat in mapList:
    df = mapper.Map(spark, raw_df, cat["name"])
    df = manipulation.Subtract(df,tref_df,cat["tref_id"]) # Tref subtraction
    df = validation.Validate(df,[-100000,100000])
    df = tot.Tot(df)
    df = calibrator.ToFloat(df,"charge")
    df = calibrator.ToFloat(df,"timing")

    # Add category name to the column names and join to the final output data frame
    new_column_names = ["event_id", cat["name"] + "_id", cat["name"] + "_charge", cat["name"] + "_timing"]
    df = df.toDF(*new_column_names)
    srppac_df = srppac_df.join(df,on="event_id",how="inner")

srppac_df.write.mode("overwrite").parquet("/home/ryokoyam/spark-oedo/rawdata/calib1029_short_srppac.parquet")
