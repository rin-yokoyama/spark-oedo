from pyspark.sql import SparkSession
from procModules import mapper, validation, tot

# Initialize Spark session
spark = SparkSession.builder.appName("MapperOedo") \
        .config("spark.sql.parquet.columnarReaderBatchSize", "1024") \
        .config("spark.sql.parquet.enableVectorizedReader", "false") \
        .config("spark.sql.sources.bucketing.enabled", "false") \
        .getOrCreate()

# Read the parquet file
df = spark.read.parquet("/home/ryokoyam/spark-oedo/rawdata/calib1029_short.parquet")
    
# Mapper list
mapList = [
    #"sr91_a",
    "sr91_x",
    "sr91_y"
    #"sr92_a",
    #"sr92_x",
    #"sr92_y",
    #"src1_a",
    #"src1_x",
    #"src1_y",
    #"src2_a",
    #"src2_x",
    #"src2_y",
    #"sr11_a",
    #"sr11_x",
    #"sr11_y",
    #"sr12_a",
    #"sr12_x",
    #"sr12_y"
]

catName = "sr91_x"
result_df = mapper.Map(spark, df, catName)
result_df = validation.Validate(result_df,catName,[-100000,100000])
result_df.write.mode("overwrite").parquet("/home/ryokoyam/spark-oedo/rawdata/calib1029_short_mapped.parquet")
result_df = tot.Tot(result_df, catName)
result_df.write.mode("overwrite").parquet("/home/ryokoyam/spark-oedo/rawdata/calib1029_short_tot.parquet")

# Save to parquet
#if result_df is not None:
#    result_df.write.mode("overwrite").parquet("/home/ryokoyam/spark-oedo/rawdata/calib1029_short_mapped.parquet")

# Save the result to a new Parquet file with a single part file
#result_df.coalesce(1).write.parquet("/home/ryokoyam/spark-oedo/rawdata/calib1029_short_mapped_single.parquet")