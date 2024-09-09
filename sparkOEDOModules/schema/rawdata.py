from pyspark.sql.types import StructType, StructField, LongType, ArrayType

# Define the innermost schema for `hits`
hits_schema = StructType([
    StructField("geo", LongType(), True),
    StructField("ch", LongType(), True),
    StructField("value", LongType(), True),
    StructField("edge", LongType(), True)
])

# Define the schema for `segdata`
segdata_schema = StructType([
    StructField("dev", LongType(), True),
    StructField("fp", LongType(), True),
    StructField("mod", LongType(), True),
    StructField("det", LongType(), True),
    StructField("hits", ArrayType(hits_schema), True)
])

# Define the overall schema
rawdata_schema = StructType([
    StructField("event_id", LongType(), True),
    StructField("runnumber", LongType(), True),
    StructField("ts", LongType(), True),
    StructField("segdata", ArrayType(segdata_schema), True)
])