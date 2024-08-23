from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
import pyspark.sql.functions as F
import pyspark.sql.types as T
from detectorProcs.srppac import srppacMain
import pyarrow as pa
from schema import rawdata
from streaming.streaminConstants import WATERMARK_TS_COL, WATERMARK_WINDOW

# Create Spark Session
spark = SparkSession.builder \
    .appName("KafkaStructuredStreaming") \
    .getOrCreate()

# Define a UDF to convert binary Arrow data to a list of Spark Row objects
def arrow_to_spark(binary_arrow):
    if binary_arrow is None:
        return None
    reader = pa.ipc.open_stream(binary_arrow)
    table = reader.read_all()
    row_data = {field: table.column(i).to_pylist()[0] for i, field in enumerate(table.schema.names)}
    return T.Row(**row_data)

# Register the UDF without hardcoding the schema
arrow_udf = udf(arrow_to_spark, rawdata.rawdata_schema)

# Define Kafka Source
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "shfs02:9092") \
    .option("subscribe", "raw-arrow-data") \
    .option("startingOffsets", "latest") \
    .load() \

kafka_df = kafka_df.select(F.col("value").alias("arrow_data"), F.col("timestamp").alias(WATERMARK_TS_COL))

## Apply the UDF to deserialize the Arrow data
deserialized_data = kafka_df.withColumn("deserialized", arrow_udf(F.col("arrow_data")))
expanded_data = deserialized_data.select("deserialized.*",WATERMARK_TS_COL)

# Explode the `segdata` array into separate rows
segdata_exploded = expanded_data.withColumn("segdata", F.explode("segdata"))

# Expand the nested structure within `segdata`
segdata_expanded = segdata_exploded.select(
    "event_id",
    "runnumber",
    "ts",
    "segdata.dev",
    "segdata.fp",
    "segdata.mod",
    "segdata.det",
    F.explode("segdata.hits").alias("hit"),
    WATERMARK_TS_COL
)

# Expand the nested `hits` struct
hits_expanded = segdata_expanded.select(
    "event_id",
    "runnumber",
    "ts",
    "dev",
    "fp",
    "mod",
    "det",
    "hit.geo",
    "hit.ch",
    "hit.value",
    "hit.edge",
    WATERMARK_TS_COL
)

# Define Sink to Write to Kafka
#query = transformed_df.writeStream \
#    .outputMode("update") \
#    .format("kafka") \
#    .option("kafka.bootstrap.servers", "shfs02:9092") \
#    .option("topic", "processed-data") \
#    .option("checkpointLocation", ".rawdata/spark-checkpoints") \
#    .start()

# Allow data up to 10 seconds late in Unix Timestamp
with_watermark = hits_expanded.withWatermark(WATERMARK_TS_COL, WATERMARK_WINDOW)

def process_batch(batch_df: F.DataFrame, batch_id: int):
    """
    This function processes each micro-batch of data as if it were a batch DataFrame.
    The processed data is then written to a Parquet file.
    """
    # Process the batch DataFrame using your existing function
    result_df = srppacMain.Process(spark, batch_df, full=True, require="sr91_x")
    result_df = result_df.join(batch_df.dropDuplicates(["event_id"]).select("event_id", "runnumber", WATERMARK_TS_COL), on=["event_id"], how="left")

    # Write the processed DataFrame to a Parquet file
    # You can customize the file path with batch_id or timestamp to avoid overwriting
    result_df.write.mode("append").parquet(f"./rawdata/processed-data/batch_{batch_id}.parquet")

# Write the stream to Parquet files
query = with_watermark.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("append") \
    .start()

# Start Streaming
query.awaitTermination()