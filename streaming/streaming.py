from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import pyarrow as pa
from detectorProcs.srppac import srppac

# Create Spark Session
spark = SparkSession.builder \
    .appName("KafkaStructuredStreaming") \
    .getOrCreate()

# Define Kafka Source
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "shfs02:9092") \
    .option("subscribe", "raw-arrow-data") \
    .option("startingOffsets", "latest") \
    .load()

# Define a UDF to deserialize the Arrow record batch
def deserialize_arrow(record_batch):
    reader = pa.ipc.open_file(record_batch)
    table = reader.read_all()
    pdf = table.to_pandas()
    return pdf

# Register the UDF
from pyspark.sql.functions import udf
from pyspark.sql.types import BinaryType

deserialize_arrow_udf = udf(deserialize_arrow, BinaryType())

# Deserialize Kafka data
deserialized_df = kafka_df.selectExpr("CAST(value AS BINARY) as value") \
    .withColumn("deserialized", deserialize_arrow_udf(col("value")))

# Convert to PySpark DataFrame
final_df = spark.createDataFrame(deserialized_df)

# Apply Transformations
transformed_df = srppac.Process(spark, final_df, False, "sr91_x")

# Convert Data to String (or another appropriate format for Kafka)
# Assuming we convert a DataFrame column to JSON format for Kafka
transformed_df = transformed_df.selectExpr("CAST(new_field AS STRING) as value")

# Define Sink to Write to Kafka
query = transformed_df.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "shfs02:9092") \
    .option("topic", "processed-data") \
    .option("checkpointLocation", "/tmp/spark-checkpoints") \
    .start()

# Start Streaming
query.awaitTermination()