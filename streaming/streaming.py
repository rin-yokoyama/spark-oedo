from pyspark.sql import SparkSession
from pyspark.sql.functions import col
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

# Apply Transformations
transformed_df = srppac.Process(spark, kafka_df, True, "sr91_x")

# Define Sink to Write to Kafka
#query = transformed_df.writeStream \
#    .format("kafka") \
#    .option("kafka.bootstrap.servers", "shfs02:9092") \
#    .option("topic", "processed-data") \
#    .option("checkpointLocation", "/tmp/spark-checkpoints") \
#    .start()

# Write the stream to Parquet files
query = transformed_df.writeStream \
    .format("parquet") \
    .option("checkpointLocation", "./rawdata/checkpoint") \
    .outputMode("append") \
    .start("./rawdata/processed-data")

# Start Streaming
query.awaitTermination()