from kafka import KafkaConsumer, TopicPartition
import pyarrow as pa
import pyarrow.ipc as ipc
import io
import time
import sys

if len(sys.argv) < 3:
    print("Usage: python saveParquet.py [topic_name] [neve] [first]")

# Kafka consumer configuration
bootstrap_servers = ['shfs02:9092']

# Create a Kafka consumer
consumer = KafkaConsumer(
    bootstrap_servers=bootstrap_servers,
    auto_offset_reset='none',
)

# Define the topic and partition
topic_name = sys.argv[1]
partition = 0
topic_partition = TopicPartition(topic_name, partition)

# Assign the consumer to the specific topic and partition
consumer.assign([topic_partition])

if len(sys.argv)<4:
    # Seek to the last available offset
    consumer.seek_to_end(topic_partition)
else:
    consumer.seek(topic_partition,int(sys.argv[3]))

filename = "output.parquet"
if len(sys.argv)==5:
    filename = sys.argv[4]

neve = int(sys.argv[2])
count = 0

tables = []

for message in consumer:

    if len(message.value) < 1:
        continue
    # The message value is expected to be a byte buffer containing serialized Arrow data
    buffer = io.BytesIO(message.value)

    # Use Arrow IPC to read the serialized RecordBatch
    reader = ipc.open_stream(buffer)
    batch = reader.read_next_batch()

    # Optionally convert the RecordBatch to a Table if you need to work with Table API
    table = pa.Table.from_batches([batch])

    tables.append(table)
    #print(table)

    count = count + 1
    if count >= neve:
        break


concat = pa.concat_tables(tables)
concat.to_pandas().to_parquet(filename)

# Close the consumer when done
consumer.close()
