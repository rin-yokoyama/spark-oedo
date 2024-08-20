from kafka import KafkaConsumer, TopicPartition
import sys

# Initialize Kafka Consumer
consumer = KafkaConsumer(
    bootstrap_servers=['shfs02:9092'],
    group_id='offsetChecker',
    auto_offset_reset='none'  # Ignore default offset reset behavior
)

# Define the topic
topic_name = sys.argv[1]

# Get all partitions for the given topic
partitions = consumer.partitions_for_topic(topic_name)

if partitions is None:
    print(f"Topic {topic_name} does not exist or no partitions found.")
    sys.exit(1)

# Initialize a dictionary to store the end offsets
end_offsets = {}

# Iterate over each partition to get the latest offset
for partition in partitions:
    topic_partition = TopicPartition(topic_name, partition)
    consumer.assign([topic_partition])
    end_off = consumer.end_offsets([topic_partition])
    end_offsets[partition] = end_off[topic_partition]

# Print the latest offsets for all partitions
print(f"Latest offsets for each partition: {end_offsets}")

# Optionally, if you only want the highest offset across all partitions
latest_offset = max(end_offsets.values())
print(f"Latest offset across all partitions: {latest_offset}")
