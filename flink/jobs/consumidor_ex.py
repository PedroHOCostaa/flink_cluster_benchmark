from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaSourceBuilder, KafkaOffsetsInitializer
from pyflink.common.serialization import SimpleStringSchema

env = StreamExecutionEnvironment.get_execution_environment()
# env.set_parallelism(1) # Adjust parallelism as needed
kafka_bootstrap_servers = "localhost:29092" 
kafka_topic = "input_stream"

kafka_source = KafkaSource \
    .builder() \
    .set_bootstrap_servers(kafka_bootstrap_servers) \
    .set_group_id("my_flink_consumer_group") \
    .set_topics(kafka_topic) \
    .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
    .set_value_only_deserializer(SimpleStringSchema()) \
    .build()

data_stream = env.from_source(
    source=kafka_source,
    watermark_strategy=None, # Or define a WatermarkStrategy
    source_name="kafka_source"
)
data_stream.print()
env.execute("PyFlink Kafka Consumer Job")