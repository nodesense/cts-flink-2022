from pyflink.common import WatermarkStrategy, Row
from pyflink.common.serialization import Encoder
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FileSink, OutputFileConfig, NumberSequenceSource
from pyflink.datastream.functions import RuntimeContext, MapFunction
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.common.serialization import JsonRowDeserializationSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer,FlinkKafkaProducer
from pyflink.common.serialization import SimpleStringSchema, SerializationSchema,JsonRowSerializationSchema,Encoder
from pyflink.common.typeinfo import Types
import json


#     kafka-topics  --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 3 --topic sensor-data
#     kafka-console-producer --bootstrap-server localhost:9092 --topic sensor-data

# {"tag": "sensor1", "value": 32.3}
# {"tag": "sensor2", "value": 34.3}

env = StreamExecutionEnvironment.get_execution_environment()
env.enable_checkpointing(1000) # 1 sec

# the sql connector for kafka is used here as it's a fat jar and could avoid dependency issues
env.add_jars("file:///opt/flink-1.15.0/lib/flink-sql-connector-kafka-1.15.0.jar")

deserialization_schema = JsonRowDeserializationSchema.builder().type_info(
                             type_info=Types.ROW_NAMED(
                             ["tag","value"], [Types.STRING(), Types.DOUBLE()])).build()


kafka_consumer = FlinkKafkaConsumer(
    topics='sensor-data',
    deserialization_schema=deserialization_schema,
    properties={'bootstrap.servers': 'localhost:9092', 'group.id': 'sensor-data-group-1'})

# create a data stream from kafka consmer 
ds = env.add_source(kafka_consumer)

ds.print()
env.execute('state_access_demo')
