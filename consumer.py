#!/usr/bin/env python
import json
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
from confluent_kafka import DeserializingConsumer


if __name__ == '__main__':
    # Parse the command line.
    parser = ArgumentParser()
    parser.add_argument('config_file', type=FileType('r'))
    args = parser.parse_args()
    # Subscribe to topic --> french stream at this point
    topic = "pksqlc-05r89FRENCH_STREAM"

    # Parse the configuration.
    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)
    config = dict(config_parser['default'])
    config.update(config_parser['consumer'])
    
    
    #Schema Registry Configuration
    schema_registry_config = dict(config_parser['schema_registry'])
    schema_registry_client = SchemaRegistryClient(schema_registry_config)
    


    #key - value deserializers
    key_deserializer = JSONDeserializer(json.dumps(json.load(open('./consumerKeySchema.json'))),from_dict = None )
    value_deserializer = JSONDeserializer(json.dumps(json.load(open('./consumerValueSchema.json'))), from_dict = None )

    consumer_conf = {'key.deserializer': key_deserializer,
                     'value.deserializer': value_deserializer,
                     'auto.offset.reset': "earliest"}


    #Updating the config with deserializers
    config.update(consumer_conf)



    # Create Consumer instance
    consumer = DeserializingConsumer(config)
    #consumer = Consumer(config) -->old version
    consumer.subscribe([topic])
    
    

    # Poll for new messages from Kafka and print them.
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                print("Waiting...")
            elif msg.error():
                print("ERROR: %s".format(msg.error()))
            else:
                print(msg.key(), msg.value(), msg.offset(), msg.partition())
                #print("Consumed event from topic {topic}: key = {key:12} value = {value:12}".format(topic=msg.topic(), key=msg.key(), value=msg.value()))
               
    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and commit final offsets
        consumer.close()