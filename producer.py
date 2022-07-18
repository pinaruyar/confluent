#!/usr/bin/env python

import requests
import json
import sys
from random import choice
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Producer
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient


if __name__ == '__main__':
    # Parse the command line.
    parser = ArgumentParser()
    parser.add_argument('config_file', type=FileType('r'))
    args = parser.parse_args()

    # Parse the configuration.
    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)
    config = dict(config_parser['default'])
    #print(config)

    #Schema Registry Configuration
    schema_registry_config = dict(config_parser['schema_registry'])
    schema_registry_client = SchemaRegistryClient(schema_registry_config)

    #key-value serializer
    key_serializer = JSONSerializer(json.dumps(json.load(open('./keySchema.json'))), schema_registry_client)
    value_serializer = JSONSerializer(json.dumps(json.load(open('./valueSchema.json'))), schema_registry_client)

    #merging the old config file with the new one    
    producer_conf = {'key.serializer': key_serializer,
                     'value.serializer': value_serializer}
    config.update(producer_conf)

    # Create Producer instance
    producer = SerializingProducer(config)
    #producer = Producer(config) --> old version

    
    # Optional per-message delivery callback (triggered by poll() or flush())
    # when a message has been successfully delivered or permanently
    # failed delivery (after retries).
    def delivery_callback(err, msg):
        if err:
            print('ERROR: Message failed delivery: {}'.format(err))
        else:
            #print(msg.key())
            print("Produced event to topic {topic}".format(topic=msg.topic()))

    response_API = requests.get('https://collectionapi.metmuseum.org/public/collection/v1/objects')
    #print(response_API.status_code)
    data = response_API.text
    parse_json = json.loads(data)
    objectIDs = parse_json['objectIDs'] #creating a list of objectIDs i pulled
    #print(type(objectIDs))
    topic = "artworks"
    while True:
        objectID = choice(objectIDs) #randomly choosing the artworks by their objectIDs
        objectID = str(objectID)
        newLink = 'https://collectionapi.metmuseum.org/public/collection/v1/objects/'+ objectID
        object_response_API = requests.get(newLink)
        dataText = object_response_API.text
        parsed_json = json.loads(dataText)
        #print(parsed_json.keys())
        artistNationality = parsed_json['artistNationality']
        objectBeginDate = parsed_json['objectBeginDate']
        title = parsed_json['title']
        artist = parsed_json['artistDisplayName']


        #passing the key - value pair 
        producer.produce('artworks',{'title' : title}, {'artist': artist, 'artistNationality': artistNationality, 'objectBeginDate': objectBeginDate}, on_delivery=delivery_callback)
        
   
   
    # Block until the messages are sent.
    producer.poll(10000)
    producer.flush()