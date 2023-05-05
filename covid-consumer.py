import argparse
from pyspark.sql import SparkSession
from confluent_kafka import Consumer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient
import pandas as pd
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

API_KEY = <>
ENDPOINT_SCHEMA_URL  = <>
API_SECRET_KEY = <>
BOOTSTRAP_SERVER = <>
SECURITY_PROTOCOL = 'SASL_SSL'
SSL_MACHENISM = 'PLAIN'
SCHEMA_REGISTRY_API_KEY = <> 
SCHEMA_REGISTRY_API_SECRET = <>


def sasl_conf():

    sasl_conf = {'sasl.mechanism': SSL_MACHENISM,
                 # Set to SASL_SSL to enable TLS support.
                #  'security.protocol': 'SASL_PLAINTEXT'}
                'bootstrap.servers':BOOTSTRAP_SERVER,
                'security.protocol': SECURITY_PROTOCOL,
                'sasl.username': API_KEY,
                'sasl.password': API_SECRET_KEY
                }
    return sasl_conf



def schema_config():
    return {'url':ENDPOINT_SCHEMA_URL,
    
    'basic.auth.user.info':f"{SCHEMA_REGISTRY_API_KEY}:{SCHEMA_REGISTRY_API_SECRET}"

    }


class Case:
    def __init__(self,record:dict):
        for k,v in record.items():
            setattr(self,k,v)
        self.record = record
    @staticmethod
    def dict_to_object(data:dict,ctx):
        return Case(record=data)

    def __str__(self):
      return f"{self.record}"

class Region:
    def __init__(self,record:dict):
        for k,v in record.items():
            setattr(self,k,v)
        self.record = record
    @staticmethod
    def dict_to_object(data:dict,ctx):
        return Region(record=data)
            
    def __str__(self):
      return f"{self.record}"

class TimeProvince:
    def __init__(self,record:dict):
        for k,v in record.items():
            setattr(self,k,v)
        self.record = record
    @staticmethod
    def dict_to_object(data:dict,ctx):
        return TimeProvince(record=data)
            
    def __str__(self):
      return f"{self.record}"

def to_dict(case:Case):
    return case.record

# Coonection string for mongoDB-atlas cluster. This uri will be passed to MongoClient to create a client connection.
uri = "mongodb+srv://admin:admin@covid-analysis.vvghdwk.mongodb.net/?retryWrites=true&w=majority"
client = MongoClient(uri, server_api=ServerApi('1'))

def consume_messages_from_kafka(topic,subject):

    schema_registry_conf = schema_config()
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    # subjects = schema_registry_client.get_subjects()
    # print(subjects)
    subject = subject
    if topic == 'case':
        Class = 'Case'
    elif topic == 'region':
        Class = 'Region'
    elif topic == 'timeProvince':
        Class = 'TimeProvince'
    else:
        pass
    
    Class = globals()[Class]
    schema = schema_registry_client.get_latest_version(subject)
    schema_str=schema.schema.schema_str

    json_deserializer = JSONDeserializer(schema_str,
                                         from_dict= Class.dict_to_object )

    consumer_conf = sasl_conf()
    consumer_conf.update({
                     'group.id': 'group1',
                     'auto.offset.reset': "earliest"})

    consumer = Consumer(consumer_conf)
    consumer.subscribe([topic])

    
    while True:
        try:
            # SIGINT can't be handled when polling, limit timeout to 1 second.
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            record = json_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))

            if record is not None:
                print("User record {}: record: {}\n"
                      .format(msg.key(), record))
                
                record = to_dict(record)
                # print(record)
                db_name = "covid_analysis"
                coll_name = topic
                database = client[db_name]
                collection = database[coll_name]
                collection.insert_one(record)
                print("Record inserted into mongodb: ", record)
                
        except KeyboardInterrupt:
            break

    consumer.close()

if __name__=="__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--topic', type=str)
    parser.add_argument('--subject', type=str)
    args = parser.parse_args()
    consume_messages_from_kafka(topic = args.topic, subject = args.subject)