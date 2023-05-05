#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2020 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


# A simple example demonstrating use of JSONSerializer.

import argparse
from uuid import uuid4
#from six.moves import input
from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
#from confluent_kafka.schema_registry import *
import pandas as pd
from typing import List

CASE_PATH = "/config/workspace/Case.csv"
case_columns=['case_id', 'province', 'city', 'group', 'infection_case', 'confirmed',
       'latitude', 'longitude']

REGION_PATH = "/config/workspace/Region.csv"
region_columns=['code','province','city','latitude','longitude','elementary_school_count','kindergarten_count','university_count',
'academy_ratio','elderly_population_ratio','elderly_alone_ratio','nursing_home_count']

TIMEPROVINCE_PATH = "/config/workspace/TimeProvince.csv"
time_province_columns = ['date','time','province','confirmed','released','deceased']

API_KEY = 'P3OUJW4OCRV4NXPQ'
ENDPOINT_SCHEMA_URL  = <>
API_SECRET_KEY = <>
BOOTSTRAP_SERVER = '<>
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

    def __str__(self):
      return f"{self.record}"

class Region:
    def __init__(self,record:dict):
        for k,v in record.items():
            setattr(self,k,v)
        self.record = record
            
    def __str__(self):
      return f"{self.record}"

class TimeProvince:
    def __init__(self,record:dict):
        for k,v in record.items():
            setattr(self,k,v)
        self.record = record
            
    def __str__(self):
      return f"{self.record}"


def case_object(file_path):
    df = pd.read_csv(file_path)
    df = df.iloc[:,0:]
    for data in df.values:
        value = Case(dict(zip(case_columns, data)))
        yield value

def region_object(file_path):
    df = pd.read_csv(file_path)
    df = df.iloc[:,0:]
    for data in df.values:
        value = Region(dict(zip(region_columns, data)))
        yield value

def timeProvince_object(file_path):
    df = pd.read_csv(file_path)
    df = df.iloc[:,0:]
    for data in df.values:
        value = TimeProvince(dict(zip(time_province_columns, data)))
        yield value

def case_to_dict(case:Case, ctx):
    """
    Returns a dict representation of a User instance for serialization.
    Args:
        user (User): User instance.
        ctx (SerializationContext): Metadata pertaining to the serialization
            operation.
    Returns:
        dict: Dict populated with user attributes to be serialized.
    """

    # User._address must not be serialized; omit from dict
    return case.record

def region_to_dict(re:Region, ctx):

    return re.record
def timeProvince_to_dict(tProvince:TimeProvince, ctx):

    return tProvince.record

def delivery_report(err, msg):
    """
    Reports the success or failure of a message delivery.
    Args:
        err (KafkaError): The error that occurred on None on success.
        msg (Message): The message that was produced or failed.
    """

    if err is not None:
        print("Delivery failed for User record {}: {}".format(msg.key(), err))
        return
    print('User record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))

def send_messages_to_kafka(topic,subject,file_name):
    schema_registry_conf = schema_config()
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    subject = subject
    # We are using topic as a placeholder to programatically call functions. There is no use of topic as such here.
    iter_func = f"{topic}_object"
    iter_func = globals()[iter_func] #globals() keeps the details of all variables, dictionaries and functions declared globally. We are using it programatically to call a global function.
    to_dict = f"{topic}_to_dict"
    to_dict_func = globals()[to_dict]
    schema = schema_registry_client.get_latest_version(subject)
    schema_str=schema.schema.schema_str

    string_serializer = StringSerializer('utf_8')
    json_serializer = JSONSerializer(schema_str, schema_registry_client, to_dict_func)

    producer = Producer(sasl_conf())

    print("Producing user records to topic {}. ^C to exit.".format(topic))
    try:
        for value in iter_func(file_path=f'/config/workspace/{file_name}'):
            print(value)
            producer.produce(topic=topic,
                            key=string_serializer(str(uuid4())),
                            value=json_serializer(value, SerializationContext(topic, MessageField.VALUE)),
                            on_delivery=delivery_report)
    except KeyboardInterrupt:
        pass
    except ValueError:
        print("Invalid input, discarding record...")
        pass

    print("\nFlushing records...")
    producer.flush()

if __name__=="__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--topic', type=str)
    parser.add_argument('--subject', type=str)
    parser.add_argument('--file_name', type=str)
    args = parser.parse_args()
    send_messages_to_kafka(topic = args.topic, subject = args.subject, file_name = args.file_name)
    