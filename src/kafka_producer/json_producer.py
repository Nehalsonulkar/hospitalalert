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
from src.kafka_config import sasl_conf, schema_config
from six.moves import input
from src.kafka_logger import logging
from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
import pandas as pd
from typing import List
from src.entity.generic import Generic, instance_to_dict

FILE_PATH = "/home/avnish/iNeuron_Private_Intelligence_Limited/industry_ready_project/projects/data_pipeline/kafka-sensor/sample_data/sensor/aps_failure_training_set1.csv"


def car_to_dict(car: Generic, ctx):
    """
    Returns a dict representation of a User instance for serialization.
    Args:
        user (User): User instance.
        ctx (SerializationContext): Metadata pertaining to the serialization
            operation.
    Returns:
        dict: Dict populated with user attributes to be serialized.
        :param car:
    """

    # User._address must not be serialized; omit from dict
    return car.record

## Purpose: This function reports whether a Kafka message was successfully delivered or if there was an error.
## err: An error object (KafkaError) that indicates if there was a failure during delivery. It is None if the delivery was successful.
## msg: The Kafka message (Message) that was produced or failed.
def delivery_report(err, msg):
    """
    Reports the success or failure of a message delivery.
    Args:
        err (KafkaError): The error that occurred on None on success.
        msg (Message): The message that was produced or failed.
    """

    if err is not None:
        logging.info("Delivery failed for User record {}: {}".format(msg.key(), err))
        return
    ## Purpose: Logs a message indicating that the delivery was successful.
    logging.info('User record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))


def product_data_using_file(topic,file_path):
    logging.info(f"Topic: {topic} file_path:{file_path}")
    ## checked the `get_schema_to_produce_consume_data` function
    schema_str = Generic.get_schema_to_produce_consume_data(file_path=file_path)
    schema_registry_conf = schema_config()      ## This line retrieves the configuration details required to connect to a schema registry service. It uses the schema_config function, which likely contains endpoint URLs and authentication credentials.
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)  ## this class is from confluent
    string_serializer = StringSerializer('utf_8')    ## This serializer likely converts data into UTF-8 encoded strings suitable for transmission over Kafka.Serializer means converting your data in a certain format so that it can be stored in the disk thats why we do the Serializer eg. pickle
    json_serializer = JSONSerializer(schema_str, schema_registry_client, instance_to_dict)   ## This serializer is tailored for JSON data and uses the schema retrieved earlier. It also uses the instance_to_dict function for data serialization.
    producer = Producer(sasl_conf())    ## A Producer object is initialized for interfacing with Kafka. The sasl_conf() function is used to provide security configuration settings,

    print("Producing user records to topic {}. ^C to exit.".format(topic))
    # while True:
    # Serve on_delivery callbacks from previous calls to produce()
    producer.poll(0.0)      ## This line serves to integrate the producer with Kafka's event loop. By calling poll(0.0), it triggers the delivery report callback for any previously sent messages and handles any events.
    try:
             ## go to the `get_obkect` function in `generic.py` file
        for instance in Generic.get_object(file_path=file_path):   ## This line iterates over each record in the file specified by file_path. The Generic.get_object method is used to read and convert each row into an instance of the Generic class.
            print(instance)
            logging.info(f"Topic: {topic} file_path:{instance.to_dict()}")   ##Each instance (record) is printed and logged for debugging purposes.

            ## The topic to which the record is sent
            ## key=string_serializer(str(uuid4()), instance.to_dict()): A unique key for each record, serialized as a string.
            ## value=json_serializer(instance, SerializationContext(topic, MessageField.VALUE)): The record itself, serialized as JSON.
            ## on_delivery=delivery_report: A callback function that will be called once the message is delivered or fails.
            producer.produce(topic=topic,
                             key=string_serializer(str(uuid4()), instance.to_dict()),
                             value=json_serializer(instance, SerializationContext(topic, MessageField.VALUE)),
                             on_delivery=delivery_report)    ## check the `delivery_report` esi file me upr hai
            print("\nFlushing records...")
            producer.flush()
    except KeyboardInterrupt:
        pass
    except ValueError:
        print("Invalid input, discarding record...")
        pass

    
