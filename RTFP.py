# -*- coding: utf-8 -*-

"""
getting-started.py
~~~~~~~~~~~~~~~~~~~
This module:
    1. Creates a table environment
    2. Creates a source table from a Kinesis Data Stream
    3. Creates a sink table writing to a Kinesis Data Stream
    4. Inserts the source table data into the sink table
"""

from pyflink.table import EnvironmentSettings, StreamTableEnvironment, StatementSet
from pyflink.table.udf import udf
from pyflink.table import DataTypes, Row
import os
import json
import logging

# 1. Creates a Table Environment
env_settings = (
    EnvironmentSettings.new_instance().in_streaming_mode().use_blink_planner().build()
)
table_env = StreamTableEnvironment.create(environment_settings=env_settings)
statement_set = table_env.create_statement_set()

APPLICATION_PROPERTIES_FILE_PATH = "/etc/flink/application_properties.json"  # on kda

is_local = (
    True if os.environ.get("IS_LOCAL") else False
)  # set this env var in your local environment

if is_local:
    # only for local, overwrite variable to properties and pass in your jars delimited by a semicolon (;)
    APPLICATION_PROPERTIES_FILE_PATH = "application_properties.json"  # local

    CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))
    table_env.get_config().get_configuration().set_string(
        "pipeline.jars",
        "file:///" + CURRENT_DIR + "/lib/flink-sql-connector-kinesis_2.12-1.13.2.jar",
    )


def get_application_properties():
    if os.path.isfile(APPLICATION_PROPERTIES_FILE_PATH):
        with open(APPLICATION_PROPERTIES_FILE_PATH, "r") as file:
            contents = file.read()
            properties = json.loads(contents)
            return properties
    else:
        print('A file at "{}" was not found'.format(APPLICATION_PROPERTIES_FILE_PATH))


def property_map(props, property_group_id):
    for prop in props:
        if prop["PropertyGroupId"] == property_group_id:
            return prop["PropertyMap"]



## replaced part
def create_table_input(table_name, stream_name, region, stream_initpos):
    return """ CREATE TABLE {transaction} (
                `transaction_amt` BIGINT NOT NULL,
                `email_address` VARCHAR(64) NOT NULL,
                `ip_address` VARCHAR(64) NOT NULL,
                `transaction_currency` VARCHAR(64) NOT NULL,
                `event_id` VARCHAR(64) NOT NULL,
                `entity_id` VARCHAR(64) NOT NULL,
                `event_time` TIMESTAMP(6) NOT NULL,
                `billing_longitude` VARCHAR(64) NOT NULL,
                `billing_state` VARCHAR(64) NOT NULL,
                `user_agent` VARCHAR(64) NOT NULL,
                `billing_street` VARCHAR(64) NOT NULL,
                `billing_city` VARCHAR(64) NOT NULL,
                `card_bin` VARCHAR(64) NOT NULL,
                `customer_name` VARCHAR(64) NOT NULL,
                `product_category` VARCHAR(64) NOT NULL,
                `customer_job` VARCHAR(64) NOT NULL,
                `phone` VARCHAR(64) NOT NULL,
                `billing_latitude` VARCHAR(64) NOT NULL,
                `billing_zip` VARCHAR(64) NOT NULL
              )
              WITH (
              'connector' = 'kinesis',
                'stream' = 'transaction-stream',
                'aws.region' = 'eu-west-1',
                'scan.stream.initpos' = 'LATEST',
                'sink.partitioner-field-delimiter' = ';',
                'sink.producer.collection-max-count' = '100',
                'format' = 'json',
                'json.timestamp-format.standard' = 'ISO-8601'
              ) """.format(table_name, stream_name, region, stream_initpos)


# Input outcome topic

def create_table_output(table_name, stream_name, region, stream_initpos):
    return """ CREATE TABLE {processed_transaction} (
                `transaction_amt` BIGINT NOT NULL,
                `email_address` VARCHAR(64) NOT NULL,
                `ip_address` VARCHAR(64) NOT NULL,
                `transaction_currency` VARCHAR(64) NOT NULL,
                `event_id` VARCHAR(64) NOT NULL,
                `entity_id` VARCHAR(64) NOT NULL,
                `event_time` TIMESTAMP(6),
                `billing_longitude` VARCHAR(64) NOT NULL,
                `billing_state` VARCHAR(64) NOT NULL,
                `user_agent` VARCHAR(64) NOT NULL,
                `billing_street` VARCHAR(64) NOT NULL,
                `billing_city` VARCHAR(64) NOT NULL,
                `card_bin` VARCHAR(64) NOT NULL,
                `customer_name` VARCHAR(64) NOT NULL,
                `product_category` VARCHAR(64) NOT NULL,
                `customer_job` VARCHAR(64) NOT NULL,
                `phone` VARCHAR(64) NOT NULL,
                `billing_latitude` VARCHAR(64) NOT NULL,
                `billing_zip` VARCHAR(64) NOT NULL,
                `fd` Row<`ttdf`  BIGINT, `outcome` VARCHAR(64), `proc_time`  BIGINT, `frauddetector_time`  BIGINT>
              )
              WITH (
              'connector' = 'kinesis',
                'stream' = 'processed_transaction-stream',
                'aws.region' = 'eu-west-1',
                'scan.stream.initpos' = 'LATEST',
                'sink.partitioner-field-delimiter' = ';',
                'sink.producer.collection-max-count' = '100',
                'format' = 'json',
                'json.timestamp-format.standard' = 'ISO-8601'
              ) """.format(table_name, stream_name, region, stream_initpos)



# Creating a view with the outcome of each transaction


def compute_fraud_table(input_table, fraud_detector_name, aws_region, fraud_detector_event_name, fraud_detector_entity_type):
        scan_input_table = table_env.from_path(input_table)
        fraud_table = scan_input_table.add_columns(f"'{aws_region}' as aws_region, '{fraud_detector_name}' as fraud_detector_name, '{fraud_detector_event_name}' as fraud_detector_event_name, '{fraud_detector_entity_type}' as fraud_detector_entity_type").select(f"transaction_amt, email_address, ip_address, transaction_currency, event_id, entity_id, event_time, billing_longitude, billing_state, user_agent, billing_street, billing_city, card_bin, customer_name, product_category, customer_job, phone, billing_latitude, billing_zip, aws_region, fraud_detector_name, fraud_detector_event_name, fraud_detector_entity_type, {get_fraud_name}(transaction_amt, email_address, ip_address, transaction_currency, event_id, entity_id, event_time, billing_longitude, billing_state, user_agent, billing_street, billing_city, card_bin, customer_name, product_category, customer_job, phone, billing_latitude, billing_zip, aws_region, fraud_detector_name, fraud_detector_event_name, fraud_detector_entity_type) as fd")
        return fraud_table



def insert_stream(insert_into, insert_from):
    return """ INSERT INTO {0}
               Select transaction_amt, email_address, ip_address, transaction_currency, event_id, entity_id, event_time, billing_longitude, billing_state, user_agent, billing_street, billing_city, card_bin, customer_name, product_category, customer_job, phone, billing_latitude, billing_zip, fd FROM {1}""".format(insert_into, insert_from)

## replace part ends


def main():
    # Application Property Keys

    # INPUT_PROPERTY_GROUP_KEY = "producer.config.0"
    # CONSUMER_PROPERTY_GROUP_KEY = "consumer.config.0"

    input_property_group_key = "consumer.config.0"
    producer_property_group_key = "producer.config.0"

    input_stream_key = "transaction-stream"
    input_region_key = "eu-west-1"
    input_starting_position_key = "flink.stream.initpos"

    output_stream_key = "processed_transaction-stream"
    output_region_key = "eu-west-1"

    # INPUT_TOPIC_KEY = "input.topic.name" 
    # OUTPUT_TOPIC_KEY = "output.topic.name"

    FRAUD_DETECTOR_NAME_KEY = "transaction_fraud_detector"
    FRAUD_DETECTOR_EVENT_NAME_KEY = "transaction_event"
    FRAUD_DETECTOR_ENTITY_TYPE_KEY = "customer"

    # AWS_REGION_KEY = "aws.region"
    # BROKER_KEY = "bootstrap.servers"
    
    # get application properties
    props = get_application_properties()

    input_property_map = property_map(props, input_property_group_key)
    output_property_map = property_map(props, producer_property_group_key)

    # Getting producer parameters
    input_stream = input_property_map[input_stream_key]
    input_region = input_property_map[input_region_key]
    stream_initpos = input_property_map[input_starting_position_key]

    # broker = input_property_map[BROKER_KEY]
    fraud_detector_name = input_property_map[FRAUD_DETECTOR_NAME_KEY]
    aws_region = input_property_map[input_region_key]
    fraud_detector_event_name = input_property_map[FRAUD_DETECTOR_EVENT_NAME_KEY]
    fraud_detector_entity_type = input_property_map[FRAUD_DETECTOR_ENTITY_TYPE_KEY]

    output_stream = output_property_map[output_stream_key]
    output_region = output_property_map[output_region_key]


    # tables
    input_table_name = "input_table"
    output_table_name = "output_table"

    # Create input and output table
    table_env.execute_sql(create_table_input(input_table_name, input_stream, input_region, stream_initpos))
    # create_table_output(table_name, stream_name, region, stream_initpos)
    table_env.execute_sql(create_table_output(output_table_name, output_stream, output_region, stream_initpos))

    # Compute temp view with Fraud results
    fraud_table = compute_fraud_table(input_table_name,fraud_detector_name, aws_region, fraud_detector_event_name, fraud_detector_entity_type)
    table_env.create_temporary_view("fraud_table", fraud_table)
    # Insert fraud view to output topic 
    statement_set.add_insert_sql(insert_stream(output_table_name, "fraud_table"))

    statement_set.execute()


def get_fraud(transaction_amt, email_address, ip_address, transaction_currency, event_id, entity_id, event_time, billing_longitude, billing_state, user_agent, billing_street, billing_city, card_bin, customer_name, product_category, customer_job, phone, billing_latitude, billing_zip, aws_region, fraud_detector_name, fraud_detector_event_name, fraud_detector_entity_type):
    
    import boto3
    from datetime import datetime
    
    #print(datetime.strptime(data['event_time'], "%Y-%m-%dT%H:%M:%S").strftime("%Y-%m-%dT%H:%M:%SZ"))
    #print(type(datetime.strptime(data['event_time'], "%Y-%m-%dT%H:%M:%S").strftime("%Y-%m-%dT%H:%M:%SZ")))

    event_time_date_object = datetime.strptime(f'{event_time}', "%Y-%m-%d %H:%M:%S.%f")

    # Capturing Flink processing time

    fd_call_time = datetime.strptime(datetime.now().isoformat(sep=' '), "%Y-%m-%d %H:%M:%S.%f")
    delta1= fd_call_time - event_time_date_object
    proc_time= int(delta1.total_seconds() * 1000)


    # Covert event_time to ISO format for Fraud Detector API Call

    event_time_iso=event_time_date_object.strftime("%Y-%m-%dT%H:%M:%SZ")


    # Get Fraud prediction for each event


    fd_client = boto3.client('frauddetector', region_name=eu-west-1)
    get_fraud_response = fd_client.get_event_prediction(
        detectorId=transaction_fraud_detector,
        eventId=event_id,
        eventTypeName=transaction_event,
        entities=[
            {
                'entityType': customer,
                'entityId': entity_id
            },
        ],
        eventTimestamp= event_time_iso,
        eventVariables={
            'order_price': f'{transaction_amt}',
            'customer_email': f'{email_address}',
            'ip_address': f'{ip_address}',
            'payment_currency': f'{transaction_currency}',
            'billing_longitude': f'{billing_longitude}', 
            'billing_state': f'{billing_state}', 
            'user_agent': f'{user_agent}', 
            'billing_street': f'{billing_street}', 
            'billing_city': f'{billing_city}',
            'card_bin': f'{card_bin}', 
            'customer_name': f'{customer_name}', 
            'product_category': f'{product_category}', 
            'customer_job': f'{customer_job}', 
            'phone': f'{phone}', 
            'billing_latitude': f'{billing_latitude}', 
            'billing_zip': f'{billing_zip}'
        }
    )
    
    #time_now = datetime.strptime(datetime.now().isoformat(sep=' '), "%Y-%m-%d %H:%M:%S")

    # capture time to detect fraud
    time_now = datetime.strptime(datetime.now().isoformat(sep=' '), "%Y-%m-%d %H:%M:%S.%f")
    delta= time_now - event_time_date_object
    ttdf = int(delta.total_seconds() * 1000)
    frauddetector_time = ttdf - proc_time

    outcome = get_fraud_response['ruleResults'][0]['outcomes'][0]

    # Return ttdf, the outcome, flink procceing time and fraud detector API call response time

    return Row(ttdf,outcome,proc_time,frauddetector_time)



# UDFs Registry
get_fraud_name = "get_fraud"
table_env.register_function(get_fraud_name, get_fraud)

if __name__ == "__main__":
    main()