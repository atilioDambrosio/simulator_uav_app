import json
import time
from time import sleep

from kafka import KafkaConsumer, KafkaProducer, KafkaAdminClient, TopicPartition
from kafka.admin import NewTopic
from libs.files import readJson
import datetime
file_cfg = readJson(file_name='/home/data.cfg')
host_kafka = file_cfg["host_kafka"]

def json_serializer(data):
    return json.dumps(data).encode('utf-8')


def get_producer():
    return KafkaProducer(bootstrap_servers=[host_kafka], value_serializer=json_serializer)


def get_consumer(topic=None):
    consumer = KafkaConsumer(bootstrap_servers=host_kafka, value_deserializer=lambda m: json.loads(m.decode('ascii')),
                             auto_offset_reset="latest")
    consumer.subscribe(topic)
    init_consumer(consumer)
    return consumer


def get_admin_client():
    return KafkaAdminClient(bootstrap_servers=host_kafka)


def create_topics(new_topics, partitions=1, replication_factor=1):
    admin_k = get_admin_client()
    list_new_topics = []
    for topic in new_topics:
        list_new_topics.append(NewTopic(name=topic, num_partitions=partitions, replication_factor=replication_factor))
    admin_k.create_topics(list_new_topics)

def admin_info():
    admin = get_admin_client()

def read_msg(consumer):
    list_msg = []
    messages = consumer.poll(timeout_ms=500)
    for key in messages:
        for msg in messages[key]:
            list_msg.append(msg)
    return list_msg

def init_consumer(consumer):
    list_msg = []
    messages = consumer.poll(timeout_ms=500)
    for key in messages:
        for msg in messages[key]:
            list_msg.append(msg)
    return list_msg

def send_msg(producer, topic, answer, partition):
    producer.send(topic, answer, partition=partition)
    producer.flush()


def get_parameters_msg(msg):
    msg_dict = json.loads(msg.value)
    subtype, directive = msg_dict["command"].split(':')
    request_type = msg_dict["request_type"]
    topic_out = msg_dict["topic_out"]
    return subtype, directive, request_type, topic_out


def get_registered_user(command, type, topic_out):
    return {
        "command": command,
        "request_type": type,
        "topic_out": topic_out,
        "Time": str(datetime.datetime.now()),
    }

