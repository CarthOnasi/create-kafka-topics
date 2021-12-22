import concurrent.futures
import os
import time

from confluent_kafka.admin import (AdminClient, NewTopic)

from parse_topics import parse_topics

kafka_waiting_time_variable_name = "KAFKA_WAITING_TIME"
environment_variable_name = "TOPICS_TO_CREATE"
bootstrap_server_variable_name = "BOOTSTRAP_SERVER"


def create_kafka_topics():
    # wait until kafka is ready
    waiting_time = int(os.environ[kafka_waiting_time_variable_name])
    time.sleep(waiting_time)

    topic_definition = os.environ[environment_variable_name]
    bootstrap_server_definition = os.environ[bootstrap_server_variable_name]

    topics = parse_topics(topic_definition)

    print("Connection to kafka server {server_url}".format(server_url=bootstrap_server_definition))

    kafka_client = AdminClient({'bootstrap.servers': bootstrap_server_definition})

    existing_topics = kafka_client.list_topics().topics

    print("Found {topics_count} topics".format(topics_count=len(existing_topics)))

    new_topics = []

    for topic in topics:
        if topic.name in existing_topics:
            print("Topic with name {topic_name} does already exist. Skipping".format(topic_name=topic.name))
            continue

        print("Topic {topic_name} does not exist. Will be created".format(topic_name=topic.name))
        new_topic = NewTopic(topic.name, topic.partition_number, topic.replication_factor)
        new_topics.append(new_topic)

    if len(new_topics) > 0:
        print("Creating topics")
        result = kafka_client.create_topics(new_topics)
        task_values = result.values()
        concurrent.futures.wait(task_values)

    print("Finished creating topics.")
