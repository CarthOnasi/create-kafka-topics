import re

from exceptions import InvalidConfigurationException
from topic import Topic


def validate_topic_not_empty(topic):
    if topic is None or topic == "":
        error_message = "Kafka topic may not be empty. Value = {value}".format(value=topic)
        raise InvalidConfigurationException(error_message)


def validate_topic_name(topic):
    topic_name_match = re.match(r"^[\w\\._0-9]*$", topic)
    if topic_name_match is None:
        error_message = "kafka Topic does not match naming rules ([a-zA-Z0-9\\._]). Value = {value}".format(value=topic)
        raise InvalidConfigurationException(error_message)


def parse_topic(topic):
    validate_topic_not_empty(topic)

    regex_match = re.match(r"^(?P<topic>[\w\\._0-9]*):(?P<partition_count>\d{,2}):(?P<replication_factor>\d{,2})$",
                           topic)

    if regex_match is not None:
        topic_name = regex_match.group("topic")
        partition_count = int(regex_match.group("partition_count"))
        replication_factor = int(regex_match.group("replication_factor"))
        return Topic(topic_name, partition_count, replication_factor)
    else:
        validate_topic_name(topic)
        return Topic(topic, 1, 1)


def parse_topics(topic_definitions):
    split_topic_definitions = str(topic_definitions).split(";")
    topics = []
    for topic in split_topic_definitions:
        parsed_topic = parse_topic(topic)
        topics.append(parsed_topic)
    return topics
