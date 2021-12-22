import unittest
from parse_topics import parse_topic, parse_topics
from exceptions import InvalidConfigurationException
from topic import Topic


class MyTestCase(unittest.TestCase):
    def test_parse_topic_no_partition_set(self):
        # arrange
        topic_string = "topic_name.valid_example"
        expected = Topic("topic_name.valid_example", 1, 1)

        # act
        result = parse_topic(topic_string)

        # assert
        self.assertEqual(result.name, expected.name)
        self.assertEqual(result.partition_number, expected.partition_number)
        self.assertEqual(result.replication_factor, expected.replication_factor)

    def test_parse_topic_has_partition_set(self):
        # arrange
        topic_string = "topic_name:3:12"
        expected = Topic("topic_name", 3, 12)

        # act
        result = parse_topic(topic_string)

        # assert
        self.assertEqual(result.name, expected.name)
        self.assertEqual(result.partition_number, expected.partition_number)
        self.assertEqual(result.replication_factor, expected.replication_factor)

    def test_parse_topic_empty_topic_throws_exception(self):
        # arrange
        empty_topic = ""

        # act / assert
        self.assertRaises(InvalidConfigurationException, lambda: parse_topic(empty_topic))

    def test_parse_topic_invalid_topic_throws_exception(self):
        # arrange
        empty_topic = "invalid&%topic"

        # act / assert
        self.assertRaises(InvalidConfigurationException, lambda: parse_topic(empty_topic))

    def test_parse_topics_splits_and_parses(self):
        # arrange
        topics = "topic1;topic2;topic3:1:14"

        # act
        result = parse_topics(topics)

        # assert
        self.assertEqual(len(result), 3)

    def test_parse_topics_splits_single_topic(self):
        # arrange
        topics = "topic1"

        # act
        result = parse_topics(topics)

        # assert
        self.assertEqual(len(result), 1)


if __name__ == '__main__':
    unittest.main()
