import os
import json
import logging
from typing import Dict, List, Any, Optional

from airflow.sensors.base import BaseSensorOperator
from airflow.utils.context import Context
from kafka import KafkaConsumer, TopicPartition
from kafka.errors import KafkaError
from dotenv import load_dotenv
load_dotenv()
logger = logging.getLogger(__name__)


class KafkaMessageSensor(BaseSensorOperator):

    def __init__(self, kafka_topic: str, **kwargs):
        super().__init__(**kwargs)
        self.kafka_topic = kafka_topic
        self.kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')

    def poke(self, context: Context) -> bool:
        try:
            logger.info(f"Checking for messages in topic: {self.kafka_topic}")

            consumer = KafkaConsumer(
                bootstrap_servers=self.kafka_servers.split(','),
                value_deserializer=lambda x: json.loads(x.decode('utf-8')) if x else None,
                consumer_timeout_ms=10000,
                auto_offset_reset='latest',
                enable_auto_commit=False,
                group_id=f'cdc_sensor_{context["ts"]}'
            )
            partitions = consumer.partitions_for_topic(self.kafka_topic)
            if not partitions:
                logger.warning(f"No partitions found for topic {self.kafka_topic}")
                consumer.close()
                return False

            topic_partitions = [TopicPartition(self.kafka_topic, p) for p in partitions]
            consumer.assign(topic_partitions)

            end_offsets = consumer.end_offsets(topic_partitions)

            has_messages = False
            total_new_messages = 0

            for tp in topic_partitions:
                try:
                    position = consumer.position(tp)
                    end_offset = end_offsets[tp]
                    new_messages = end_offset - position

                    if new_messages > 0:
                        has_messages = True
                        total_new_messages += new_messages
                        logger.info(f"Found {new_messages} new messages in partition {tp.partition}")
                except Exception as e:
                    logger.warning(f"Error checking partition {tp}: {str(e)}")
                    continue

            consumer.close()

            if has_messages:
                logger.info(f"Total {total_new_messages} new messages found in topic {self.kafka_topic}")
            else:
                logger.debug(f"No new messages in topic {self.kafka_topic}")

            return has_messages

        except KafkaError as e:
            logger.error(f"Kafka error while checking messages: {str(e)}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error checking Kafka messages: {str(e)}")
            return False


class KafkaHealthChecker:
    def __init__(self):
        self.kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')

    def check_topic_exists(self, topic_name: str) -> bool:
        try:
            consumer = KafkaConsumer(
                bootstrap_servers=self.kafka_servers.split(','),
                consumer_timeout_ms=5000
            )

            topics = consumer.topics()
            logger.info(f"Available topics: {topics}")
            exists = topic_name in topics

            consumer.close()

            logger.info(f"Topic {topic_name} {'exists' if exists else 'does not exist'}")
            return exists

        except Exception as e:
            logger.error(f"Error checking topic existence: {str(e)}")
            return False

    def get_topic_info(self, topic_name: str) -> Dict[str, Any]:
        try:
            consumer = KafkaConsumer(
                bootstrap_servers=self.kafka_servers.split(','),
                consumer_timeout_ms=5000
            )

            partitions = consumer.partitions_for_topic(topic_name)
            if not partitions:
                return {'error': f'Topic {topic_name} not found'}

            topic_partitions = [TopicPartition(topic_name, p) for p in partitions]
            consumer.assign(topic_partitions)

            beginning_offsets = consumer.beginning_offsets(topic_partitions)
            end_offsets = consumer.end_offsets(topic_partitions)

            partition_info = []
            total_messages = 0

            for tp in topic_partitions:
                messages_in_partition = end_offsets[tp] - beginning_offsets[tp]
                total_messages += messages_in_partition

                partition_info.append({
                    'partition': tp.partition,
                    'beginning_offset': beginning_offsets[tp],
                    'end_offset': end_offsets[tp],
                    'messages': messages_in_partition
                })

            consumer.close()

            return {
                'topic': topic_name,
                'partitions': len(partitions),
                'total_messages': total_messages,
                'partition_details': partition_info
            }

        except Exception as e:
            logger.error(f"Error getting topic info: {str(e)}")
            return {'error': str(e)}
