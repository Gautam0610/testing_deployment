import logging
import json
import time
from confluent_kafka import Consumer, KafkaException, OFFSET_BEGINNING
from config import Config
from kafka_producer import KafkaProducer

logger = logging.getLogger(__name__)

class KafkaConsumer:
    def __init__(self, process_message_func):
        common_config = Config.get_kafka_common_config()
        consumer_config = {
            'group.id': Config.KAFKA_CONSUMER_GROUP_ID,
            'auto.offset.reset': Config.KAFKA_CONSUMER_AUTO_OFFSET_RESET,
            'enable.auto.commit': Config.KAFKA_CONSUMER_ENABLE_AUTO_COMMIT,
            'auto.commit.interval.ms': Config.KAFKA_CONSUMER_AUTO_COMMIT_INTERVAL_MS,
            **common_config
        }
        self.consumer = Consumer(consumer_config)
        self.process_message_func = process_message_func
        self.producer = KafkaProducer()
        logger.info("Kafka Consumer initialized with config: %s", consumer_config)

    def subscribe_and_consume(self):
        topics = [Config.KAFKA_INPUT_TOPIC]
        self.consumer.subscribe(topics, on_assign=self.on_assign)
        logger.info("Subscribed to topics: %s", topics)

        try:
            while True:
                msg = self.consumer.poll(1.0) # Poll for messages with a timeout

                if msg is None:
                    # logger.debug("No message received within timeout.")
                    continue
                if msg.error():
                    if msg.error().is_fatal():
                        logger.error("Fatal Kafka consumer error: %s", msg.error())
                        raise KafkaException(msg.error())
                    else:
                        logger.warning("Non-fatal Kafka consumer error: %s", msg.error())
                        continue

                self._handle_message(msg)

        except KeyboardInterrupt:
            logger.info("Consumer interrupted by user.")
        except Exception as e:
            logger.critical("An unhandled exception occurred in the consumer loop: %s", e, exc_info=True)
        finally:
            self.close()

    def _handle_message(self, msg):
        message_value = msg.value().decode('utf-8')
        message_key = msg.key().decode('utf-8') if msg.key() else 'N/A'
        logger.info("Received message: topic=%s, partition=%d, offset=%d, key=%s",
                    msg.topic(), msg.partition(), msg.offset(), message_key)

        try:
            parsed_message = json.loads(message_value)
        except json.JSONDecodeError as e:
            logger.error("Failed to parse JSON message from topic %s: %s", msg.topic(), e)
            self._send_to_dlq(msg, {"error": "JSON parsing error", "details": str(e)})
            return

        retries = 0
        while retries < Config.KAFKA_RETRY_MAX_ATTEMPTS:
            try:
                processed_data = self.process_message_func(parsed_message)
                if processed_data:
                    self.producer.publish_message(
                        Config.KAFKA_OUTPUT_TOPIC,
                        message_key,
                        processed_data
                    )
                break # Success, exit retry loop
            except Exception as e:
                retries += 1
                wait_time = Config.KAFKA_RETRY_INITIAL_INTERVAL_SEC * (2 ** (retries - 1))
                logger.warning("Error processing message (attempt %d/%d): %s. Retrying in %d seconds.",
                               retries, Config.KAFKA_RETRY_MAX_ATTEMPTS, e, wait_time)
                time.sleep(wait_time)
                if retries >= Config.KAFKA_RETRY_MAX_ATTEMPTS:
                    logger.error("Max retries reached for message: %s", message_key)
                    self._send_to_dlq(msg, {"error": "Processing failed after retries", "details": str(e)})

    def _send_to_dlq(self, original_msg, error_details):
        dlq_message = {
            "original_message_topic": original_msg.topic(),
            "original_message_partition": original_msg.partition(),
            "original_message_offset": original_msg.offset(),
            "original_message_key": original_msg.key().decode('utf-8') if original_msg.key() else None,
            "original_message_value": original_msg.value().decode('utf-8'),
            "error_details": error_details,
            "timestamp": time.time()
        }
        try:
            self.producer.publish_message(
                Config.KAFKA_DLQ_TOPIC,
                original_msg.key(),
                dlq_message
            )
            logger.info("Message sent to DLQ: %s", original_msg.key())
        except Exception as e:
            logger.error("Failed to send message to DLQ topic %s: %s", Config.KAFKA_DLQ_TOPIC, e)

    def on_assign(self, consumer, partitions):
        logger.info("Partition assignment: %s", partitions)
        # Optionally, you can set consumer offset here if needed
        # for p in partitions:
        # p.offset = OFFSET_BEGINNING
        # consumer.assign([p])

    def close(self):
        if self.consumer:
            self.consumer.close()
            logger.info("Kafka Consumer closed.")
        self.producer.close()
