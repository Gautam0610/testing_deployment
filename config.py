import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    KAFKA_SECURITY_PROTOCOL = os.getenv('KAFKA_SECURITY_PROTOCOL', 'SASL_PLAINTEXT')
    KAFKA_SASL_MECHANISM = os.getenv('KAFKA_SASL_MECHANISM', 'PLAIN')
    KAFKA_SASL_USERNAME = os.getenv('KAFKA_SASL_USERNAME')
    KAFKA_SASL_PASSWORD = os.getenv('KAFKA_SASL_PASSWORD')
    KAFKA_INPUT_TOPIC = os.getenv('KAFKA_INPUT_TOPIC', 'input-orders')
    KAFKA_OUTPUT_TOPIC = os.getenv('KAFKA_OUTPUT_TOPIC', 'processed-orders')
    KAFKA_DLQ_TOPIC = os.getenv('KAFKA_DLQ_TOPIC', 'order-dlq')
    KAFKA_CONSUMER_GROUP_ID = os.getenv('KAFKA_CONSUMER_GROUP_ID', 'order-processor-group')

    KAFKA_RETRY_MAX_ATTEMPTS = int(os.getenv('KAFKA_RETRY_MAX_ATTEMPTS', 5))
    KAFKA_RETRY_INITIAL_INTERVAL_SEC = int(os.getenv('KAFKA_RETRY_INITIAL_INTERVAL_SEC', 1))

    KAFKA_CONSUMER_AUTO_OFFSET_RESET = os.getenv('KAFKA_CONSUMER_AUTO_OFFSET_RESET', 'earliest')
    KAFKA_CONSUMER_ENABLE_AUTO_COMMIT = os.getenv('KAFKA_CONSUMER_ENABLE_AUTO_COMMIT', 'True').lower() == 'true'
    KAFKA_CONSUMER_AUTO_COMMIT_INTERVAL_MS = int(os.getenv('KAFKA_CONSUMER_AUTO_COMMIT_INTERVAL_MS', 5000))

    @classmethod
    def get_kafka_common_config(cls):
        conf = {
            'bootstrap.servers': cls.KAFKA_BOOTSTRAP_SERVERS,
            'security.protocol': cls.KAFKA_SECURITY_PROTOCOL
        }
        if cls.KAFKA_SECURITY_PROTOCOL.startswith('SASL'):
            conf.update({
                'sasl.mechanism': cls.KAFKA_SASL_MECHANISM,
                'sasl.username': cls.KAFKA_SASL_USERNAME,
                'sasl.password': cls.KAFKA_SASL_PASSWORD
            })
        return conf
