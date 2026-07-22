import logging
from kafka_consumer import KafkaConsumer
from config import Config
import json
import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def process_order_message(message_data):
    """Simulates processing an order message. Replace with actual business logic."""
    logger.info("Processing order: %s", message_data.get('order_id'))
    
    # Example: Add a processed timestamp and status
    processed_data = message_data.copy()
    processed_data['status'] = 'processed'
    processed_data['processed_timestamp'] = datetime.datetime.now().isoformat() + 'Z'
    processed_data['processed_price'] = message_data.get('price', 0.0)

    # Simulate a potential processing error for retry testing
    # if message_data.get('item') == 'FaultyItem':
    #     raise ValueError("Simulated processing error for FaultyItem")

    return processed_data

if __name__ == "__main__":
    logger.info("Starting Kafka Processor Application...")
    logger.info("Kafka Input Topic: %s", Config.KAFKA_INPUT_TOPIC)
    logger.info("Kafka Output Topic: %s", Config.KAFKA_OUTPUT_TOPIC)
    logger.info("Kafka DLQ Topic: %s", Config.KAFKA_DLQ_TOPIC)

    consumer_app = KafkaConsumer(process_order_message)
    try:
        consumer_app.subscribe_and_consume()
    except Exception as e:
        logger.critical("Application terminated due to an unhandled error: %s", e, exc_info=True)
    finally:
        logger.info("Kafka Processor Application stopped.")
