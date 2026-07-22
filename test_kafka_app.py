import pytest
import os
import json
import time
from unittest.mock import MagicMock, patch

# Import modules to be tested
from config import Config
from kafka_producer import KafkaProducer
from kafka_consumer import KafkaConsumer
from main import process_order_message # For testing the processing logic

# --- Fixtures for environment variables ---
@pytest.fixture
def set_env_vars():
    # Set minimal required environment variables for testing
    os.environ['KAFKA_BOOTSTRAP_SERVERS'] = 'test_bootstrap:9092'
    os.environ['KAFKA_INPUT_TOPIC'] = 'test-input'
    os.environ['KAFKA_OUTPUT_TOPIC'] = 'test-output'
    os.environ['KAFKA_DLQ_TOPIC'] = 'test-dlq'
    os.environ['KAFKA_CONSUMER_GROUP_ID'] = 'test-group'
    os.environ['KAFKA_RETRY_MAX_ATTEMPTS'] = '2'
    os.environ['KAFKA_RETRY_INITIAL_INTERVAL_SEC'] = '0.01' # Small interval for tests
    os.environ['KAFKA_SECURITY_PROTOCOL'] = 'PLAINTEXT'
    os.environ['KAFKA_SASL_MECHANISM'] = 'PLAIN'
    os.environ['KAFKA_SASL_USERNAME'] = 'test_user'
    os.environ['KAFKA_SASL_PASSWORD'] = 'test_password'
    os.environ['KAFKA_CONSUMER_AUTO_OFFSET_RESET'] = 'earliest'
    os.environ['KAFKA_CONSUMER_ENABLE_AUTO_COMMIT'] = 'True'
    os.environ['KAFKA_CONSUMER_AUTO_COMMIT_INTERVAL_MS'] = '100'

    yield # Let the test run

    # Clean up environment variables after test
    del os.environ['KAFKA_BOOTSTRAP_SERVERS']
    del os.environ['KAFKA_INPUT_TOPIC']
    del os.environ['KAFKA_OUTPUT_TOPIC']
    del os.environ['KAFKA_DLQ_TOPIC']
    del os.environ['KAFKA_CONSUMER_GROUP_ID']
    del os.environ['KAFKA_RETRY_MAX_ATTEMPTS']
    del os.environ['KAFKA_RETRY_INITIAL_INTERVAL_SEC']
    del os.environ['KAFKA_SECURITY_PROTOCOL']
    del os.environ['KAFKA_SASL_MECHANISM']
    del os.environ['KAFKA_SASL_USERNAME']
    del os.environ['KAFKA_SASL_PASSWORD']
    del os.environ['KAFKA_CONSUMER_AUTO_OFFSET_RESET']
    del os.environ['KAFKA_CONSUMER_ENABLE_AUTO_COMMIT']
    del os.environ['KAFKA_CONSUMER_AUTO_COMMIT_INTERVAL_MS']

# --- Tests for config.py ---

def test_config_loading(set_env_vars):
    assert Config.KAFKA_BOOTSTRAP_SERVERS == 'test_bootstrap:9092'
    assert Config.KAFKA_INPUT_TOPIC == 'test-input'
    assert Config.KAFKA_RETRY_MAX_ATTEMPTS == 2
    assert Config.KAFKA_CONSUMER_ENABLE_AUTO_COMMIT is True

def test_config_kafka_common_config(set_env_vars):
    common_conf = Config.get_kafka_common_config()
    assert common_conf['bootstrap.servers'] == 'test_bootstrap:9092'
    assert common_conf['security.protocol'] == 'PLAINTEXT'
    assert 'sasl.username' in common_conf # Check that SASL properties are present

# --- Tests for kafka_producer.py ---

@pytest.fixture
def mock_confluent_producer():
    with patch('confluent_kafka.Producer') as MockProducer:
        yield MockProducer

def test_producer_initialization(mock_confluent_producer, set_env_vars):
    producer = KafkaProducer()
    mock_confluent_producer.assert_called_once()

def test_producer_publish_message(mock_confluent_producer, set_env_vars):
    mock_producer_instance = mock_confluent_producer.return_value
    producer = KafkaProducer()
    topic = "test-topic"
    key = "test-key"
    value = {"data": "test-value"}

    producer.publish_message(topic, key, value)

    mock_producer_instance.poll.assert_called_with(0)
    mock_producer_instance.produce.assert_called_once_with(
        topic, key=key, value=json.dumps(value).encode('utf-8'), callback=producer.delivery_report
    )

def test_producer_delivery_report_success():
    producer = KafkaProducer()
    err = None
    msg = MagicMock()
    msg.topic.return_value = "test-topic"
    msg.partition.return_value = 0
    msg.offset.return_value = 123

    # Capture log output (optional, but good for testing logging behavior)
    with patch('logging.Logger.info') as mock_log_info:
        producer.delivery_report(err, msg)
        mock_log_info.assert_called_once()
        assert "Message delivered" in mock_log_info.call_args[0][0]

def test_producer_delivery_report_failure():
    producer = KafkaProducer()
    err = MagicMock(spec=Exception)
    msg = MagicMock()
    msg.topic.return_value = "test-topic"

    with patch('logging.Logger.error') as mock_log_error:
        producer.delivery_report(err, msg)
        mock_log_error.assert_called_once()
        assert "Message delivery failed" in mock_log_error.call_args[0][0]

# --- Tests for main.py (processing logic) ---

def test_process_order_message():
    input_message = {
        "order_id": "123",
        "customer_id": "CUST001",
        "item": "Book",
        "quantity": 2,
        "price": 25.50
    }
    processed_message = process_order_message(input_message)
    assert processed_message['order_id'] == "123"
    assert processed_message['status'] == "processed"
    assert "processed_timestamp" in processed_message
    assert processed_message['processed_price'] == 25.50

# --- Tests for kafka_consumer.py ---

@pytest.fixture
def mock_confluent_consumer():
    with patch('confluent_kafka.Consumer') as MockConsumer:
        yield MockConsumer

@pytest.fixture
def mock_kafka_producer_instance():
    with patch('kafka_producer.KafkaProducer') as MockKafkaProducerClass:
        mock_instance = MockKafkaProducerClass.return_value
        yield mock_instance

def create_mock_message(value, key=None, topic="test-input", partition=0, offset=0, error=None):
    msg = MagicMock()
    msg.value.return_value = value.encode('utf-8')
    msg.key.return_value = key.encode('utf-8') if key else None
    msg.topic.return_value = topic
    msg.partition.return_value = partition
    msg.offset.return_value = offset
    msg.error.return_value = error
    return msg

def test_consumer_initialization(mock_confluent_consumer, mock_kafka_producer_instance, set_env_vars):
    consumer_app = KafkaConsumer(process_order_message)
    mock_confluent_consumer.assert_called_once()
    assert consumer_app.process_message_func == process_order_message
    mock_kafka_producer_instance.assert_called_once()

def test_consumer_json_decode_error(mock_confluent_consumer, mock_kafka_producer_instance, set_env_vars):
    # Simulate a consumer that returns a bad JSON message once
    bad_json_msg = create_mock_message(value="not a json", key="bad-key")

    mock_consumer_instance = mock_confluent_consumer.return_value
    mock_consumer_instance.poll.side_effect = [bad_json_msg, None, None] # Return bad msg, then None twice to exit loop

    consumer_app = KafkaConsumer(process_order_message)

    # Patch _send_to_dlq to verify it's called
    with patch.object(consumer_app, '_send_to_dlq') as mock_send_to_dlq:
        # We need to simulate the consumer loop running a few times
        # Patching the infinite loop for testing purposes
        with patch.object(consumer_app, 'subscribe_and_consume', wraps=consumer_app.subscribe_and_consume) as mock_sac:
            # Simulate 3 calls to the consumer loop by patching its internal poll loop
            mock_consumer_instance.poll.side_effect = [bad_json_msg, None, None, None, KeyboardInterrupt]
            
            try:
                consumer_app.subscribe_and_consume()
            except KeyboardInterrupt:
                pass
            
            mock_send_to_dlq.assert_called_once() # Expect DLQ call for bad JSON
            args, kwargs = mock_send_to_dlq.call_args
            assert args[0].value() == bad_json_msg.value()
            assert "JSON parsing error" in args[1]['error']

def test_consumer_processing_retry_and_dlq(mock_confluent_consumer, mock_kafka_producer_instance, set_env_vars):
    # Simulate a message that causes processing error multiple times
    def failing_processor(msg_data):
        failing_processor.call_count += 1
        if failing_processor.call_count <= Config.KAFKA_RETRY_MAX_ATTEMPTS:
            raise ValueError("Simulated processing error")
        return {"status": "processed"}
    failing_processor.call_count = 0

    input_msg = create_mock_message(value=json.dumps({"id": "retry-test", "item": "test"}), key="retry-test")

    mock_consumer_instance = mock_confluent_consumer.return_value
    mock_consumer_instance.poll.side_effect = [input_msg, None, None, None, KeyboardInterrupt] # Message, then None to exit

    consumer_app = KafkaConsumer(failing_processor)

    with patch.object(consumer_app, '_send_to_dlq') as mock_send_to_dlq:
        try:
            consumer_app.subscribe_and_consume()
        except KeyboardInterrupt:
            pass

        assert failing_processor.call_count == Config.KAFKA_RETRY_MAX_ATTEMPTS
        mock_send_to_dlq.assert_called_once() # Expect DLQ call after max retries
        args, kwargs = mock_send_to_dlq.call_args
        assert args[0].value() == input_msg.value()
        assert "Processing failed after retries" in args[1]['error']

def test_consumer_successful_processing(mock_confluent_consumer, mock_kafka_producer_instance, set_env_vars):
    # Simulate a message that processes successfully
    def successful_processor(msg_data):
        return {"processed_id": msg_data['id']}

    input_msg = create_mock_message(value=json.dumps({"id": "success-test"}), key="success-test")

    mock_consumer_instance = mock_confluent_consumer.return_value
    mock_consumer_instance.poll.side_effect = [input_msg, None, KeyboardInterrupt] # Message, then None to exit

    consumer_app = KafkaConsumer(successful_processor)

    with patch.object(consumer_app, '_send_to_dlq') as mock_send_to_dlq:
        try:
            consumer_app.subscribe_and_consume()
        except KeyboardInterrupt:
            pass

        mock_kafka_producer_instance.publish_message.assert_called_once()
        args, kwargs = mock_kafka_producer_instance.publish_message.call_args
        assert args[0] == Config.KAFKA_OUTPUT_TOPIC
        assert args[1] == "success-test"
        assert args[2]['processed_id'] == "success-test"
        mock_send_to_dlq.assert_not_called() # DLQ should not be called on success

def test_consumer_close(mock_confluent_consumer, mock_kafka_producer_instance, set_env_vars):
    consumer_app = KafkaConsumer(process_order_message)
    consumer_app.close()
    mock_confluent_consumer.return_value.close.assert_called_once()
    mock_kafka_producer_instance.close.assert_called_once()
