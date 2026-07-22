# Kafka Processor Application

This application consumes JSON messages from an input Kafka topic, processes them, and publishes the results to an output Kafka topic. It includes robust error handling, retry mechanisms, and configurable settings via environment variables.

## Features
- Reads JSON messages from Kafka.
- Processes messages (placeholder for actual processing logic).
- Publishes processed messages to an output Kafka topic.
- Configurable Kafka settings (bootstrap servers, security, topics, consumer group).
- Retry handling for temporary Kafka failures.
- Structured logging.
- Graceful error handling.

## Setup

1.  **Environment Variables**: Create a `.env` file based on `.env.example` with your Kafka and application settings.

    ```bash
    cp .env.example .env
    # Edit .env with your specific values
    ```

2.  **Install Dependencies**: If running locally outside Docker.

    ```bash
    pip install -r requirements.txt
    ```

3.  **Run Application**:

    ```bash
    python main.py
    ```

## Docker

To build and run the application using Docker:

```bash
# Build the Docker image
docker build -t kafka-processor .

# Run the Docker container (replace with your actual environment variables)
docker run -d --name kafka-processor \
    -e KAFKA_BOOTSTRAP_SERVERS="your_bootstrap_servers" \
    -e KAFKA_SECURITY_PROTOCOL="SASL_PLAINTEXT" \
    -e KAFKA_SASL_MECHANISM="SCRAM-SHA-512" \
    -e KAFKA_SASL_USERNAME="your_username" \
    -e KAFKA_SASL_PASSWORD="your_password" \
    -e KAFKA_INPUT_TOPIC="input_topic" \
    -e KAFKA_OUTPUT_TOPIC="output_topic" \
    -e KAFKA_DLQ_TOPIC="dlq_topic" \
    -e KAFKA_CONSUMER_GROUP_ID="my_consumer_group" \
    -e KAFKA_RETRY_MAX_ATTEMPTS=5 \
    -e KAFKA_RETRY_INITIAL_INTERVAL_SEC=1 \
    kafka-processor
```

## Project Structure

```
kafka_processor/
├── Dockerfile
├── README.md
├── requirements.txt
├── .env.example
├── env_variables.json
├── config.py
├── kafka_consumer.py
├── kafka_producer.py
├── main.py
├── samples/
│   ├── input.json
│   └── output.json
└── test_kafka_app.py
```
