# flake8: noqa: F401
# Kafka diagnostic script to help troubleshoot connection issues

# Import needed libraries
import os
from dotenv import load_dotenv
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError
import json
import time


def test_kafka_connection():
    # Test Kafka connection with detailed debugging
    load_dotenv()

    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = os.getenv("KAFKA_TOPIC", "twiiter-search")

    print("Testing Kafka connection...")
    print(f"Bootstrap servers: {bootstrap_servers}")
    print(f"Topic: {topic}")
    print("-" * 40)

    # Test 1: Basic connection
    try:
        print("Test 1: Creating consumer...")
        consumer = KafkaConsumer(
            bootstrap_servers=bootstrap_servers.split(","),
            consumer_timeout_ms=5000,
            api_version=(0, 10, 1)
        )

        print("Consumer created successfully")

        # Get cluster metadata
        metadata = consumer.list_consumer_groups()
        print("Connected to cluster")

        consumer.close()

    except Exception as e:
        print(f"Consumer creation failed: {e}")
        return False
