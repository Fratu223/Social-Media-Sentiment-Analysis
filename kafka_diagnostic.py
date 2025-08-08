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
            api_version=(0, 10, 1),
        )

        print("Consumer created successfully")

        # Get cluster metadata
        metadata = consumer.list_consumer_groups()
        print(f"Connected to cluster {metadata}")

        consumer.close()

    except Exception as e:
        print(f"Consumer creation failed: {e}")
        return False

    # Test 2: List topics
    try:
        print("\nTest 2: Listing topics...")
        consumer = KafkaConsumer(
            bootstrap_servers=bootstrap_servers.split(","), consumer_timeout_ms=5000
        )

        topics = consumer.topics()
        print(f"Available topics: {list(topics)}")

        if topic in topics:
            print(f"Target topic '{topic}' exists")
        else:
            print(f"Target topic '{topic}' does not exist")
            print("You may need to create it or check your twitter_producer.py")

        consumer.close()

    except Exception as e:
        print(f"Topic listing failed: {e}")
        return False
