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

    # Test 3: Test producer
    try:
        print("\nTest 3: Testing producer...")
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers.split(","),
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            api_version=(0, 10, 1),
        )

        # Send test message
        test_message = {
            "test": True,
            "timestamp": int(time.time()),
            "message": "Kafka diagnostic test",
        }

        future = producer.send(topic, test_message)
        record_metadata = future.get(timeout=10)

        print(f"Test message sent to topic '{topic}'")
        print(f"    Partition: {record_metadata.partition}")
        print(f"    Offset: {record_metadata.offset}")

        producer.close()

    except Exception as e:
        print(f"Producer test failed: {e}")
        return False

    # Test 4: Test consumer with topic
    try:
        print("\nTest 4: Testing consumer subscription...")
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers.split(","),
            auto_offset_reset="latest",
            consumer_timeout_ms=5000,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            group_id="diagnostic-group",
        )

        print(f"Consumer subscribed to topic '{topic}'")

        # Try to read one message
        print("Waiting for messages (5 seconds)...")
        messages = consumer.poll(timeout_ms=5000)

        if messages:
            print(f"Received {len(messages)} message(s)")
            for topic_partition, msgs in messages.items():
                for msg in msgs:
                    print(f"    Message: {msg.value}")
        else:
            print("No messages received (this is normal if no producer is running)")

        consumer.close()

    except Exception as e:
        print(f"Consumer subscription failed: {e}")
        return False

    print("\n" + "=" * 40)
    print("All Kafka tests passed!")
    return True
