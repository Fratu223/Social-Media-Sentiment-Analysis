# flake8: noqa: F401
# Test script to verify all components work correctly

# Import needed libraries
import os
import sys
import json
import time
import requests
from dotenv import load_dotenv


def test_imports():
    # Test that all required packages can be imported
    print("Testing imports...")

    try:
        import kafka

        print("kafka-python imported successfully")
    except ImportError as e:
        print(f"Failed to import kafka: {e}")
        return False

    try:
        from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

        print("VADER sentiment imported successfully")
    except ImportError as e:
        print(f"Failed to import VADER: {e}")
        return False

    try:
        from textblob import TextBlob

        print("TextBlob imported successfully")
    except ImportError as e:
        print(f"Failed to import TextBlob: {e}")
        return False

    try:
        import flask

        print("Flask imported successfully")
    except ImportError as e:
        print(f"Failed to import Flask: {e}")
        return False

    try:
        import pandas

        print("Pandas imported successfully")
    except ImportError as e:
        print(f"Failed to import Pandas: {e}")
        return False

    print(("All imports successful!\n"))
    return True


def test_environment():
    # Test environment variables
    print("Testing environment variables...")

    load_dotenv()

    twitter_token = os.getenv("TWITTER_BEARER_TOKEN")
    if twitter_token:
        print(f"TWITTER_BEARER_TOKEN found (length: {len(twitter_token)})")
    else:
        print("TWITTER_BEARER_TOKEN not found in environment")

    kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    print(f"Kafka servers: {kafka_servers}")

    kafka_topic = os.getenv("KAFKA_TOPIC", "twitter-search")
    print(f"Kafka topic: {kafka_topic}")

    search_query = os.getenv("SEARCH_QUERY", "premierleague OR championship")
    print(f"Search query: {search_query}")

    print()


def test_sentiment_analysis():
    # Test sentiment analysis functionality
    print("Testing sentiment analysis..")

    try:
        from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
        from textblob import TextBlob

        analyzer = SentimentIntensityAnalyzer

        # Test texts
        test_texts = [
            "I love this amazing product!",
            "This is terribel and awful.",
            "It's okay, nothing special.",
            "Best day ever! So excited!",
        ]

        for text in test_texts:
            # VADER
            vader_scores = analyzer.polarity_scores(text)

            # TextBlob
            blob = TextBlob(text)
            textblob_sentiment = blob.sentiment

            print(f"Text: '{text}'")
            print(f"    VADER: {vader_scores}")
            print(
                f"    TextBlob: polarity={textblob_sentiment.polarity:.3f}, subjectivity={textblob_sentiment.subjectivity:.3f}"
            )
            print()

        print("Sentiment analysis working correctly")
        return True

    except Exception as e:
        print(f"Sentiment analysis error: {e}\n")
        return False


def test_kafka_connection():
    # Test Kafka connection
    print("Testing Kafka connection...")

    try:
        from kafka import KafkaProducer, KafkaConsumer
        from kafka.errors import KafkaError

        load_dotenv
        bootstrap_servers = os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
        ).split(",")

        # Test producer
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )

        # Test consumer
        consumer = KafkaConsumer(
            bootstrap_servers=bootstrap_servers, consumer_timeout_ms=1000
        )

        producer.close()
        consumer.close()

        print("Kafka connection successful!\n")
        return True

    except Exception as e:
        print(f"Kafka connection failed: {e}")
        print("Make sure Kafka is running on localhost:9092\n")
        return False


def test_file_system():
    # Test file system permissions
    print("Testing file system...")

    try:
        # Test output directory creation
        output_path = "./test_output"
        os.makedirs(output_path, exist_ok=True)

        # Test file writing
        test_file = os.path.join(output_path, "test.json")
        with open(test_file, "w") as f:
            json.dump({"test": "data"}, f)

        # Test file reading
        with open(test_file, "r") as f:
            data = json.load(f)

        # Cleanup
        os.remove(test_file)
        os.rmdir(output_path)

        print("File system operations successful!\n")
        return True

    except Exception as e:
        print(f"File system error: {e}\n")
        return False
