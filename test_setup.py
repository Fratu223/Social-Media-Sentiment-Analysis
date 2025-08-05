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
