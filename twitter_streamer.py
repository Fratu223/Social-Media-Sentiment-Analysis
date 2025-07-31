# flake8: noqa: F401
# Import needed libraries
import json
import logging
import os
import signal
import sys
import time
from typing import Dict, Any, Optional
from dotenv import load_dotenv

import requests
from kafka import KafkaConsumer
from kafka.errors import KafkaError
import threading


class SimpleTwitterStreamer:
    def __init__(self, kafka_config: Dict[str, Any], sentiment_api_url: str):
        self.kafka_config = kafka_config
        self.sentiment_api_url = sentiment_api_url
        self.running = True

        # Setup logging
        logging.basicConfig(
            level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
        )
        self.logger = logging.getLogger(__name__)

        # Initialize Kafka consumer
        self.consumer = None
        self.init_kafka_consumer()

    def init_kafka_consumer(self):
        # Initialize Kafka consumer
        try:
            self.consumer = KafkaConsumer(
                self.kafka_config["topic"],
                bootstrap_servers=self.kafka_config["bootstrap_servers"].split(","),
                auto_offset_reset="latest",
                enable_auto_commit=True,
                group_id="twitter-sentiment-group",
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                consumer_timeout_ms=10000,
            )
            self.logger.info(
                f"Kafka consumer initialized for topic: {self.kafka_config['topic']}"
            )

        except Exception as e:
            self.logger.error(f"Failed to initialize Kafka consumer: {e}")
            raise

    def clean_text(self, text: str) -> str:
        # Clean tweet text for better sentiment analysis
        if not text:
            return ""

        # Remove URLs, mentions, hashtags symbols (but keep the content)
        cleaned = text
        cleaned = cleaned.replace("RT @", "")
        cleaned = " ".join(cleaned.split())

        return cleaned.strip()

    def call_sentiment_api(self, text: str) -> Dict[str, Any]:
        # Call the sentiment analysis API
        try:
            if not text or not text.strip():
                return {
                    "sentiment": "neutral",
                    "confidence": 0.0,
                    "compound": 0.0,
                    "positive": 0.0,
                    "negative": 0.0,
                    "neutral": 1.0,
                }

            response = requests.post(
                self.sentiment_api_url, json={"text": text}, timeout=10
            )

            if response.status_code == 200:
                return response.json()
            else:
                self.logger.warning(f"Sentiment API error: {response.status_code}")
                return {
                    "sentiment": "neutral",
                    "confidence": 0.0,
                    "compound": 0.0,
                    "positive": 0.0,
                    "negative": 0.0,
                    "neutral": 1.0,
                }
        except Exception as e:
            self.logger.error(f"Error calling sentiment API: {e}")
            return {
                "sentiment": "neutral",
                "confidence": 0.0,
                "compound": 0.0,
                "positive": 0.0,
                "negative": 0.0,
                "neutral": 1.0,
            }

    def process_tweet(self, tweet_data: Dict[str, Any]) -> Dict[str, Any]:
        # Process a single tweet
        try:
            # Extract tweet information
            data = tweet_data.get("data", {})
            tweet_id = data.get("id", "")
            tweet_text = data.get("text", "")
            created_at = data.get("created_at", "")
            author_id = data.get("author_id", "")
            language = data.get("lang", "")

            # Extract public metrics
            public_metrics = data.get("public_metrics", {})
            retweet_count = public_metrics.get("retweet_count", 0)
            like_count = public_metrics.get("like_count", 0)
            reply_count = public_metrics.get("reply_count", 0)
            quote_count = public_metrics.get("quote_count", 0)

            # Clean text
            cleaned_text = self.clean_text(tweet_text)

            # Skip if no text or not English
            if not cleaned_text or language != "en":
                return None

            # Get sentiment analysis
            sentiment_result = self.call_sentiment_api(cleaned_text)

            # Create enriched tweet data
            enriched_tweet = {
                "tweet_id": tweet_id,
                "tweet_text": tweet_text,
                "cleaned_text": cleaned_text,
                "created_at": created_at,
                "author_id": author_id,
                "language": language,
                "retweet_count": retweet_count,
                "like_count": like_count,
                "reply_count": reply_count,
                "quote_count": quote_count,
                "sentiment": sentiment_result.get("sentiment", "neutral"),
                "sentiment_confidence": sentiment_result.get("confidence", 0.0),
                "sentiment_compound": sentiment_result.get("compound", 0.0),
                "sentiment_positive": sentiment_result.get("positive", 0.0),
                "sentiment_negative": sentiment_result.get("negative", 0.0),
                "sentiment_neutral": sentiment_result.get("neutral", 1.0),
                "kafka_timestamp": tweet_data.get("kafka_timestamp"),
                "processed_timestamp": int(time.time() * 1000),
            }

            return enriched_tweet

        except Exception as e:
            self.logger.error(f"Error processing tweet: {e}")
            return None

    def save_to_file(self, tweet_data: Dict[str, Any]):
        # Save tweet data to file
        try:
            output_path = self.kafka_config.get(
                "output_path", "/tmp/twitter_sentiment_output"
            )

            # Create directory if it doesn't exist
            os.makedirs(output_path, exist_ok=True)

            # Generate filename with timestamp
            timestamp = time.strftime("%Y%m%d_%h")
            filename = f"{output_path}/tweets_{timestamp}.jsonl"

            # Append to file
            with open(filename, "a", encoding="utf-8") as f:
                f.write(json.dumps(tweet_data) + "\n")

        except Exception as e:
            self.logger.error(f"Error saving to file: {e}")

    def send_to_sentiment_service(self, tweet_data: Dict[str, Any]):
        # Send tweet data to sentiment analysis service for storage
        try:
            response = requests.post(
                f"{self.sentiment_api_url.replace('/analyze', '/store')}",
                json=tweet_data,
                timeout=5,
            )

            if response.status_code == 200:
                self.logger.debug(
                    f"Sent tweet {tweet_data['tweet_id']} to sentiment service"
                )
            else:
                self.logger.warning(
                    f"Failed to send tweet to sentiment service: {response.status_code}"
                )

        except Exception as e:
            self.logger.error(f"Error sending tweet to sentiment service: {e}")

    def cleanup(self):
        # Clean up resources
        self.running = False
        if self.spark:
            try:
                self.spark.stop()
                self.logger.info("Spark session stopped")
            except Exception as e:
                self.logger.error(f"Error stopping Spark session: {e}")

    def signal_handler(self, signum, frame):
        # Handle shutdown signals
        self.logger.info(f"Received signal {signum}, shutting down...")
        self.cleanup()
        sys.exit(0)


def main():
    # Load environment variables
    load_dotenv()

    # Configuration
    KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "twitter-search")
    SENTIMENT_API_URL = os.getenv("SENTIMENT_API_URL", "http://localhost:5000/analyze")
    OUTPUT_PATH = os.getenv("OUTPUT_PATH", "/tmp/twitter_sentiment_output")

    # Kafka configuration
    kafka_config = {
        "bootstrap_servers": KAFKA_BOOTSTRAP_SERVERS,
        "topic": KAFKA_TOPIC,
        "output_path": OUTPUT_PATH,
    }

    # Initialize streamer
    try:
        streamer = SimpleTwitterStreamer(kafka_config, SENTIMENT_API_URL)

        # Setup signal handlers
        signal.signal(signal.SIGINT, streamer.signal_handler)
        signal.signal(signal.SIGTERM, streamer.signal_handler)

        print("Starting Twitter sentiment streaming...")
        print(f"Reading from Kafka topic: {KAFKA_TOPIC}")
        print(f"Sentiment API URL: {SENTIMENT_API_URL}")
        print(f"Output path: {OUTPUT_PATH}")
        print("Press Ctrl+C to stop...")

        # Start processing
        streamer.process_tweets()

    except KeyboardInterrupt:
        print("\nShutdown requested by user")
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
