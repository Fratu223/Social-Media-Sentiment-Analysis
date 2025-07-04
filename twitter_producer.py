# flake8: noqa: F401, E501
# Import needed libraries
import json
import logging
import os
import signal
import sys
import time
from typing import Dict, Any, Optional

import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError


class TwitterKafkaProducer:
    def __init__(self, bearer_token: str, kafka_config: Dict[str, Any], topic: str):
        self.bearer_token = bearer_token
        self.topic = topic
        self.running = True

        # Setup logging
        logging.basicConfig(
            level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
        )
        self.logger = logging.getLogger(__name__)

        # Initialize Kafka producer
        try:
            self.prducer = KafkaProducer(
                value_serializer=lambda v: json.dumps(v).encode("utf-8"), **kafka_config
            )
            self.logger.info(f"Kafka producer initialized for topic: {topic}")
        except Exception as e:
            self.logger.error(f"Failed to initialize Kafka producer: {e}")
            raise

        # Twitter API endpoints
        self.stream_url = "stream_url"
        self.rules_url = "rules_url"

    def get_headers(self) -> Dict[str, str]:
        # Get headers for Twitter API requests
        return {
            "Authorization": f"Bearer {self.bearer_token}",
            "Content-Type": "application/json",
        }

    def get_stream_rules(self) -> Optional[Dict]:
        # Get current stream rules
        try:
            response = requests.get(self.rules_url, headers=self.get_headers())
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error getting stream rules: {e}")
            return None

    def delete_stream_rules(self, rule_ids: list) -> bool:
        # Delete existing stream rules
        if not rule_ids:
            return True

        try:
            payload = {"delete": {"ids": rule_ids}}
            response = requests.post(
                self.rules_url, headers=self.get_headers(), json=payload
            )
            response.raise_for_status()
            self.logger.info(f"Deleted {len(rule_ids)} stream rules")
            return True
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error deleting stream rules: {e}")
            return False

    def add_stream_rules(self, rules: list) -> bool:
        # Add new stream rules
        try:
            payload = {"add": rules}
            response = requests.post(
                self.rules_url, headers=self.get_headers(), json=payload
            )
            response.raise_for_status()
            result = response.json()

            if "errors" in result:
                self.logger.error(f"Error adding rules: {result['errors']}")
                return False

            self.logger.info(f"Added {len(result)} stream rules")
            return True
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error adding stream rules: {e}")
            return False

    def setup_stream_rules(self, keywords: list) -> bool:
        # Get exisiting rules
        current_rules = self.get_stream_rules()

        # Delete existing rules
        if current_rules and "data" in current_rules:
            rule_ids = [rule[id] for rule in current_rules["data"]]
            if not self.delete_stream_rules(rule_ids):
                return False
        # Add new rules
        rules = []
        for keyword in keywords:
            rules.append(
                {"value": keyword, "tag": f"keyword_{keyword.replace(' ','_')}"}
            )

        return self.add_stream_rules(rules)

    def publish_to_kafka(self, tweet_data: Dict[str, Any]) -> bool:
        # Publish tweet data to Kafka topic
        try:
            # Add timestamp for processing
            tweet_data["kafka_timestamp"] = int(time.time() * 1000)

            future = self.producer.send(self.topic, value=tweet_data)
            record_metadata = future.get(timeout=10)

            self.logger.debug(
                f"Tweet published to {record_metadata.topic}"
                f"partition {record_metadata.partition}"
                f"offset {record_metadata.offset}"
            )
            return True

        except KafkaError as e:
            self.logger.error(f"Failed to publish tweet to Kafka: {e}")
            return False

        except Exception as e:
            self.logger.error(f"Unexpected error publishing to Kafka: {e}")
            return False

    def stream_tweets(self, tweet_fields: Optional[Dict] = None) -> None:
        # Stream tweets and publish to Kafka
        if tweet_fields is None:
            tweet_fields = [
                "id",
                "text",
                "created_at",
                "author_id",
                "public_metrics",
                "lang",
                "context_annotation",
            ]

        params = {
            "tweet.fields": ",".join(tweet_fields),
            "expansion": "author_id",
            "user.fields": "id,name,username,verified,public_metrics",
        }

        self.logger.info("Starting Twitter stream...")

        try:
            with requests.get(
                self.stream_url(),
                headers=self.get_headers(),
                params=params,
                stream=True,
                timeout=30,
            ) as response:

                response.raise_for_status()
                self.logger.info("Connected to Twitter stream")

                for line in response.iter_lines():
                    if not self.running:
                        break

                    if line:
                        try:
                            tweet_data = json.loads(line.decode("utf-8"))

                            # Skip heartbeat messages
                            if not tweet_data or "data" not in tweet_data:
                                continue

                            # Log tweet info
                            tweet = tweet_data["data"]
                            self.logger.info(
                                f"Received tweet: {tweet['id']} - "
                                f"{tweet['text'][:50]}..."
                            )

                            # Publish to Kafka
                            if self.publish_to_kafka(tweet_data):
                                self.logger.debug(
                                    f"Published tweet {tweet['id']} to Kafka"
                                )

                        except json.JSONDecodeError as e:
                            self.logger.error(f"Error parsing tweet JSON: {e}")
                        except KeyError as e:
                            self.logger.error(f"Missing expected field in format: {e}")
                        except Exception as e:
                            self.logger.error(f"Error processing tweet: {e}")
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error streaming tweets: {e}")
        except KeyboardInterrupt:
            self.logger.error("Stream interrupted by user")
        except Exception as e:
            self.logger.error(f"Unexpected error in stream: {e}")
        finally:
            self.cleanup()

    def cleanup(self):
        # Clean up resources
        self.running = False
        if hasattr(self, "producer"):
            self.prducer.flush()
            self.prducer.close()
            self.logger.info("Kafka producer closed")

    def signal_handler(self, signum, frame):
        # Handle shutdown signals
        self.logger.info(f"Received signal {signum}, shutting down...")
        self.running = False


def main():
    # Configuration
    BEARER_TOKEN = os.getenv("TWITTER_BEREAR_TOKEN")
    KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "twitter-stream")

    # Keywords to track
    KEYWORDS = ["premier league", "championship", "league one", "league two"]

    if not BEARER_TOKEN:
        print("Error: TWITTER_BEREAR_TOKEN environment variable is required")
        sys.exit(1)

    # Kafka configuration
    kafka_config = {
        "bootstrap_servers": KAFKA_BOOTSTRAP_SERVERS.split(","),
        "api_version": (0, 10, 1),
        "retries": 5,
        "max_in_flight_requests_per_connection": 1,
        "acks": "all",
    }

    # Initialize producer
    try:
        producer = TwitterKafkaProducer(
            bearer_token=BEARER_TOKEN, kafka_config=kafka_config, topic=KAFKA_TOPIC
        )

        # Setup signal headers
        signal.signal(signal.SIGINT, producer.signal_handler)
        signal.signal(signal.SIGTERM, producer.signal_handler)

        # Setup stream rules
        if producer.setup_stream_rules(KEYWORDS):
            print(f"Stream rules setup successfully for keywords: {KEYWORDS}")
            print(f"Publishing tweets to Kafka topic: {KAFKA_TOPIC}")
            print("Press CTRL+C to stop...")

            # Start streaming
            producer.stream_tweets()
        else:
            print("Failed to setup stream rules")
            sys.exit(1)

    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
