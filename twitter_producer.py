# flake8: noqa: F401, E501
# Import needed libraries
import json
import logging
import os
import signal
import sys
import time
from typing import Dict, Any, Optional, Set
from dotenv import load_dotenv

load_dotenv()

import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError


class TwitterKafkaProducer:
    def __init__(self, bearer_token: str, kafka_config: Dict[str, Any], topic: str):
        self.bearer_token = bearer_token
        self.topic = topic
        self.running = True
        self.seen_tweets: Set[str] = set()

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
        self.search_url = "https://api.twitter.com/2/tweets/search/recent"

    def get_headers(self) -> Dict[str, str]:
        # Get headers for Twitter API requests
        return {
            "Authorization": f"Bearer {self.bearer_token}",
            "Content-Type": "application/json",
        }

    def search_tweets(
        self, query: str, max_results: int = 10, tweet_fields: Optional[list] = None
    ) -> Optional[Dict]:
        # Search for tweets using recent search endpoint
        if tweet_fields is None:
            tweet_fields = [
                "id",
                "text",
                "created_at",
                "author_id",
                "public_metrics",
                "lang",
                "context_annotations",
            ]

        params = {
            "query": query,
            "max_results": min(max_results, 100),
            "tweet.fields": ",".join(tweet_fields),
            "expansions": "author_id",
            "user.fields": "id,name,username,verified,public_metrics",
        }

        try:
            response = requests.get(
                self.search_url, headers=self.get_headers(), params=params
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error searching tweets: {e}")
            if hasattr(e, "response") and e.response:
                self.logger.error(f"Response: {e.response.text}")
            return None

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

    def poll_and_publish(self, query: str, poll_interval: int = 60) -> None:
        # Poll Twitter search API and publish new tweets to Kafka
        self.logger.info(f"Starting Twitter search polling for query: {query}")
        self.logger.info(f"Poll interval: {poll_interval} seconds")

        while self.running:
            try:
                # Search for tweets
                result = self.search_tweets(query)

                if result and "data" in result:
                    tweets = result["data"]
                    new_tweets = 0

                    for tweet in tweets:
                        tweet_id = tweet["id"]

                        # Skip if we've already seen this tweet
                        if tweet_id in self.seen_tweets:
                            continue

                        # Add to seen tweets set
                        self.seen_tweets.add(tweet_id)

                        # Create tweet data structure
                        tweet_data = {
                            "data": tweet,
                            "includes": result.get("includes", {}),
                        }

                        # Log tweet info
                        self.logger.info(
                            f"New tweet: {tweet['id']} -" f"{tweet['text'][:50]}..."
                        )

                        # Publish to Kafka
                        if self.publish_to_kafka(tweet_data):
                            new_tweets += 1
                            self.logger.debug(f"Published tweet {tweet['id']} to Kafka")

                    self.logger.info(f"Processed {new_tweets} new tweets")

                    # Clean up seen tweets set if it gets too large
                    if len(self.seen_tweets) > 10000:
                        self.logger.info("Cleaning up seen tweets cache")
                        self.seen_tweets.clear()

                elif result and "errors" in result:
                    self.logger.error(f"API errors: {result['errors']}")
                else:
                    self.logger.info("No new tweets found")

                # Wait before next poll
                if self.running:
                    time.sleep(poll_interval)

            except KeyboardInterrupt:
                self.logger.info("polling interrupted by user")
                break
            except Exception as e:
                self.logger.error(f"Error in polling loop: {e}")
                if self.running:
                    time.sleep(poll_interval)

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
    BEARER_TOKEN = os.getenv("TWITTER_BEARER_TOKEN")
    KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "twitter-search")
    POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "60"))

    # Search query
    SEARCH_QUERY = os.getenv("SEARCH_QUERY", "premierleague OR championship")

    if not BEARER_TOKEN:
        print("Error: TWITTER_BEARER_TOKEN environment variable is required")
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

        print(f"Search query: {SEARCH_QUERY}")
        print(f"Publishing tweets to Kafka topic: {KAFKA_TOPIC}")
        print(f"Poll interval: {POLL_INTERVAL} seconds")
        print("Press Ctrl+C to stop...")

        # Start polling
        producer.poll_and_publish(SEARCH_QUERY, POLL_INTERVAL)

    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
