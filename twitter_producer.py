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
