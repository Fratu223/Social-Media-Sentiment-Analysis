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
                self.logger.error(f"Error adding rules: {result["errors"]}")
                return False
            
            self.logger.info(f"Added {len(result)} stream rules")
            return True
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error adding stream rules: {e}")
            return False
