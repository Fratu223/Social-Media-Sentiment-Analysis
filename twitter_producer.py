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
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)