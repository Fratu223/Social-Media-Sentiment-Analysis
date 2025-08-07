# flake8: noqa: F401
# Kafka diagnostic script to help troubleshoot connection issues

# Import needed libraries
import os
from dotenv import load_dotenv
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError
import json
import time
