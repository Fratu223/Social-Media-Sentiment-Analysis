# flake8: noqa: F401
# Import needed libraries
import json
import logging
import os
import signal
import sys
from typing import Dict, Any, Optional
from dotenv import load_dotenv

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json,
    col,
    udf,
    current_timestamp,
    regexp_replace,
    lower,
    trim,
    when,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    BooleanType,
    ArrayType,
    LongType,
    MapType,
    DoubleType,
)
import requests


class TwitterSparkStreamer:
    def __init__(self, kafka_config: Dict[str, Any], sentiment_api_url: str):
        self.kafka_config = kafka_config
        self.sentiment_api_url = sentiment_api_url
        self.running = True

        # Setup logging
        logging.basicConfig(
            level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
        )
        self.logger = logging.getLogger(__name__)

        # Initialize Spark session
        self.spark = None
        self.init_spark_session()

        # Define schema for tweet data
        self.tweet_schema = self.get_tweet_schema()
