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

    def init_spark_session(self):
        # Initialize Spark session with Kafka integration
        try:
            self.spark = (
                SparkSession.builder.appName("TwitterSentimentStreaming")
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                .config(
                    "spark.jars.packages",
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0",
                )
                .config("spark.streaming.stopGracefullyOnShutdown", "true")
                .getOrCreate()
            )

            # Set log level to reduce noise
            self.spark.sparkContext.setLogLevel("WARN")
            self.logger.info("Spark session initialized successfully")

        except Exception as e:
            self.logger.error(f"Failed to initialize Spark session: {e}")
            raise

    def get_tweet_schema(self) -> StructType:
        # Define the schema for tweet data structure
        return StructType(
            [
                StructField(
                    "data",
                    StructType(
                        [
                            StructField("id", StringType(), True),
                            StructField("text", StringType(), True),
                            StructField("created_at", StringType(), True),
                            StructField("author_id", StringType(), True),
                            StructField("lang", StringType(), True),
                            StructField(
                                "public_metrics",
                                StringType(
                                    [
                                        StructField(
                                            "retweet_count", IntegerType(), True
                                        ),
                                        StructField("reply_count", IntegerType(), True),
                                        StructField("quote_count", IntegerType(), True),
                                    ]
                                ),
                                True,
                            ),
                        ]
                    ),
                    True,
                ),
                StructField(
                    "includes",
                    StructType(
                        [
                            StructField(
                                "users",
                                ArrayType(
                                    [
                                        StructField("id", StringType(), True),
                                        StructField("name", StringType(), True),
                                        StructField("username", StringType(), True),
                                        StructField(
                                            "public_metrics",
                                            StructType(
                                                [
                                                    StructField(
                                                        "followers_count",
                                                        IntegerType(),
                                                        True,
                                                    ),
                                                    StructField(
                                                        "following_count",
                                                        IntegerType(),
                                                        True,
                                                    ),
                                                    StructField(
                                                        "tweet_count",
                                                        IntegerType(),
                                                        True,
                                                    ),
                                                    StructField(
                                                        "listed_count",
                                                        IntegerType(),
                                                        True,
                                                    ),
                                                ]
                                            ),
                                            True,
                                        ),
                                    ]
                                ),
                                True,
                            )
                        ]
                    ),
                    True,
                ),
                StructField("kafka_timestamp", LongType(), True),
            ]
        )
