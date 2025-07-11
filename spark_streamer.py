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

    def process_tweets(self):
        # Process tweets from Kafka stream
        try:
            # Read from Kafka
            kafka_df = (
                self.spark.readStream.format("kafka")
                .option(
                    "kafka.bootstrap.servers", self.kafka_config["bootstrap_servers"]
                )
                .option("subscribe", self.kafka_config["topic"])
                .option("startingOffsets", "latest")
                .option("failOnDataLoss", "false")
                .load()
            )

            # Parse JSON data
            parsed_df = kafka_df.select(
                from_json(col("value").cast("string"), self.tweet_schema).alias(
                    "tweet_data"
                ),
                col("timestamp").alias("kafka_received_timestamp"),
            )

            # Extract tweet fields
            tweets_df = parsed_df.select(
                col("tweet_data.data.id").alias("tweet_id"),
                col("tweet_data.data.text").alias("tweet_text"),
                col("tweet_data.data.created_at").alias("created_at"),
                col("tweet_data.data.author_id").alias("author_id"),
                col("tweet_data.data.lang").alias("language"),
                col("tweet_data.data.public_metrics.retweet_count").alias(
                    "retweet_count"
                ),
                col("tweet_data.data.public_metrics.like_count").alias("like_count"),
                col("tweet_data.data.public_metrics.reply_count").alias("reply_count"),
                col("tweet_data.data.public_metrics.quote_count").alias("quote_count"),
                col("tweet_data.includes.users").alias("users"),
                col("tweet_data.kafka_timestamp").alias("kafka_timestamp"),
                col("kafka_received_timestamp"),
                current_timestamp().alias("processed_timestamp"),
            )

            # Clean tweet text
            clean_text_udf = udf(self.clean_text, StringType())
            tweets_df = tweets_df.withColumn(
                "cleaned_text", clean_text_udf(col("tweet_text"))
            )

            # Filter out empty tweets and non-English tweets for sentiment analysis
            filtered_df = tweets_df.filter(
                (col("cleaned_text").isNotNull())
                & (col("cleaned_text") != "")
                & (col("language") == "en")
            )

            # Define UDF for sentiment analysis
            def get_sentiment(text):
                if not text:
                    return json.dumps(
                        {
                            "sentiment": "neutral",
                            "confidence": 0.0,
                            "compound": 0.0,
                            "positive": 0.0,
                            "negative": 0.0,
                            "neutral": 1.0,
                        }
                    )

                result = self.call_sentiment_api(text)
                return json.dumps(result)

            sentiment_udf = udf(get_sentiment, StringType())

            # Add sentiment analysis
            enriched_df = filtered_df.withColumn(
                "sentiment_raw", sentiment_udf(col("cleaned_text"))
            )

            # Parse sentiment JSON
            sentiment_schema = StructType(
                [
                    StructField("sentiment", StringType(), True),
                    StructField("confidence", DoubleType(), True),
                    StructField("compound", DoubleType(), True),
                    StructField("positive", DoubleType(), True),
                    StructField("negative", DoubleType(), True),
                    StructField("neutral", DoubleType(), True),
                ]
            )

            final_df = (
                enriched_df.select(
                    col("tweet_id"),
                    col("tweet_text"),
                    col("cleaned_text"),
                    col("created_at"),
                    col("author_id"),
                    col("language"),
                    col("retweet_count"),
                    col("like_count"),
                    col("reply_count"),
                    col("quote_count"),
                    col("users"),
                    col("kafka_timestamp"),
                    col("kafka_received_timestamp"),
                    col("processed_timestamp"),
                    from_json(col("sentiment_raw"), sentiment_schema).alias(
                        "sentiment_data"
                    ),
                )
                .select(
                    "*",
                    col("sentiment_data.sentiment").alias("sentiment"),
                    col("sentiment_data.confidence").alias("sentiment_confidence"),
                    col("sentiment_data.compound").alias("sentiment_compound"),
                    col("sentiment_data.positive").alias("sentiment_positive"),
                    col("sentiment_data.negative").alias("sentiment_negative"),
                    col("sentiment_data.neutral").alias("sentiment_neutral"),
                )
                .drop("sentiment_data", "sentiment_raw")
            )

            # Write to console for monitoring
            console_query = (
                final_df.writeStream.outputMode("append")
                .format("console")
                .option("truncate", "false")
                .trigger(processingTime="30 seconds")
                .start()
            )

            # Write to file (JSON format)
            output_path = self.kafka_config.get(
                "output_path", "/tmp/twitter_sentiment_output"
            )

            file_query = (
                final_df.writeStream.outputMode("append")
                .format("json")
                .option("path", output_path)
                .option("checkpointLocation", f"{output_path}/checkpoints")
                .trigger(processingTime="60 seconds")
                .start()
            )

            # Forward enriched data to sentiment analysis service
            def send_to_sentiment_service(df, epoch_id):
                try:
                    # Convert to JSON and send to sentiment anlysis service
                    tweets_json = df.toJSON().collect()

                    for tweet_json in tweets_json:
                        try:
                            requests.post(
                                f"{self.sentiment_api_url}/store",
                                json=json.loads(tweet_json),
                                timeout=5,
                            )
                        except Exception as e:
                            self.logger.error(
                                f"Error sending tweet to sentiment service {e}"
                            )

                except Exception as e:
                    self.logger.error(f"Error in batch processing: {e}")

            # Send to sentiment analysis service
            service_query = (
                final_df.writeStream.outputMode("append")
                .foreachBatch(send_to_sentiment_service)
                .trigger(processingTime="30 seconds")
                .start()
            )

            self.logger.info("Streaming queries started successfully")

            # Wait for termination
            console_query.awaitTermination()
            file_query.awaitTermination()
            service_query.awaitTermination()

        except Exception as e:
            self.logger.error(f"Error in stream processing: {e}")
            raise
