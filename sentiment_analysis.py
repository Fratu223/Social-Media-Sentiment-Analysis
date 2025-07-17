# flake8: noqa: F401
# Import needed libraries
import json
import logging
import os
import signal
import sys
import sqlite3
from datetime import datetime
from typing import Dict, Any, Optional, List
from dotenv import load_dotenv

from flask import Flask, request, jsonify
import psycopg2
from psycopg2.extras import RealDictCursor
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from textblob import TextBlob
import pandas as pd


class SentimentAnalyzer:
    def __init__(self, db_config: Optional[Dict[str, Any]] = None):
        self.db_config = db_config
        self.db_connection = None
        self.use_postgresql = db_config is not None

        # Setup logging
        logging.basicConfig(
            level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
        )
        self.logger = logging.getLogger(__name__)

        # Initialize sentiment analyzers
        self.vader_analyzer = SentimentIntensityAnalyzer()

        # Initialize database
        self.init_database()

        # Initialize Flask app
        self.app = Flask(__name__)
        self.setup_routes()

    def init_database(self):
        # Initialize database connection and create tables
        try:
            if self.use_postgresql:
                self.init_postgresql()
            else:
                self.init_sqlite()

            self.create_tables()
            self.logger.info("Database initialized successfully")

        except Exception as e:
            self.logger.error(f"Failed to initialize database: {e}")
            raise

    def init_postgresql(self):
        # Initialize Postgresql connection
        try:
            self.db_connection = psycopg2.connect(
                host=self.db_config["host"],
                database=self.db_config["database"],
                user=self.db_config["user"],
                password=self.db_config["password"],
                port=self.db_config.get("port", 5432),
            )
            self.db_connection.autocommit = True
            self.logger.info("PostgreSQL connection established")

        except Exception as e:
            self.logger.error(f"Failed to connect to PostgreSQL: {e}")
            raise

    def init_sqlite(self):
        # Initialize SQLite connection
        try:
            db_path = os.getenv("SQLITE_DB_PATH", "twitter_sentiment.db")
            self.db_connection = sqlite3.connect(db_path, check_same_thread=False)
            self.db_connection.row_factory = sqlite3.Row
            self.logger.info(f"SQLite database initialized: {db_path}")

        except Exception as e:
            self.logger.error(f"Failed to initialize SQLite: {e}")
            raise

    def create_tables(self):
        # Create database tables
        try:
            if self.use_postgresql:
                cursor = self.db_connection.cursor()

                # Create tweets table
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXIST tweets (
                        id SERIAL PRIMARY KEY,
                        tweet_id VARCHAR(50) UNIQUE NOT NULL,
                        tweet_text TEXT NOT NULL,
                        cleaned_text TEXT,
                        created_at TIMESTAMP,
                        author_id VARCHAR(50),
                        language VARCHAR(10),
                        retweet_count INTEGER DEFAULT 0,
                        like_count INTEGER DEFAULT 0,
                        reply_count INTEGER DEFAULT 0,
                        quote_count INTEGER DEFAULT 0,

                        -- VADER sentiment scores
                        vader_sentiment VARCHAR(20),
                        vader_compound FLOAT,
                        vader_positive FLOAT,
                        vader_negative FLOAT,
                        vader_neutral FLOAT,

                        -- TextBlob sentiment scores
                        textblob_sentiment VARCHAR(20),
                        textblob_polarity FLOAT,
                        textblob_subjectivity FLOAT,

                        -- Combined sentiment
                        final_sentiment VARCHAR(20),
                        confidence_score FLOAT,

                        -- Metadata
                        processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        kafka_timestamp BIGINT,

                        -- Indexes
                        UNIQUE(tweet_id)
                    )
                """
                )

                # Create sentiment_summary table
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS sentiment_summary (
                        id SERIAL PRIMARY KEY,
                        date_hour TIMESTAMP,
                        sentiment VARCHAR(20),
                        tweet_count INTEGER,
                        avg_confidence FLOAT,
                        total_likes INTEGER,
                        total_retweets INTEGER,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """
                )

                cursor.close()

            else:
                cursor = self.db_connection.cursor()

                # Create tweets table
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS tweets (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        tweet_id TEXT UNIQUE NOT NULL,
                        tweet_text TEXT NOT NULL,
                        cleaned_text TEXT,
                        created_at TEXT,
                        author_id TEXT,
                        language TEXT,
                        retweet_count INTEGER DEFAULT 0,
                        like_count INTEGER DEFAULT 0,
                        reply_count INTEGER DEFAULT 0,
                        quote_count INTEGER DEFAULT 0,

                        -- VADER sentiment scores
                        vader_sentiment TEXT,
                        vader_compound REAL,
                        vader_positive REAL,
                        vader_negative REAL,
                        vader_neutral REAL,

                        -- TextBlob sentiment scores
                        textblob_sentiment TEXT,
                        textblob_polarity REAL,
                        textblob_subjectivity REAL,

                        -- Combined sentiment
                        final_sentiment TEXT,
                        confidence_score REAL,

                        -- Metadat
                        processed_at TEXT DEFAULT CURRENT_TIMESTAMP,
                        kafka_timestamp INTEGER
                    )
                """
                )

                # Create sentiment_summary table
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS sentiment_summary (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        date_hour TEXT,
                        sentiment TEXT,
                        tweet_count INTEGER,
                        avg_confidence REAL,
                        total_likes INTEGER,
                        total_retweets INTEGER,
                        created_at TEXT DEFAULT CURRENT_TIMESTAMP
                    )
                """
                )

                self.db_connection.commit()
                cursor.close()

        except Exception as e:
            self.logger.error(f"Failed to create tables: {e}")
            raise

    def analyze_sentiment_vader(self, text: str) -> Dict[str, Any]:
        # Analyze sentiment using VADER
        try:
            scores = self.vader_analyzer.polarity_scores(text)

            # Determine sentiment based on compound score
            if scores["compound"] >= 0.05:
                sentiment = "positive"
            elif scores["compound"] <= -0.05:
                sentiment = "negative"
            else:
                sentiment = "neutral"

            return {
                "sentiment": sentiment,
                "compound": scores["compound"],
                "positive": scores["pos"],
                "negative": scores["neg"],
                "neutral": scores["neu"],
            }

        except Exception as e:
            self.logger.error(f"Error in VADER analysis: {e}")
            return {
                "sentiment": "neutral",
                "compound": 0.0,
                "positive": 0.0,
                "negative": 0.0,
                "neutral": 1.0,
            }

    def analyze_sentiment_textblob(self, text: str) -> Dict[str, Any]:
        # Analyze sentiment using TextBlob
        try:
            blob = TextBlob(text)
            polarity = blob.sentiment.polarity
            subjectivity = blob.sentiment.subjectivity

            # Determine sentiment based on polarity
            if polarity > 0.1:
                sentiment = "positive"
            elif polarity < -0.1:
                sentiment = "negative"
            else:
                sentiment = "neutral"

            return {
                "sentiment": sentiment,
                "polarity": polarity,
                "subjectivity": subjectivity,
            }

        except Exception as e:
            self.logger.error(f"Error in TextBlob analysis: {e}")
            return {"sentiment": "neutral", "polarity": 0.0, "subjectivity": 0.0}

    def combine_sentiment_scores(
        self, vader_result: Dict, textblob_result: Dict
    ) -> Dict[str, Any]:
        # Combine VADER and TextBlob results for final sentiment
        try:
            # Simple ensemble approach
            vader_weight = 0.6
            textblob_weight = 0.4

            # Convert sentiment to numeric scores
            sentiment_to_score = {"positive": 1, "neutral": 0, "negative": -1}

            vader_score = sentiment_to_score.get(vader_result["sentiment"], 0)
            textblob_score = sentiment_to_score.get(textblob_result["sentiment"], 0)

            # Weighted combination
            combined_score = (
                vader_weight * vader_score + textblob_weight * textblob_score
            )

            # Determine final sentiment
            if combined_score > 0.1:
                final_sentiment = "positive"
            elif combined_score < -0.1:
                final_sentiment = "negative"
            else:
                final_sentiment = "neutral"

            # Calculate confidence based on agreement and strength
            confidence = abs(combined_score)
            if vader_result["sentiment"] == textblob_result["sentiment"]:
                confidence *= 1.2

            confidence = min(confidence, 1.0)

            return {
                "sentiment": final_sentiment,
                "confidence": confidence,
                "combined_score": combined_score,
            }

        except Exception as e:
            self.logger.error(f"Error combining sentiment scores: {e}")
            return {"sentiment": "neutral", "confidence": 0.0, "combined_score": 0.0}

    def analyze_text(self, text: str) -> Dict[str, Any]:
        # Perform complete sentiment analysis
        try:
            # Clean text
            cleaned_text = text.strip()

            if not cleaned_text:
                return {
                    "sentiment": "neutral",
                    "confidence": 0.0,
                    "compund": 0.0,
                    "positive": 0.0,
                    "negative": 0.0,
                    "neutral": 1.0,
                }

            # Analyze with both methods
            vader_result = self.analyze_sentiment_vader(cleaned_text)
            textblob_result = self.analyze_sentiment_textblob(cleaned_text)
            combined_result = self.combine_sentiment_scores(
                vader_result, textblob_result
            )

            # Return combined result with detailed scores
            return {
                "sentiment": combined_result["sentiment"],
                "confidence": combined_result["confidence"],
                "compound": vader_result["compound"],
                "positive": vader_result["positive"],
                "negative": vader_result["negative"],
                "neutral": vader_result["neutral"],
                "polarity": textblob_result["polarity"],
                "subjectivity": textblob_result["subjectivity"],
            }

        except Exception as e:
            self.logger.error(f"Error in text analysis: {e}")
            return {
                "sentiment": "neutral",
                "confidence": 0.0,
                "compound": 0.0,
                "positive": 0.0,
                "negative": 0.0,
                "neutral": 1.0,
                "polarity": 0.0,
                "subjectivity": 0.0,
            }
