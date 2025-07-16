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

    def create_table(self):
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
