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

    def store_tweet(self, tweet_data: Dict[str, Any]) -> bool:
        # Store tweet with sentiment analysis in database
        try:
            cursor = self.db_connection.cursor()

            # Extract tweet information
            tweet_id = tweet_data.get("tweet_id")
            tweet_text = tweet_data.get("tweet_text", "")
            cleaned_text = tweet_data.get("cleaned_text", tweet_text)

            # Analyze sentiment
            sentiment_result = self.analyze_text(cleaned_text)

            # Prepare insert query
            if self.use_postgresql:
                query = """
                    INSERT INTO tweets (
                        tweet_id, tweet_text, cleaned_text, created_at, author_id,
                        language, retweet_count, like_count, reply_count, quote_count,
                        vader_sentiment, vader_compound, vader_positive, vader_negative,
                        textblob_sentiment, textblob_polarity, textblob_subjectivity,
                        final_sentiment, confidence_score, kafka_timestamp
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    ) ON CONFLICT (tweet_id) DO NOTHING
                """
            else:
                query = """
                    INSERT OR IGNORE INTO tweets (
                        tweet_id, tweet_text, cleaned_text, created_at, author_id,
                        language, retweet_count, like_count, reply_count, quote_count,
                        vader_sentiment, vader_compound, vader_positive, vader_negative,
                        textblob_sentiment, textblob_polarity, textblob_subjectivity,
                        final_sentiment, confidence_score, kafka_timestamp
                    ) VALUES (
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                    )
                """

            # Execute query
            cursor.execute(
                query,
                (
                    tweet_id,
                    tweet_text,
                    cleaned_text,
                    tweet_data.get("created_at"),
                    tweet_data.get("author_id"),
                    tweet_data.get("language"),
                    tweet_data.get("retweet_count", 0),
                    tweet_data.get("like_count", 0),
                    tweet_data.get("reply_count", 0),
                    tweet_data.get("quote_count", 0),
                    sentiment_result["sentiment"],
                    sentiment_result["compound"],
                    sentiment_result["positive"],
                    sentiment_result["negative"],
                    sentiment_result["neutral"],
                    sentiment_result["sentiment"],
                    sentiment_result["polarity"],
                    sentiment_result["subjectivity"],
                    sentiment_result["sentiment"],
                    sentiment_result["confidence"],
                    tweet_data.get("kafka_timestamp"),
                ),
            )

            if not self.use_postgresql:
                self.db_connection.commit()

            cursor.close()

            self.logger.info(
                f"Stored tweet {tweet_id} with sentiment: {sentiment_result['sentiment']}"
            )
            return True

        except Exception as e:
            self.logger.error(f"Error storing tweet: {e}")
            return False

    def get_sentiment_summary(self, hours: int = 24) -> List[Dict[str, Any]]:
        # Get sentiment summary for the last N hours
        try:
            cursor = self.db_connection.cursor()

            if self.use_postgresql:
                query = """
                    SELECT
                        final_sentiment,
                        COUNT(*) as tweet_count,
                        AVG(confidence_score) as avg_confidence,
                        SUM(like_count) as total_likes,
                        SUM(retweet_count) as total_retweets
                    FROM tweets
                    WHERE processed_at >= NOW() - INTERVAL '%s hours'
                    GROUP BY final_sentiment
                    ORDER BY tweet_count DESC
                """
                cursor.execute(query, (hours,))
            else:
                query = """
                    SELECT
                        final_sentiment,
                        COUNT(*) as tweet_count,
                        AVG(confidence_score) as avg_confidence,
                        SUM(like_count) as total_likes,
                        SUM(retweet_count) as total_retweets
                    FROM tweets
                    WHERE processed_at >= datetime('now', '-%s hours')
                    GROUP BY final_sentiment
                    ORDER BY tweet_count DESC
                """
                cursor.execute(query, (hours,))

            results = cursor.fetchall()
            cursor.close()

            # Convert to list of dictionaries
            summary = []
            for row in results:
                if self.use_postgresql:
                    summary.append(
                        {
                            "sentiment": row[0],
                            "tweet_count": row[1],
                            "avg_confidence": float(row[2]) if row[2] else 0.0,
                            "total_likes": row[3] or 0,
                            "total_retweets": row[4] or 0,
                        }
                    )
                else:
                    summary.append(
                        {
                            "sentiment": row["final_sentiment"],
                            "tweet_count": row["tweet_count"],
                            "avg_confidence": (
                                float(row["avg_confidence"])
                                if row["avg_confidence"]
                                else 0.0
                            ),
                            "total_likes": row["total_likes"] or 0,
                            "total_retweets": row["total_retweets"] or 0,
                        }
                    )

            return summary

        except Exception as e:
            self.logger.error(f"Error getting sentiment summary: {e}")
            return []

    def get_recent_tweets(
        self, limit: int = 50, sentiment_filter: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        # Get recent tweets with optional sentiment filter
        try:
            cursor = self.db_connection.cursor()

            if sentiment_filter:
                if self.use_postgresql:
                    query = """
                        SELECT * FROM tweets
                        WHERE final_sentiment = %s
                        ORDER BY processed_at DESC
                        LIMIT %s
                    """
                    cursor.execute(query, (sentiment_filter, limit))
                else:
                    query = """
                        SELECT * FROM tweets
                        WHERE final_sentiment = ?
                        ORDER BY processed_at DESC
                        LIMIT ?
                    """
                    cursor.execute(query, (sentiment_filter, limit))
            else:
                if self.use_postgresql:
                    query = """
                        SELECT * FROM tweets
                        ORDER BY processed_at DESC
                        LIMIT %s
                    """
                    cursor.execute(query, (limit,))
                else:
                    query = """
                        SELECT * FROM tweets
                        ORDER BY processed_at DESC
                        LIMIT ?
                    """
                    cursor.execute(query, (limit,))

            results = cursor.fetchall()
            cursor.close()

            # Convert to list of dictionaries
            tweets = []
            for row in results:
                tweets.append(dict(row))

            return tweets

        except Exception as e:
            self.logger.error(f"Error getting recent tweets: {e}")
            return []

    def setup_routes(self):
        # Setup Flask routes for API endpoints

        @self.app.route("/analyze", methods=["POST"])
        def analyze_sentiment():
            # Analyze sentiment of provided text
            try:
                data = request.get_json()
                text = data.get("text", "")

                if not text:
                    return jsonify({"error": "No text provided"}), 400

                result = self.analyze_text(text)
                return jsonify(result)

            except Exception as e:
                self.logger.error(f"Error in /analyze endpoint: {e}")
                return jsonify({"error": "Internal server error"}), 500

        @self.app.route("/store", methods=["POST"])
        def store_tweet():
            # Store tweet with sentiment analysis
            try:
                tweet_data = request.get_json()

                if not tweet_data:
                    return jsonify({"error": "No tweet data provided"}), 400

                success = self.store_tweet(tweet_data)

                if success:
                    return jsonify(
                        {"status": "success", "message": "Tweet stored successfully"}
                    )
                else:
                    return jsonify({"error": "Failed to store tweet"}), 500

            except Exception as e:
                self.logger.error(f"Error in /store endpoint: {e}")
                return jsonify({"error": "Internal server error"}), 500

        @self.app.route("/summary", methods=["GET"])
        def get_summary():
            # Get sentiment summary
            try:
                hours = request.args.get("hours", 24, type=int)
                summary = self.get_sentiment_summary(hours)

                return jsonify(
                    {
                        "summary": summary,
                        "hours": hours,
                        "total_tweets": sum(item["tweet_count"] for item in summary),
                    }
                )

            except Exception as e:
                self.logger.error(f"Error in /summary endpoint: {e}")
                return jsonify({"error": "Internal server error"}), 500

        @self.app.route("/tweets", methods=["GET"])
        def get_tweets():
            # Get recent tweets
            try:
                limit = request.args.get("limit", 50, type=int)
                sentiment_filter = request.args.get("sentiment")

                tweets = self.get_recent_tweets(limit, sentiment_filter)

                return jsonify(
                    {
                        "tweets": tweets,
                        "count": len(tweets),
                        "sentiment_filter": sentiment_filter,
                    }
                )

            except Exception as e:
                self.logger.error(f"Error in /tweets endpoint: {e}")
                return jsonify({"error": "Internal server error"}), 500

        @self.app.route("/health", methods=["GET"])
        def health_check():
            # Health check enpoint
            return jsonify(
                {
                    "status": "healthy",
                    "timestamp": datetime.now().isoformat(),
                    "database": "postgresql" if self.use_postgresql else "sqlite",
                }
            )

        @self.app.route("/export", methods=["GET"])
        def export_data():
            # Export data to CSV
            try:
                format_type = request.args.get("format", "csv").lower()
                hours = request.args.get("hours", 24, type=int)

                cursor = self.db_connection.cursor()

                if self.use_postgresql:
                    query = """
                        SELECT * FROM tweets
                        WHERE processed_at >= NOW() - INTERVAL '%s hours'
                        ORDER BY processed_at DESC
                    """
                    cursor.execute(query, (hours,))
                else:
                    query = """
                        SELECT * FROM tweets
                        WHERE processed_at >= datetime('now', '-%s hours')
                        ORDER BY processed_at DESC
                    """
                    cursor.execute(query, (hours,))

                results = cursor.fetchall()
                cursor.close()

                if format_type == "csv":
                    # Convert to pandas DataFrame for easy CSV export
                    df = pd.DataFrame(results)
                    csv_data = df.to_csv(index=False)

                    return (
                        csv_data,
                        200,
                        {
                            "Content-Type": "text/csv",
                            "Content-Disposition": f"attachment; filname=tweets_{hours}h.csv",
                        },
                    )
                else:
                    # Return JSON
                    tweets = [dict(row) for row in results]
                    return jsonify({"tweets": tweets, "count": len(tweets)})

            except Exception as e:
                self.logger.error(f"Error in /export endpoint: {e}")
                return jsonify({"error": "Internal server error"}), 500

    def run_server(self, host: str = "0.0.0.0", port: int = 5000, debug: bool = False):
        # Run the Flask server
        self.logger.info(f"Starting sentiment analysis server on {host}:{port}")
        self.app.run(host=host, port=port, debug=debug)
