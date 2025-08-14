# Twitter Sentiment Analysis Pipeline

This pipeline consists of three main components:
1. **twitter_producer.py** - Fetches tweets from Twitter API and publishes to Kafka
2. **twitter_streamer.py** - Processes tweets using Kafka
3. **sentiment_analysis.py** - Provides sentiment analysis API and data storage

## Requirements

### Python Dependencies

The 'requirements.txt' file:

```txt
tweepy==4.14.0
kafka-python==2.0.2
pandas==2.2.2
vaderSentiment==3.3.2
streamlit==1.35.0
psycopg2-binary==2.9.9
SQLAlchemy==1.4.49
scikit-learn==1.4.2
pyspark==3.5.1
textblob==0.18.0
requests==2.31.0
python-dotenv==1.0.1
flask==2.2.5
werkzeug==2.2.3
```

### System Dependencies

1. **Java** - OpenJDK 11 or higher
2. **Apache Kafka** - Message broker
3. **PostgreSQL** - Database (optional, SQLite is used by default)

## Environment Setup

Create a `.env` file in your project directory:

```bash
# Twitter API Configuration
TWITTER_BEARER_TOKEN=your_twitter_bearer_token_here

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC=twitter-search
POLL_INTERVAL=60

# Search Configuration
SEARCH_QUERY=premierleague OR championship

# Spark Configuration
SENTIMENT_API_URL=http://localhost:5000/analyze
OUTPUT_PATH=/tmp/twitter_sentiment_output

# Database Configuration (PostgreSQL - optional)
USE_POSTGRESQL=false
POSTGRES_HOST=localhost
POSTGRES_DB=twitter_sentiment
POSTGRES_USER=postgres
POSTGRES_PASSWORD=password
POSTGRES_PORT=5432

# SQLite Configuration (default)
SQLITE_DB_PATH=twitter_sentiment.db

# Sentiment Analysis Service
SENTIMENT_HOST=0.0.0.0
SENTIMENT_PORT=5000
DEBUG=false
```

## Installation Steps

### 1. Install Python Dependencies

```bash
pip install -r requirements.txt
```

### 2. Download and Setup Kafka

```bash
# Download Kafka
wget https://downloads.apache.org/kafka/2.13-3.5.0/kafka_2.13-3.5.0.tgz
tar -xzf kafka_2.13-3.5.0.tgz
cd kafka_2.13-3.5.0

# Start Zookeeper
bin/zookeeper-server-start.sh config/zookeeper.properties

# Start Kafka (in another terminal)
bin/kafka-server-start.sh config/server.properties

# Create topic
bin/kafka-topics.sh --create --topic twitter-search --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

### 3. Setup Database (Optional PostgreSQL)

```bash
# Install PostgreSQL
sudo apt-get install postgresql postgresql-contrib

# Create database
sudo -u postgres psql
CREATE DATABASE twitter_sentiment;
CREATE USER sentiment_user WITH PASSWORD 'password';
GRANT ALL PRIVILEGES ON DATABASE twitter_sentiment TO sentiment_user;
\q
```

### 4. Get Twitter API Bearer Token

1. Go to [X Developer Portal](https://developer.x.com/en/porta)
2. Create a new app
3. Generate Bearer Token
4. Add it to your `.env` file

## Running the Pipeline

### Terminal 1: Start Sentiment Analysis Service

```bash
python sentiment_analysis.py
```

This will:
- Start Flask API server on port 5000
- Initialize database (SQLite by default)
- Provide endpoints for sentiment analysis and data retrieval

### Terminal 2: Start Twitter Streaming

```bash
python twitter_streamer.py
```

This will:
- Connect to Kafka and consume tweets
- Process tweets and call sentiment analysis API
- Output results to console and files
