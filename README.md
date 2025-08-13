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
