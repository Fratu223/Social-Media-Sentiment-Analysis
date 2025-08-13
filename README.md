# Twitter Sentiment Analysis Pipeline

This pipeline consists of three main components:
1. **twitter_producer.py** - Fetches tweets from Twitter API and publishes to Kafka
2. **twitter_streamer.py** - Processes tweets using Kafka
3. **sentiment_analysis.py** - Provides sentiment analysis API and data storage
