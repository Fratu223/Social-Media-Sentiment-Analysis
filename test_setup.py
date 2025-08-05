# flake8: noqa: F401
# Test script to verify all components work correctly

# Import needed libraries
import os
import sys
import json
import time
import requests
from dotenv import load_dotenv


def test_imports():
    # Test that all required packages can be imported
    print("Testing imports...")

    try:
        import kafka

        print("kafka-python imported successfully")
    except ImportError as e:
        print(f"Failed to import kafka: {e}")
        return False

    try:
        from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

        print("VADER sentiment imported successfully")
    except ImportError as e:
        print(f"Failed to import VADER: {e}")
        return False

    try:
        from textblob import TextBlob

        print("TextBlob imported successfully")
    except ImportError as e:
        print(f"Failed to import TextBlob: {e}")
        return False

    try:
        import flask

        print("Flask imported successfully")
    except ImportError as e:
        print(f"Failed to import Flask: {e}")
        return False

    try:
        import pandas

        print("Pandas imported successfully")
    except ImportError as e:
        print(f"Failed to import Pandas: {e}")
        return False

    print(("All imports successful!\n"))
    return True
