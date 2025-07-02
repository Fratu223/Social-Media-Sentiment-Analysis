import json
import logging
import os
import signal
import sys
import time
from typing import Dict, Any, Optional

import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError