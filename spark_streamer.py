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
    isnan,
    isnull,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    BooleanType,
    ArrayType,
    LongType,
    TimestampType,
)
from pyspark.sql.streaming import StreamingQuery
