import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .appName("PytestSpark") \
        .config("spark.jars", "/opt/spark/jars/mysql-connector-j-8.3.0.jar") \
        .master("local[*]") \
        .getOrCreate()

