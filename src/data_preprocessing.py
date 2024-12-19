from pyspark.sql import SparkSession
from datetime import datetime

def create_spark_session(config_path):
    """Create a Spark session using the config."""
    import json
    with open(config_path, "r") as file:
        config = json.load(file)
    spark = SparkSession.builder \
        .appName(config["app_name"]) \
        .master(config["master"]) \
        .getOrCreate()
    spark.sparkContext.setLogLevel(config["log_level"])
    return spark

def filter_last_5_years(spark, raw_data_path, processed_data_path):
    """Filter data for the last 5 years using Spark."""
    current_year = datetime.now().year

    # Load the data with Spark
    df = spark.read.csv(raw_data_path, header=True, inferSchema=True)

    # Filter rows where roll_year is within the last 5 years
    filtered_df = df.filter(df.roll_year >= (current_year - 5))

    # Save the filtered data
    filtered_df.write.csv(processed_data_path, header=True, mode="overwrite")
    print(f"Filtered data saved to {processed_data_path}")
