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
    """
    Filter data for the last 5 years and sort it in descending order by year.

    Args:
        spark: SparkSession object.
        raw_data_path: Path to the raw data CSV file.
        processed_data_path: Path to save the processed and sorted data.
    """
    current_year = datetime.now().year

    # Load the data with Spark
    df = spark.read.csv(raw_data_path, header=True, inferSchema=True)

    # Filter rows where roll_year is within the last 5 years
    filtered_df = df.filter(df.roll_year >= (current_year - 5))

    # Sort the filtered data in descending order by roll_year
    sorted_df = filtered_df.orderBy(filtered_df.roll_year.desc())

    # Save the sorted data
    sorted_df.write.csv(processed_data_path, header=True, mode="overwrite")
    print(f"Filtered and sorted data saved to {processed_data_path}")
