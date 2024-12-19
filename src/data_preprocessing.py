from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType
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


def remove_outliers(df, column, method="iqr"):
    """
    Remove outliers from a Spark DataFrame based on the specified method.

    Args:
        df: Input Spark DataFrame.
        column: Column name to apply outlier removal.
        method: Outlier detection method ("iqr" or "std").

    Returns:
        Spark DataFrame without outliers.
    """
    if method == "iqr":
        # Calculate Q1, Q3, and IQR
        quantiles = df.approxQuantile(column, [0.25, 0.75], 0.05)
        q1, q3 = quantiles[0], quantiles[1]
        iqr = q3 - q1

        # Define lower and upper bounds
        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr

        # Filter out rows outside the bounds
        df = df.filter((F.col(column) >= lower_bound) & (F.col(column) <= upper_bound))
        print(f"Outliers removed from {column} using IQR.")
    elif method == "std":
        # Calculate mean and standard deviation
        stats = df.select(
            F.mean(F.col(column)).alias("mean"),
            F.stddev(F.col(column)).alias("std")
        ).collect()[0]
        mean, stddev = stats["mean"], stats["std"]

        # Define lower and upper bounds
        lower_bound = mean - 3 * stddev
        upper_bound = mean + 3 * stddev

        # Filter out rows outside the bounds
        df = df.filter((F.col(column) >= lower_bound) & (F.col(column) <= upper_bound))
        print(f"Outliers removed from {column} using standard deviation.")
    else:
        raise ValueError("Invalid method. Choose 'iqr' or 'std'.")

    return df


def filter_and_clean_data(spark, raw_data_path, processed_data_path):
    """
    Filter data for the last 5 years, remove outliers, and save the cleaned data.

    Args:
        spark: SparkSession object.
        raw_data_path: Path to the raw data CSV file.
        processed_data_path: Path to save the cleaned data.
    """
    current_year = datetime.now().year

    # Load the data
    df = spark.read.csv(raw_data_path, header=True, inferSchema=True)

    # Filter rows where roll_year is within the last 5 years
    df = df.filter(df.roll_year >= (current_year - 5))

    # Remove outliers for numeric columns
    numeric_columns = ["assessed_value", "land_size_sm", "land_size_sf", "land_size_ac"]
    for column in numeric_columns:
        df = remove_outliers(df, column, method="iqr")

    # Sort the data in descending order by year
    df = df.orderBy(df.roll_year.desc())

    # Save the cleaned data
    df.write.csv(processed_data_path, header=True, mode="overwrite")
    print(f"Filtered and cleaned data saved to {processed_data_path}")
