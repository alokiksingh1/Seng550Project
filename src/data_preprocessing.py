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
    Remove outliers from a DataFrame column using the specified method.

    Args:
        df (DataFrame): Input DataFrame.
        column (str): Column name to process.
        method (str): Method for outlier removal (default: "iqr").

    Returns:
        DataFrame: DataFrame with outliers removed.
    """
    if method == "iqr":
        # Calculate IQR and filter out outliers
        q1, q3 = df.approxQuantile(column, [0.25, 0.75], 0.05)
        iqr = q3 - q1
        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr
        return df.filter((F.col(column) >= lower_bound) & (F.col(column) <= upper_bound))
    else:
        raise ValueError(f"Unknown method '{method}' for outlier removal.")



def filter_and_clean_data(spark, raw_data_path, processed_data_path):
    """
    Load raw data, clean it, handle nulls, and filter outliers.
    Save the cleaned and filtered data to the processed data path.
    """
    # Load raw data
    df = spark.read.csv(raw_data_path, header=True, inferSchema=True)

    # Example: Convert year_of_construction to property_age
    current_year = 2023
    df = df.withColumn(
        "property_age",
        F.when(F.col("year_of_construction").isNotNull(), current_year - F.col("year_of_construction")).otherwise(None)
    )

    # Handle nulls in numerical columns
    numerical_fill_values = {
        "property_age": 0,
        "assessed_value": 0,
        "land_size_sm": 0,
        "land_size_sf": 0,
        "land_size_ac": 0
    }
    df = df.fillna(numerical_fill_values)

    # Handle nulls in categorical columns
    categorical_fill_values = {
        "assessment_class": "Unknown",
        "sub_property_use": "Unknown"
    }
    df = df.fillna(categorical_fill_values)

    # Drop rows with nulls in critical columns
    critical_columns = ["property_age", "assessed_value"]
    df = df.dropna(subset=critical_columns)

    # Remove outliers using IQR method for numerical columns
    def remove_outliers(df, column):
        q1, q3 = df.approxQuantile(column, [0.25, 0.75], 0.05)
        iqr = q3 - q1
        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr
        return df.filter((F.col(column) >= lower_bound) & (F.col(column) <= upper_bound))

    numerical_columns = ["assessed_value", "land_size_sm", "land_size_sf", "land_size_ac"]
    for column in numerical_columns:
        if column in df.columns:
            df = remove_outliers(df, column)
            print(f"Outliers removed from {column} using IQR.")

    # Save the cleaned and filtered data
    df.write.csv(processed_data_path, header=True, mode="overwrite")
    print(f"Filtered and cleaned data saved to {processed_data_path}")
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
