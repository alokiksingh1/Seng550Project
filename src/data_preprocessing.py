from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType
from datetime import datetime
from pyspark.sql import SparkSession
import json
def create_spark_session(config_path):
    """
    Create a Spark session using the configuration file.

    Args:
        config_path (str): Path to the configuration file.

    Returns:
        SparkSession: A configured Spark session.
    """
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
    Remove rows where the specified column contains outliers based on the given method.

    Args:
        df (DataFrame): Input DataFrame.
        column (str): Column name for outlier removal.
        method (str): Outlier removal method. Default is "iqr".

    Returns:
        DataFrame: DataFrame with outliers removed.
    """
    if method == "iqr":
        # Check if the column has any non-zero or non-default values
        if df.filter(F.col(column) > 0).count() == 0:
            print(f"Skipping outlier removal for column: {column} (no valid values)")
            return df

        # Calculate IQR
        quantiles = df.approxQuantile(column, [0.25, 0.75], 0.01)
        if len(quantiles) < 2 or quantiles[0] is None or quantiles[1] is None:
            print(f"Skipping outlier removal for column: {column} (unable to compute quantiles)")
            return df

        q1, q3 = quantiles
        iqr = q3 - q1
        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr

        # Filter out outliers
        return df.filter((F.col(column) >= lower_bound) & (F.col(column) <= upper_bound))
    else:
        raise ValueError(f"Unsupported method: {method}")

def filter_and_clean_data(spark, raw_data_path, processed_data_path):
    """
    Load raw data, clean it, handle missing columns, and filter outliers.
    Save the cleaned and filtered data to the processed data path.
    """
    # Load raw data
    df = spark.read.csv(raw_data_path, header=True, inferSchema=True)
    print(f"Initial row count: {df.count()}")

    # Drop duplicates
    df = df.dropDuplicates()

    # Add missing columns with default values
    required_columns = {
        "ASSESSED_VALUE": 0.0,
        "LAND_SIZE_SM": 0.0,
        "LAND_SIZE_SF": 0.0,
        "LAND_SIZE_AC": 0.0,
        "YEAR_OF_CONSTRUCTION": 0,
        "PROPERTY_TYPE": "Unknown",
    }

    for col_name, default_value in required_columns.items():
        if col_name not in df.columns:
            df = df.withColumn(col_name, F.lit(default_value))

    print(f"Rows after adding missing columns: {df.count()}")

    # Calculate property age
    current_year = datetime.now().year
    if "YEAR_OF_CONSTRUCTION" in df.columns:
        df = df.withColumn(
            "PROPERTY_AGE",
            F.when(F.col("YEAR_OF_CONSTRUCTION") > 0, current_year - F.col("YEAR_OF_CONSTRUCTION")).otherwise(0),
        )
    else:
        df = df.withColumn("PROPERTY_AGE", F.lit(0))

    # Log `PROPERTY_AGE` for debugging
    print("Sample PROPERTY_AGE values:")
    df.select("YEAR_OF_CONSTRUCTION", "PROPERTY_AGE").show(10)

    # Drop unnecessary columns
    unnecessary_columns = ["MULTIPOLYGON", "COMM_CODE"]
    df = df.drop(*[col for col in unnecessary_columns if col in df.columns])

    # Drop rows with missing critical values
    critical_columns = ["ASSESSED_VALUE", "PROPERTY_AGE"]
    df = df.dropna(subset=critical_columns)

    print(f"Rows after dropping NA: {df.count()}")

    # Remove outliers using IQR
    numerical_columns = ["ASSESSED_VALUE", "LAND_SIZE_SM", "LAND_SIZE_SF", "LAND_SIZE_AC"]
    for column in numerical_columns:
        if column in df.columns:
            print(f"Removing outliers in column: {column}")
            df_before = df.count()
            df = remove_outliers(df, column, method="iqr")
            df_after = df.count()
            print(f"Rows before: {df_before}, after: {df_after}")

    # Save the cleaned and filtered data
    final_row_count = df.count()
    if final_row_count > 0:
        df.write.csv(processed_data_path, header=True, mode="overwrite")
        print(f"Filtered and cleaned data saved to {processed_data_path}")
    else:
        print("No data available after filtering. Nothing saved.")
