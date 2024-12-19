from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, DoubleType

def clean_data_spark(spark, input_path, output_path):
    """Load, clean, and save data using Spark."""
    # Load raw data into Spark DataFrame
    df = spark.read.csv(input_path, header=True, inferSchema=True)

    # Convert numeric columns to appropriate types
    numeric_columns = [
        "assessed_value", "re_assessed_value", "nr_assessed_value",
        "fl_assessed_value", "land_size_sm", "land_size_sf", "land_size_ac"
    ]
    for col_name in numeric_columns:
        if col_name in df.columns:
            df = df.withColumn(col_name, col(col_name).cast(DoubleType()))

    # Convert year-related columns to integers
    year_columns = ["roll_year", "year_of_construction"]
    for col_name in year_columns:
        if col_name in df.columns:
            df = df.withColumn(col_name, col(col_name).cast(IntegerType()))

    # Handle missing values (only for existing columns)
    default_values = {
        "assessed_value": 0,
        "re_assessed_value": 0,
        "nr_assessed_value": 0,
        "fl_assessed_value": 0,
        "comm_name": "Unknown",
        "property_type": "Unknown",
        "land_use_designation": "Unknown",
        "sub_property_use": "Unknown",
    }
    for col_name, default_value in default_values.items():
        if col_name in df.columns:
            df = df.fillna({col_name: default_value})

    # Drop unnecessary columns
    unnecessary_columns = ["multipolygon", "comm_code"]
    for col_name in unnecessary_columns:
        if col_name in df.columns:
            df = df.drop(col_name)

    # Remove duplicates
    df = df.dropDuplicates()

    # Save cleaned data
    df.write.csv(output_path, header=True, mode="overwrite")

    print(f"Cleaned data saved to {output_path}")
