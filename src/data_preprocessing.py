from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, DoubleType

def clean_data_spark(spark, input_path, output_path):
    """Load, clean, and save data using Spark."""
    # Load raw data into Spark DataFrame
    df = spark.read.csv(input_path, header=True, inferSchema=True)

    # Filter for the last 5 years
    from pyspark.sql.functions import year, current_date
    df = df.filter(col("roll_year").cast(IntegerType()) >= (int(current_date().substr(1, 4)) - 5))

    # Convert columns to appropriate types
    numeric_columns = [
        "assessed_value", "re_assessed_value", "nr_assessed_value",
        "fl_assessed_value", "land_size_sm", "land_size_sf", "land_size_ac"
    ]
    for col_name in numeric_columns:
        if col_name in df.columns:
            df = df.withColumn(col_name, col(col_name).cast(DoubleType()))

    # Handle missing values
    df = df.fillna({
        "re_assessed_value": 0,
        "nr_assessed_value": 0,
        "fl_assessed_value": 0,
        "comm_name": "Unknown",
        "property_type": "Unknown",
        "land_use_designation": "Unknown",
        "sub_property_use": "Unknown",
    })

    # Drop unnecessary columns
    if "multipolygon" in df.columns:
        df = df.drop("multipolygon")

    # Save cleaned data
    df.write.csv(output_path, header=True, mode="overwrite")

    print(f"Cleaned data saved to {output_path}")
