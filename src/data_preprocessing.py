from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.sql.types import IntegerType, DoubleType

def clean_data_spark(spark, input_path, output_path):
    """
    Load, clean, remove outliers, and save data using Spark.
    """
    # Load raw data
    df = spark.read.csv(input_path, header=True, inferSchema=True)

    # Drop duplicates
    df = df.dropDuplicates()


    # Fill missing values
    default_values = {
        "ASSESSED_VALUE": 0.0,
        "RE_ASSESSED_VALUE": 0.0,
        "NR_ASSESSED_VALUE": 0.0,
        "FL_ASSESSED_VALUE": 0.0,
        "LAND_SIZE_SM": 0.0,
        "LAND_SIZE_SF": 0.0,
        "LAND_SIZE_AC": 0.0,
        "YEAR_OF_CONSTRUCTION": 0,
        "PROPERTY_TYPE": "Unknown",
        "COMM_NAME": "Unknown",
        "LAND_USE_DESIGNATION": "Unknown",
    }
    df = df.fillna(default_values)

    # Drop unnecessary columns
    df = df.drop("MULTIPOLYGON", "COMM_CODE")

    # Convert year-related columns
    df = df.withColumn("ROLL_YEAR", col("ROLL_YEAR").cast(IntegerType()))
    df = df.withColumn("YEAR_OF_CONSTRUCTION", col("YEAR_OF_CONSTRUCTION").cast(IntegerType()))

    # Calculate property_age
    df = df.withColumn("property_age", when(col("YEAR_OF_CONSTRUCTION") == 0, 0)
                        .otherwise(2024 - col("YEAR_OF_CONSTRUCTION")))

    # Compute bounds for outlier removal
    bounds = {}
    numeric_columns = ["ASSESSED_VALUE", "RE_ASSESSED_VALUE", "NR_ASSESSED_VALUE", "FL_ASSESSED_VALUE"]
    for col_name in numeric_columns:
        q1, q3 = df.approxQuantile(col_name, [0.25, 0.75], 0.1)
        iqr = q3 - q1
        bounds[col_name] = (q1 - 1.5 * iqr, q3 + 1.5 * iqr)

    # Apply filters
    for col_name, (lower, upper) in bounds.items():
        df = df.filter((col(col_name) >= lower) & (col(col_name) <= upper))

    # Save cleaned data
    df.write.csv(output_path, header=True, mode="overwrite")
    print(f"Cleaned data saved to {output_path}")
    
if __name__ == "__main__":
    # Hardcoded input and output paths for testing
    input_path = "../data/raw/calgary_housing_raw.csv"
    output_path = "../data/processed/calgary_housing_cleaned/"

    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Data Cleaning") \
        .master("local[*]") \
        .getOrCreate()

    # Run the cleaning function
    clean_data_spark(spark, input_path, output_path)
