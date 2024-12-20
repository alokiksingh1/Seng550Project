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

    # # Handle missing values with defaults
    # default_values = {
    #    "ASSESSED_VALUE": 0.0,
    #     "RE_ASSESSED_VALUE": 0.0,
    #     "NR_ASSESSED_VALUE": 0.0,
    #     "FL_ASSESSED_VALUE": 0.0,
    #     "LAND_SIZE_SM": 0.0,
    #     "LAND_SIZE_SF": 0.0,
    #     "LAND_SIZE_AC": 0.0,
    #     "YEAR_OF_CONSTRUCTION": 0,
    #     "PROPERTY_TYPE": "Unknown",
    #     "COMM_NAME": "Unknown",
    #     "LAND_USE_DESIGNATION": "Unknown",
    # }
    # for col_name, default_value in default_values.items():
    #     if col_name in df.columns:
    #         df = df.fillna({col_name: default_value})

    # # Remove unnecessary columns
    # unnecessary_columns = ["MULTIPOLYGON", "COMM_CODE"]
    # for col_name in unnecessary_columns:
    #     if col_name in df.columns:
    #         df = df.drop(col_name)

    # # Convert numeric columns to appropriate types
    # numeric_columns = ["ASSESSED_VALUE", "RE_ASSESSED_VALUE", "NR_ASSESSED_VALUE", "FL_ASSESSED_VALUE"]
    # for col_name in numeric_columns:
    #     if col_name in df.columns:
    #         df = df.withColumn(col_name, col(col_name).cast(DoubleType()))

    # # Convert year-related columns to integers
    # year_columns = ["ROLL_YEAR", "YEAR_OF_CONSTRUCTION"]
    # for col_name in year_columns:
    #     if col_name in df.columns:
    #         df = df.withColumn(col_name, col(col_name).cast(IntegerType()))

    # # Outlier removal using IQR method
    # def remove_outliers(df, column_name):
    #     """.
    #     Remove rows where the specified column contains outliers based on IQR.
    #     """
    #     q1 = df.approxQuantile(column_name, [0.25], 0.01)[0]
    #     q3 = df.approxQuantile(column_name, [0.75], 0.01)[0]
    #     iqr = q3 - q1
    #     lower_bound = q1 - 1.5 * iqr
    #     upper_bound = q3 + 1.5 * iqr
    #     return df.filter((col(column_name) >= lower_bound) & (col(column_name) <= upper_bound))

    # # Apply outlier removal for numeric columns
    # for col_name in numeric_columns:
    #     if col_name in df.columns:
    #         print(f"Removing outliers in column: {col_name}")
    #         df = remove_outliers(df, col_name)

    

    

    # # # Add calculated columns (e.g., property age)
    # # df = df.withColumn("property_age", 2024 - col("year_of_construction"))

    # # Calculate property_age, setting it to 0 for rows where YEAR_OF_CONSTRUCTION is missing (0)
    # df = df.withColumn("property_age", when(col("year_of_construction") == 0, 0).otherwise(2024 - col("year_of_construction")))

    

    # # Save the cleaned data
    # df.write.csv(output_path, header=True, mode="overwrite")
    # print(f"Cleaned data saved to {output_path}")
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