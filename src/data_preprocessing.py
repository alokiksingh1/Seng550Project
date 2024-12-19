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

    # Handle missing values with defaults
    default_values = {
        "assessed_value": 0.0,
        "re_assessed_value": 0.0,
        "nr_assessed_value": 0.0,
        "fl_assessed_value": 0.0,
        "comm_name": "Unknown",
        "property_type": "Unknown",
        "land_use_designation": "Unknown",
    }
    for col_name, default_value in default_values.items():
        if col_name in df.columns:
            df = df.fillna({col_name: default_value})

    # Convert numeric columns to appropriate types
    numeric_columns = ["assessed_value", "re_assessed_value", "nr_assessed_value", "fl_assessed_value"]
    for col_name in numeric_columns:
        if col_name in df.columns:
            df = df.withColumn(col_name, col(col_name).cast(DoubleType()))

    # Convert year-related columns to integers
    year_columns = ["roll_year", "year_of_construction"]
    for col_name in year_columns:
        if col_name in df.columns:
            df = df.withColumn(col_name, col(col_name).cast(IntegerType()))

    # Add calculated columns (e.g., property age)
    df = df.withColumn("property_age", 2024 - col("year_of_construction"))

    # Remove unnecessary columns
    unnecessary_columns = ["multipolygon", "comm_code"]
    for col_name in unnecessary_columns:
        if col_name in df.columns:
            df = df.drop(col_name)

    # Outlier removal using IQR method
    def remove_outliers(df, column_name):
        """
        Remove rows where the specified column contains outliers based on IQR.
        """
        q1 = df.approxQuantile(column_name, [0.25], 0.01)[0]
        q3 = df.approxQuantile(column_name, [0.75], 0.01)[0]
        iqr = q3 - q1
        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr
        return df.filter((col(column_name) >= lower_bound) & (col(column_name) <= upper_bound))

    # Apply outlier removal for numeric columns
    for col_name in numeric_columns:
        if col_name in df.columns:
            print(f"Removing outliers in column: {col_name}")
            df = remove_outliers(df, col_name)

    # Save the cleaned data
    df.write.csv(output_path, header=True, mode="overwrite")
    print(f"Cleaned data saved to {output_path}")
