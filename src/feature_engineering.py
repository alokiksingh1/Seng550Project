from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
from datetime import datetime

def perform_feature_engineering(spark, processed_data_path, engineered_data_path):
    """
    Perform feature engineering on the processed data.

    Args:
        spark: SparkSession object.
        processed_data_path: Path to the processed data CSV.
        engineered_data_path: Path to save the engineered data CSV.
    """
    # Load the processed data
    df = spark.read.csv(processed_data_path, header=True, inferSchema=True)
    print("Columns in processed data:", df.columns)

    # Handle missing values
    df = df.fillna({"year_of_construction": 0, "assessed_value": 0})

    # Feature 1: Create 'property_age' (current_year - year_of_construction)
    current_year = datetime.now().year
    df = df.withColumn("property_age", F.lit(current_year) - F.col("year_of_construction"))

    # Feature 2: Normalize 'assessed_value'
    df = df.withColumn("assessed_value_normalized", 
                       F.when(F.col("assessed_value") > 0, F.col("assessed_value") / 1e6).otherwise(0))

    # Feature 3: Encode categorical 'assessment_class_description'
    df = df.withColumn(
        "assessment_class_encoded",
        F.when(F.col("assessment_class_description") == "Residential", 1)
         .when(F.col("assessment_class_description") == "Commercial", 2)
         .otherwise(0)
    )

    # Drop unnecessary columns
    columns_to_drop = ["multipolygon", "comm_code"]  # Example columns
    df = df.drop(*[col for col in columns_to_drop if col in df.columns])

    # Save the engineered data
    df.write.csv(engineered_data_path, header=True, mode="overwrite")
    print(f"Feature-engineered data saved to {engineered_data_path}")
