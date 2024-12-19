from pyspark.sql.functions import col, expr
from pyspark.ml.feature import OneHotEncoder, StringIndexer
import os

def feature_engineering_spark(spark,input_path, output_path):
    """
    Perform feature engineering on cleaned data using Spark.
    """
    print("Loading data for feature engineering...")
    # Load data from Spark output
    df_spark = spark.read.csv(input_path, header=True, inferSchema=True)

    # Create new features
    print("Creating new features...")
    df_spark = df_spark.withColumn("price_per_sqft", col("assessed_value") / (col("land_size_sf") + 1))
    df_spark = df_spark.withColumn("property_age", expr("2024 - year_of_construction"))

    # One-hot encode categorical features
    print("Encoding categorical features...")
    indexer = StringIndexer(inputCol="property_type", outputCol="property_type_index")
    encoder = OneHotEncoder(inputCol="property_type_index", outputCol="property_type_vec")
    df_spark = encoder.fit(indexer.fit(df_spark).transform(df_spark)).transform(df_spark)

    # Save the feature-engineered data
    print("Saving feature-engineered data...")
    os.makedirs(output_path, exist_ok=True)
    df_spark.write.csv(output_path, header=True, mode="overwrite")
    print(f"Feature-engineered data saved to {output_path}")
