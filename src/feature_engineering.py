from pyspark.sql.functions import col, expr, avg
from pyspark.ml.feature import OneHotEncoder, StringIndexer
from pyspark.sql.window import Window
import os
from pyspark.sql import functions as F

def feature_engineering_spark(spark, input_path, output_path):
    """
    Perform feature engineering on cleaned data using Spark.
    """
    print("Loading data for feature engineering...")
    df_spark = spark.read.csv(input_path, header=True, inferSchema=True)

    # Convert all column names to lowercase
    df_spark = df_spark.select([F.col(col_name).alias(col_name.lower()) for col_name in df_spark.columns])

    # Check for required columns
    required_columns = ["assessed_value", "land_size_sf", "year_of_construction"]
    missing_columns = [col for col in required_columns if col not in df_spark.columns]

    if missing_columns:
        print(f"Missing required columns for feature engineering: {missing_columns}")
        return


    # Create new features
    print("Creating new features...")
    df_spark = df_spark.withColumn("price_per_sqft", col("assessed_value") / (col("land_size_sf") + 1))
    df_spark = df_spark.withColumn("property_age", expr("2024 - year_of_construction"))

    # Add neighborhood trends
    print("Calculating neighborhood trends...")
    window_spec = Window.partitionBy("comm_name")
    df_spark = df_spark.withColumn("avg_comm_value", avg("assessed_value").over(window_spec))

    # One-hot encode categorical features
    # print("Encoding categorical features...")
    # if "property_type" in df_spark.columns:
    #     indexer = StringIndexer(inputCol="property_type", outputCol="property_type_index")
    #     encoder = OneHotEncoder(inputCol="property_type_index", outputCol="property_type_vec")
    #     df_spark = encoder.fit(indexer.fit(df_spark).transform(df_spark)).transform(df_spark)

    # if "comm_name" in df_spark.columns:
    #     indexer = StringIndexer(inputCol="comm_name", outputCol="comm_name_index")
    #     encoder = OneHotEncoder(inputCol="comm_name_index", outputCol="comm_name_vec")
    #     df_spark = encoder.fit(indexer.fit(df_spark).transform(df_spark)).transform(df_spark)

    # Save the feature-engineered data
    print("Saving feature-engineered data...")
    os.makedirs(output_path, exist_ok=True)
    df_spark.write.csv(output_path, header=True, mode="overwrite")
    print(f"Feature-engineered data saved to {output_path}")
