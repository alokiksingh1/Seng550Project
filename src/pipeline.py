from pyspark.sql import SparkSession
from data_collection import fetch_all_data_with_pagination, read_csv_with_spark, save_spark_dataframe
from data_preprocessing import clean_data_spark
from feature_engineering import feature_engineering_spark
from model_training import train_and_evaluate_model

from eda import eda_pipeline
import os

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


def main():
    # Paths and configurations
    api_url = "https://data.calgary.ca/resource/4ur7-wsgc.json"
    config_path = "../config/spark_config.json"
    raw_data_path = "../data/raw/calgary_housing_raw.csv"
    cleaned_data_path = "../data/processed/calgary_housing_cleaned/"
    uncleaned_data_path = "../data/processed/collection_spark_output/"
    preprocessed_data_path = "../data/processed/pre-processed_data/"
    feature_engineered_data_path = "../data/engineered/calgary_housing_features.csv"
    eda_output_path = "../reports/figures"
    model_output_path = "../models/re_assessed_value_model"
    predictions_output_path = "../data/predictions/re_assessed_value_predictions.csv"

    # Step 1: Initialize Spark session
    print("Initializing Spark session...")
    spark = create_spark_session(config_path)
    
    success = fetch_all_data_with_pagination(api_url, limit=1000, total_limit=100000, raw_data_path=raw_data_path)
    if not success:
        print("Data fetching failed.")

    # raw_data = read_csv_with_spark(spark, raw_data_path)
    # if raw_data is None:
    #     print("Raw data loading failed. Exiting pipeline.")
    #     return
    # save_spark_dataframe(raw_data, uncleaned_data_path)
    
    # Step 2: Clean raw data with Spark
    print("Cleaning data...")
    clean_data_spark(spark, raw_data_path, cleaned_data_path)
   

    # Step 3: Perform feature engineering
    print("Performing feature engineering...")
    feature_engineering_spark(spark,cleaned_data_path, feature_engineered_data_path)

    # # Step 4: Perform EDA
    # print("Running EDA...")
    # eda_pipeline(feature_engineered_data_path, "assessed_value")
    
    print("Training and evaluating the model...")
    train_and_evaluate_model(spark, feature_engineered_data_path, model_output_path, predictions_output_path)


    print("Pipeline executed successfully!")

if __name__ == "__main__":
    main()
 