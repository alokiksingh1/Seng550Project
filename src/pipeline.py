from data_collection import fetch_all_data_with_pagination
from data_preprocessing import create_spark_session, filter_and_clean_data
from feature_engineering import perform_feature_engineering
from model_training import train_and_evaluate_model

def main():
    # Paths and configurations
    config_path = "../config/spark_config.json"
    api_url = "https://data.calgary.ca/resource/4ur7-wsgc.json"
    raw_data_path = "../data/raw/calgary_housing_raw.csv"
    processed_data_path = "../data/processed/calgary_housing_cleaned/"
    engineered_data_path = "../data/engineered/calgary_housing_features/"

    # Step 1: Fetch raw data
    print("Fetching data from API...")
    success = fetch_all_data_with_pagination(api_url, limit=1000, total_limit=200000, raw_data_path=raw_data_path)
    if not success:
        print("Data fetching failed. Exiting pipeline.")
        return

    # Step 2: Filter, clean, and preprocess data
    print("Initializing Spark session...")
    spark = create_spark_session(config_path)
    print("Filtering, cleaning, and removing outliers...")
    filter_and_clean_data(spark, raw_data_path, processed_data_path)

    # Step 3: Perform feature engineering
    print("Performing feature engineering...")
    perform_feature_engineering(spark, processed_data_path, engineered_data_path)

    # Step 4: Train and evaluate the model
    print("Training and evaluating the model...")
    train_and_evaluate_model(spark, engineered_data_path, model_type="linear")


    print("Pipeline executed successfully!")

if __name__ == "__main__":
    main()
