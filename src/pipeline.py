from data_collection import fetch_all_data_with_pagination
from data_preprocessing import create_spark_session, filter_last_5_years

def main():
    # Paths and configurations
    config_path = "../config/spark_config.json"
    api_url = "https://data.calgary.ca/resource/4ur7-wsgc.json"
    raw_data_path = "../data/raw/calgary_housing_raw.csv"
    processed_data_path = "../data/processed/calgary_housing_filtered/"

    # Step 1: Fetch raw data using pagination with a total limit
    print("Fetching data from API...")
    success = fetch_all_data_with_pagination(api_url, limit=1000, total_limit=100000, raw_data_path=raw_data_path)
    if not success:
        print("Data fetching failed. Exiting pipeline.")
        return

    # Step 2: Filter data for the last 5 years using Spark
    print("Initializing Spark session...")
    spark = create_spark_session(config_path)
    print("Filtering data for the last 5 years...")
    filter_last_5_years(spark, raw_data_path, processed_data_path)

    print("Pipeline executed successfully!")

if __name__ == "__main__":
    main()
