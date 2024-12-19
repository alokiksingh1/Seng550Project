from data_collection import fetch_all_data_with_pagination
from data_preprocessing import create_spark_session, filter_last_5_years

def main():
    # Paths and configurations
    config_path = "../config/spark_config.json"
    api_url = "https://data.calgary.ca/resource/4ur7-wsgc.json?$query=SELECT%0A%20%20%60roll_year%60%2C%0A%20%20%60roll_number%60%2C%0A%20%20%60address%60%2C%0A%20%20%60assessed_value%60%2C%0A%20%20%60assessment_class%60%2C%0A%20%20%60assessment_class_description%60%2C%0A%20%20%60re_assessed_value%60%2C%0A%20%20%60nr_assessed_value%60%2C%0A%20%20%60fl_assessed_value%60%2C%0A%20%20%60comm_code%60%2C%0A%20%20%60comm_name%60%2C%0A%20%20%60year_of_construction%60%2C%0A%20%20%60land_use_designation%60%2C%0A%20%20%60property_type%60%2C%0A%20%20%60land_size_sm%60%2C%0A%20%20%60land_size_sf%60%2C%0A%20%20%60land_size_ac%60%2C%0A%20%20%60sub_property_use%60%2C%0A%20%20%60multipolygon%60"
    raw_data_path = "../data/raw/calgary_housing_raw.csv"
    processed_data_path = "../data/processed/calgary_housing_filtered/"

    # Step 1: Fetch raw data using pagination
    print("Fetching data from API...")
    success = fetch_all_data_with_pagination(api_url, limit=100000, raw_data_path=raw_data_path)
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
