from pyspark.sql import SparkSession
from data_collection import fetch_data
from data_preprocessing import clean_data_spark

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

def merge_spark_output(input_path, output_path):
    """Merge Spark output files into a single CSV."""
    spark = SparkSession.builder.getOrCreate()
    df = spark.read.csv(input_path, header=True, inferSchema=True)
    df.coalesce(1).write.csv(output_path, header=True, mode="overwrite")
    print(f"Merged file saved to {output_path}")

def main():
    # Config and file paths
    config_path = "../config/spark_config.json"
    api_url = "https://data.calgary.ca/resource/4ur7-wsgc.json?$query=SELECT%0A%20%20%60roll_year%60%2C%0A%20%20%60roll_number%60%2C%0A%20%20%60address%60%2C%0A%20%20%60assessed_value%60%2C%0A%20%20%60assessment_class%60%2C%0A%20%20%60assessment_class_description%60%2C%0A%20%20%60re_assessed_value%60%2C%0A%20%20%60nr_assessed_value%60%2C%0A%20%20%60fl_assessed_value%60%2C%0A%20%20%60comm_code%60%2C%0A%20%20%60comm_name%60%2C%0A%20%20%60year_of_construction%60%2C%0A%20%20%60land_use_designation%60%2C%0A%20%20%60property_type%60%2C%0A%20%20%60land_size_sm%60%2C%0A%20%20%60land_size_sf%60%2C%0A%20%20%60land_size_ac%60%2C%0A%20%20%60sub_property_use%60%2C%0A%20%20%60multipolygon%60"
    raw_data_path = "../data/raw/calgary_housing_raw.csv"
    processed_data_path = "../data/processed/calgary_housing_cleaned/"
    merged_data_path = "../data/processed/calgary_housing_merged.csv"

    # Step 1: Create Spark session
    print("Initializing Spark session...")
    spark = create_spark_session(config_path)

    # Step 2: Fetch data and save as raw CSV
    print("Fetching data from API...")
    data = fetch_data(api_url)
    if data:
        import pandas as pd
        df = pd.DataFrame(data)
        df.to_csv(raw_data_path, index=False)
        print(f"Raw data saved to {raw_data_path}")
    else:
        print("No data fetched.")
        return

    # Step 3: Preprocess data using Spark
    print("Starting preprocessing with Spark...")
    clean_data_spark(spark, raw_data_path, processed_data_path)
    print(f"Processed data saved to {processed_data_path}")

    # Step 4: Merge Spark output
    print("Merging Spark output files...")
    merge_spark_output(processed_data_path, merged_data_path)

    print("Pipeline executed successfully!")

if __name__ == "__main__":
    main()
