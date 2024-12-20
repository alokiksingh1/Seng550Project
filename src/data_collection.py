from pyspark.sql import SparkSession
import os
import pandas as pd
import requests


def fetch_all_data_with_pagination(api_url, limit, total_limit, raw_data_path):
    """
    Fetch all data using pagination and save it to a CSV file, with a total limit on rows.

    Args:
        api_url (str): The API endpoint URL.
        limit (int): Number of rows to fetch per batch.
        total_limit (int): Maximum total number of rows to fetch.
        raw_data_path (str): Path to save the raw data as CSV.

    Returns:
        bool: True if data was successfully fetched and saved, False otherwise.
    """
    all_data = []
    offset = 0

    while len(all_data) < total_limit:
        remaining_rows = total_limit - len(all_data)
        current_limit = min(limit, remaining_rows)  # Adjust the batch size for the last fetch
        paginated_url = f"{api_url}?$limit={current_limit}&$offset={offset}"
        print(f"Fetching data from: {paginated_url}")
        response = requests.get(paginated_url)

        if response.status_code == 200:
            batch_data = response.json()
            if not batch_data:  # No more data
                break
            all_data.extend(batch_data)
            offset += current_limit
        else:
            print(f"Failed to fetch data at offset {offset}. Status code: {response.status_code}")
            break

    # Save all fetched data to a CSV file
    if all_data:
        os.makedirs(os.path.dirname(raw_data_path), exist_ok=True)
        df = pd.DataFrame(all_data)
        df.to_csv(raw_data_path, index=False)
        print(f"Total rows fetched: {len(df)}")
        print(f"All data saved to {raw_data_path}")
        return True
    else:
        print("No data fetched.")
        return False



def read_csv_with_spark(spark, file_path):
    """
    Read a large CSV file using Spark, handling partitioned files if necessary.

    Args:
        spark: SparkSession object.
        file_path (str): Path to the input CSV file or directory.

    Returns:
        Spark DataFrame: Loaded data.
    """
    if os.path.exists(file_path):
        print(f"Loading data from {file_path}...")
        if os.path.isdir(file_path):
            # Handle partitioned files (e.g., Spark outputs multiple CSV files)
            return spark.read.csv(f"{file_path}/*.csv", header=True)
        else:
            # Single CSV file
            return spark.read.csv(file_path, header=True, inferSchema=True)
    else:
        print(f"{file_path} does not exist. Exiting.")
        return None


def save_spark_dataframe(df, output_path):
    """
    Save Spark DataFrame to CSV in a memory-efficient way.

    Args:
        df: Spark DataFrame.
        output_path (str): Path to save the output CSV.
    """
    os.makedirs(output_path, exist_ok=True)
    df.write.csv(output_path, header=True, mode="overwrite")
    print(f"Data saved to {output_path}")


# if __name__ == "__main__":
#     # Example usage
#     api_url = "https://data.calgary.ca/resource/4ur7-wsgc.json"
#     raw_data_path = "../data/raw/calgary_housing_raw.csv"
#     config_path = "../config/spark_config.json"

#     # Fetch data using API (if needed)
#     if not os.path.exists(raw_data_path):
#         fetch_all_data_with_pagination(api_url, limit=1000, total_limit=100000, raw_data_path=raw_data_path)

#     # Create Spark session
#     spark = create_spark_session(config_path)

#     # Read CSV with Spark
#     spark_df = read_csv_with_spark(spark, raw_data_path)
#     if spark_df is not None:
#         # Perform some operation or save
#         save_spark_dataframe(spark_df, "../data/processed/spark_output/")


if __name__ == "__main__":
    api_url = "https://data.calgary.ca/resource/4ur7-wsgc.json"
    raw_data_path = "../data/raw/calgary_housing_raw.csv"
    success = fetch_all_data_with_pagination(api_url, limit=1000, total_limit=100000, raw_data_path=raw_data_path)
    if not success:
        print("Data fetching failed.")
