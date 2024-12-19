import pandas as pd
import requests

def fetch_data(api_url):
    """Fetch data from the API."""
    response = requests.get(api_url)
    if response.status_code == 200:
        print("Data fetched successfully.")
        return response.json()
    else:
        print(f"Failed to fetch data. Status code: {response.status_code}")
        return None

def save_data_to_csv(data, file_path):
    """Save the fetched data to a CSV file."""
    if data:
        df = pd.DataFrame(data)
        df.to_csv(file_path, index=False)
        print(f"Data saved to {file_path}")
    else:
        print("No data to save.")

if __name__ == "__main__":
    # Define the API URL
    api_url = "https://data.calgary.ca/resource/4ur7-wsgc.json?$query=SELECT%0A%20%20%60roll_year%60%2C%0A%20%20%60roll_number%60%2C%0A%20%20%60address%60%2C%0A%20%20%60assessed_value%60%2C%0A%20%20%60assessment_class%60%2C%0A%20%20%60assessment_class_description%60%2C%0A%20%20%60re_assessed_value%60%2C%0A%20%20%60nr_assessed_value%60%2C%0A%20%20%60fl_assessed_value%60%2C%0A%20%20%60comm_code%60%2C%0A%20%20%60comm_name%60%2C%0A%20%20%60year_of_construction%60%2C%0A%20%20%60land_use_designation%60%2C%0A%20%20%60property_type%60%2C%0A%20%20%60lan..."

    # Path to save raw data
    raw_data_path = "data/raw/calgary_housing_raw.csv"

    # Fetch and save the data
    data = fetch_data(api_url)
    save_data_to_csv(data, raw_data_path)
