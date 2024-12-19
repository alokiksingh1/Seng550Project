import requests
import pandas as pd

def fetch_all_data_with_pagination(api_url, limit, raw_data_path):
    """Fetch all data using pagination and save it to a CSV file."""
    all_data = []
    offset = 0

    while True:
        paginated_url = f"{api_url}?$limit={limit}&$offset={offset}"
        print(f"Fetching data from: {paginated_url}")
        response = requests.get(paginated_url)

        if response.status_code == 200:
            batch_data = response.json()
            if not batch_data:  # No more data
                break
            all_data.extend(batch_data)
            offset += limit
        else:
            print(f"Failed to fetch data at offset {offset}. Status code: {response.status_code}")
            break

    # Save all fetched data to a CSV file
    if all_data:
        df = pd.DataFrame(all_data)
        df.to_csv(raw_data_path, index=False)
        print(f"All data saved to {raw_data_path}")
        return True
    else:
        print("No data fetched.")
        return False

if __name__ == "__main__":
    api_url = "https://data.calgary.ca/resource/4ur7-wsgc.json?$query=SELECT%0A%20%20%60roll_year%60%2C%0A%20%20%60roll_number%60%2C%0A%20%20%60address%60%2C%0A%20%20%60assessed_value%60%2C%0A%20%20%60assessment_class%60%2C%0A%20%20%60assessment_class_description%60%2C%0A%20%20%60re_assessed_value%60%2C%0A%20%20%60nr_assessed_value%60%2C%0A%20%20%60fl_assessed_value%60%2C%0A%20%20%60comm_code%60%2C%0A%20%20%60comm_name%60%2C%0A%20%20%60year_of_construction%60%2C%0A%20%20%60land_use_designation%60%2C%0A%20%20%60property_type%60%2C%0A%20%20%60land_size_sm%60%2C%0A%20%20%60land_size_sf%60%2C%0A%20%20%60land_size_ac%60%2C%0A%20%20%60sub_property_use%60%2C%0A%20%20%60multipolygon%60"
    raw_data_path = "../data/raw/calgary_housing_raw.csv"
    success = fetch_all_data_with_pagination(api_url, limit=100000, raw_data_path=raw_data_path)
    if not success:
        print("Data fetching failed.")
