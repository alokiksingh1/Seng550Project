import requests
import pandas as pd

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
        df = pd.DataFrame(all_data)
        df.to_csv(raw_data_path, index=False)
        print(f"Total rows fetched: {len(df)}")
        print(f"All data saved to {raw_data_path}")
        return True
    else:
        print("No data fetched.")
        return False




if __name__ == "__main__":
    api_url = "https://data.calgary.ca/resource/4ur7-wsgc.json"
    raw_data_path = "../data/raw/calgary_housing_raw.csv"
    success = fetch_all_data_with_pagination(api_url, limit=1000, total_limit=100000, raw_data_path=raw_data_path)
    if not success:
        print("Data fetching failed.")
