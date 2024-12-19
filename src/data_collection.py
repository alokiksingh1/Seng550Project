from sodapy import Socrata
import pandas as pd

def fetch_data(dataset_id, limit=10000):
    """Fetch data from the Calgary Open Data Portal."""
    client = Socrata("data.calgary.ca", None)
    results = client.get(dataset_id, limit=limit)
    df = pd.DataFrame.from_records(results)
    return df

def save_data(df, path):
    """Save data to a CSV file."""
    df.to_csv(path, index=False)

if __name__ == "__main__":
    dataset_id = "your_dataset_id"  # Replace with actual dataset ID
    raw_path = "data/raw/housing_data.csv"
    data = fetch_data(dataset_id)
    save_data(data, raw_path)
