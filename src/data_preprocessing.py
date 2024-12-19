import pandas as pd

def clean_data(df):
    """Clean and preprocess raw data."""
    df = df.dropna()  # Drop missing values
    df = df.drop_duplicates()  # Remove duplicates

    # Handle outliers using the IQR method
    Q1 = df['price'].quantile(0.25)
    Q3 = df['price'].quantile(0.75)
    IQR = Q3 - Q1
    df = df[(df['price'] >= Q1 - 1.5 * IQR) & (df['price'] <= Q3 + 1.5 * IQR)]
    return df

def preprocess_and_save(input_path, output_path):
    """Load raw data, clean it, and save processed data."""
    df = pd.read_csv(input_path)
    df_cleaned = clean_data(df)
    df_cleaned.to_csv(output_path, index=False)

if __name__ == "__main__":
    raw_path = "data/raw/housing_data.csv"
    processed_path = "data/processed/cleaned_data.csv"
    preprocess_and_save(raw_path, processed_path)
