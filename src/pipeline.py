import yaml
import pandas as pd
from sklearn.model_selection import train_test_split
from src.data_preprocessing import clean_data
from src.feature_engineering import create_feature_pipeline
from src.model_training import train_model
from src.evaluation import evaluate_model

def main():
    # Load configuration
    with open("config/config.yaml", "r") as file:
        config = yaml.safe_load(file)

    # Load and clean data
    raw_path = config["data"]["raw_path"]
    processed_path = config["data"]["processed_path"]
    df = pd.read_csv(raw_path)
    df_cleaned = clean_data(df)
    df_cleaned.to_csv(processed_path, index=False)

    # Feature engineering
    numerical_cols = ['sqft', 'bedrooms', 'bathrooms']
    categorical_cols = ['neighborhood']
    preprocessor = create_feature_pipeline(numerical_cols, categorical_cols)

    X = df_cleaned[numerical_cols + categorical_cols]
    y = df_cleaned[config["pipeline"]["label_col"]]

    X_preprocessed = preprocessor.fit_transform(X)

    # Split data
    X_train, X_test, y_train, y_test = train_test_split(X_preprocessed, y, test_size=0.2, random_state=42)

    # Train model
    model = train_model(X_train, y_train)

    # Evaluate model
    y_pred = model.predict(X_test)
    evaluate_model(y_test, y_pred)

if __name__ == "__main__":
    main()
