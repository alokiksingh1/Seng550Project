import yaml
from pyspark.sql import SparkSession
from src.data_preprocessing import clean_data
from src.feature_engineering import create_feature_pipeline
from src.model_training import train_model
from src.evaluation import evaluate_model

def main():
    # Load configurations
    with open("config/config.yaml", "r") as file:
        config = yaml.safe_load(file)

    # Initialize Spark session
    spark = SparkSession.builder \
        .appName(config["app_name"]) \
        .getOrCreate()
    spark.sparkContext.setLogLevel(config["log_level"])

    # Load raw data
    raw_data = spark.read.csv(config["data"]["raw_path"], header=True, inferSchema=True)

    # Preprocess data
    cleaned_data = clean_data(raw_data)

    # Feature engineering
    feature_pipeline = create_feature_pipeline(["neighborhood", "sqft", "bedrooms", "bathrooms"], "features")
    processed_data = feature_pipeline.fit(cleaned_data).transform(cleaned_data)

    # Train-test split
    train_data, test_data = processed_data.randomSplit([config["pipeline"]["train_test_split"], 1 - config["pipeline"]["train_test_split"]])

    # Train model
    model = train_model(train_data, features_col="features", label_col=config["pipeline"]["label_col"])

    # Evaluate model
    predictions = model.transform(test_data)
    evaluate_model(predictions)

    # Save model
    model.save(config["model"]["save_path"])

if __name__ == "__main__":
    main()
