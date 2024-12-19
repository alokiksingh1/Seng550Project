from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql import functions as F

def train_and_evaluate_model(spark, engineered_data_path, model_type="linear"):
    """
    Train and evaluate a regression model (Linear Regression or Random Forest).

    Args:
        spark: SparkSession object.
        engineered_data_path: Path to the engineered data CSV.
        model_type: Model to use ("linear" or "random_forest").

    Returns:
        None
    """
    # Load the engineered data
    df = spark.read.csv(engineered_data_path, header=True, inferSchema=True)

    # Assemble features into a single vector
    feature_columns = ["property_age", "assessed_value_normalized", "assessment_class_encoded"]
    assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
    df = assembler.transform(df)

    # Select features and label
    df = df.select("features", F.col("assessed_value").alias("label"))

    # Split data into training and testing sets (80/20 split)
    train_data, test_data = df.randomSplit([0.8, 0.2], seed=42)

    # Train the model
    if model_type == "linear":
        model = LinearRegression(featuresCol="features", labelCol="label")
    elif model_type == "random_forest":
        raise NotImplementedError("Random Forest model is not implemented in this function.")
    else:
        raise ValueError("Invalid model type. Choose 'linear' or 'random_forest'.")

    trained_model = model.fit(train_data)

    # Make predictions on the test data
    predictions = trained_model.transform(test_data)

    # Evaluate the model
    evaluator = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="rmse")
    rmse = evaluator.evaluate(predictions)
    print(f"Root Mean Squared Error (RMSE): {rmse}")

    # Save predictions for further analysis
    predictions.select("prediction", "label").write.csv("../data/predictions/predictions.csv", header=True, mode="overwrite")

    # Print model coefficients (for Linear Regression only)
    if model_type == "linear":
        print(f"Coefficients: {trained_model.coefficients}")
        print(f"Intercept: {trained_model.intercept}")

    print("Model training and evaluation complete.")
