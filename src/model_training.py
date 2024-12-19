from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator


def train_and_evaluate_model(spark, input_path: str, model_type="linear"):
    """
    Train and evaluate a regression model using the input dataset.
    """
    # Load the feature-engineered data
    df = spark.read.csv(input_path, header=True, inferSchema=True)

    # Ensure all features are numeric
    features = ["property_age", "assessed_value_normalized", "land_size_sm", "land_size_sf", "land_size_ac"]
    df = df.select(*[col for col in features], "target_column")

    # Assemble features
    assembler = VectorAssembler(inputCols=features, outputCol="features")
    df = assembler.transform(df).select("features", "target_column")

    # Split into training and testing data
    train_data, test_data = df.randomSplit([0.8, 0.2], seed=42)

    # Train the model
    if model_type == "linear":
        model = LinearRegression(featuresCol="features", labelCol="target_column")
    else:
        raise ValueError("Unsupported model type")

    trained_model = model.fit(train_data)

    # Evaluate the model
    predictions = trained_model.transform(test_data)
    evaluator = RegressionEvaluator(labelCol="target_column", predictionCol="prediction", metricName="rmse")
    rmse = evaluator.evaluate(predictions)

    print(f"Root Mean Squared Error (RMSE): {rmse}")
