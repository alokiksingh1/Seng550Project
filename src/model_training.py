from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator
import os


def train_and_evaluate_model(spark, engineered_data_path, model_output_path, predictions_output_path):
    """
    Train and evaluate a regression model to predict the re-assessed value of a property.

    Args:
        spark: SparkSession object.
        engineered_data_path: Path to the feature-engineered data.
        model_output_path: Path to save the trained model.
        predictions_output_path: Path to save the predictions.
    """
    # Step 1: Load the feature-engineered data
    print("Loading feature-engineered data...")
    df = spark.read.csv(engineered_data_path, header=True, inferSchema=True)

    feature_columns = ["assessed_value", "land_size_sf", "property_age", "price_per_sqft", "avg_comm_value"]
    label_column = "re_assessed_value"
    missing_columns = [col for col in feature_columns + [label_column] if col not in df.columns]
    if missing_columns:
        print(f"Missing required columns for model training: {missing_columns}")
        return

    # Step 2: Prepare data
    print("Vectorizing features...")
    assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
    df = assembler.transform(df)

    # Split data into training and testing
    print("Splitting data...")
    train_df, test_df = df.randomSplit([0.8, 0.2], seed=42) # 80/20 split

    # Step 3: Train the model
    print("Training the model...")
    lr = LinearRegression(featuresCol="features", labelCol=label_column, maxIter=100, regParam=0.3, elasticNetParam=0.8)
    model = lr.fit(train_df)

    # Step 4: Evaluate the model
    print("Evaluating the model on test data...")
    predictions = model.transform(test_df)

    evaluator = RegressionEvaluator(labelCol=label_column, predictionCol="prediction", metricName="rmse")
    rmse = evaluator.evaluate(predictions)
    print(f"Root Mean Squared Error (RMSE) on test data: {rmse}")

    r2 = evaluator.evaluate(predictions, {evaluator.metricName: "r2"})
    print(f"R2 Score on test data: {r2}")

    # Step 5: Save the model
    print("Saving the model...")
    os.makedirs(model_output_path, exist_ok=True)
    model.write().overwrite().save(model_output_path) 

    # Save predictions
    print("Saving predictions...")
    # Convert the `features` column to a string or drop it before saving
    predictions_to_save = predictions.withColumn("features", predictions["features"].cast("string"))

    # Save the predictions without the complex `features` column
    predictions_to_save.select("features", "re_assessed_value", "prediction").write.csv(
        predictions_output_path, header=True, mode="overwrite"
    )
    print(f"Predictions saved to {predictions_output_path}")


    print("Model training and evaluation completed.")
