from pyspark.ml.evaluation import RegressionEvaluator

def evaluate_model(predictions, label_col="price", prediction_col="prediction"):
    # Evaluate the model performance.
    evaluator = RegressionEvaluator(labelCol=label_col, predictionCol=prediction_col, metricName="rmse")
    rmse = evaluator.evaluate(predictions)
    print(f"Root Mean Squared Error (RMSE): {rmse}")
    return rmse
