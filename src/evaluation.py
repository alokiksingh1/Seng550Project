from sklearn.metrics import mean_squared_error

def evaluate_model(y_test, y_pred):
    """Evaluate the model and print RMSE."""
    rmse = mean_squared_error(y_test, y_pred, squared=False)
    print(f"Root Mean Squared Error (RMSE): {rmse}")
    return rmse
