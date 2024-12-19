from sklearn.metrics import mean_squared_error
from sklearn.model_selection import cross_val_score

def evaluate_model(y_test, y_pred):
    """Evaluate the model with RMSE."""
    rmse = mean_squared_error(y_test, y_pred, squared=False)
    print(f"Root Mean Squared Error (RMSE): {rmse}")
    return rmse

def cross_validate_model(model, X, y, cv=5):
    """Perform cross-validation."""
    scores = cross_val_score(model, X, y, cv=cv, scoring='neg_root_mean_squared_error')
    print(f"Cross-Validation RMSE: {abs(scores.mean())}")
    return abs(scores.mean())
