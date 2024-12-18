from pyspark.ml.regression import LinearRegression

def train_model(df, features_col, label_col):
    # Train a Linear Regression model.
    lr = LinearRegression(featuresCol=features_col, labelCol=label_col)
    model = lr.fit(df)
    return model
