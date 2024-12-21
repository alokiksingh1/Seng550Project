# Seng550Project
Seng 550 project for predicting housing prices in Calgary

# Calgary Housing Price Prediction

This project predicts housing prices in Calgary using historical data with Pandas and PySpark.

## Folder Structure
- `data/`: Raw and processed datasets
- `src/`: Source code for pipeline components
- `scripts/`: Utility scripts to run the pipeline
- `config/`: Configuration files

## How to Run
1. Create virtual python environment with requirements.txt
2. Install dependencies:
   pip install -r requirements.txt
3. Run Pipeline:
   python pipeline.py

## Results
-  Model saved in models/
-  Data saved in data/

### README Update for `src` Folder and `pipeline.py`
The `src` folder contains all the core scripts required to execute the pipeline for downloading, processing, feature engineering, training, and evaluating models on the Calgary housing dataset. Each file within the `src` folder has a specific role to perform:

---

#### **Files in `src`**
1. **`pipeline.py`**:
   - The central script that orchestrates the entire process.
   - It automates the workflow for downloading raw data from the API, preprocessing it, performing feature engineering, and training and evaluating the machine learning model.

2. **`data_collection.py`**:
   - This script contains functions to fetch the raw housing dataset from an external API.
   - Handles API connections, ensures data integrity, and saves the downloaded data to the appropriate location.

3. **`data_preprocessing.py`**:
   - Responsible for cleaning and preparing the raw dataset.
   - Includes steps to handle missing data, remove duplicates, remove outliers, and create new columns required for downstream processes.

4. **`feature_engineering.py`**:
   - Performs feature engineering on the cleaned dataset.
   - Creates new derived features such as `price_per_sqft`, `property_age`, and `avg_comm_value`.
   - Prepares the dataset for machine learning by ensuring all required features are ready.

5. **`model_training.py`**:
   - Handles the training and evaluation of machine learning models.
   - Includes multiple regression models (Linear Regression, Decision Tree, and Random Forest).
   - Performs cross-validation for hyperparameter tuning and saves the best model for predictions.

---

#### **Explanation of `pipeline.py`**

The `pipeline.py` file is the main script that orchestrates the entire machine learning workflow. Below is a step-by-step breakdown of its functionalities:

1. **Downloading Raw Data**:
   - The pipeline begins by calling the `data_collection` module, which connects to an external API to fetch the raw Calgary housing dataset. We are limiting to 100,000 rows for the scope of this project, but it is scalable.
   - The raw data is saved locally in the `../data/raw/` directory.

2. **Data Preprocessing**:
   - The raw data is passed through the `data_preprocessing` module, where it undergoes cleaning and preparation.
   - Steps include handling missing values, dropping duplicates, removing outliers, and creating essential columns like `property_age`.

3. **Feature Engineering**:
   - The cleaned dataset is processed using the `feature_engineering` module.
   - New features such as `price_per_sqft` and `avg_comm_value` are generated, enhancing the dataset's predictive power.
   - The engineered dataset is saved in the `../data/engineered/` directory for reuse.

4. **Model Training and Evaluation**:
   - The processed dataset is fed into the `model_training` module.
   - Various regression models (Linear Regression, Decision Tree, and Random Forest) are trained and evaluated on the dataset.
   - Cross-validation is used to optimize the hyperparameters of the models.
   - Performance metrics such as RMSE and R² are calculated to assess model quality.

5. **Saving Results**:
   - The best-performing model is saved to the `../models/` directory for future use.
   - Predictions and evaluation metrics are saved in the `../data/predictions/` directory for analysis.

---

This modular design allows the pipeline to be easily extensible and maintainable. Each script performs a dedicated task, ensuring clarity and scalability of the overall project. The `pipeline.py` acts as a one-stop script to run the end-to-end workflow, making it user-friendly and efficient.