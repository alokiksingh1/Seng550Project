import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os

def load_data(file_path):
    """Load data for EDA."""
    return pd.read_csv(file_path)

def plot_distributions(df, numeric_columns):
    """Plot distributions for numeric columns."""
    for column in numeric_columns:
        plt.figure(figsize=(10, 5))
        sns.histplot(df[column], kde=True, bins=30)
        plt.title(f"Distribution of {column}")
        plt.savefig(f"../reports/figures/{column}_distribution.png")
        plt.close()

def plot_correlations(df):
    """Plot correlation heatmap."""
    plt.figure(figsize=(12, 8))
    sns.heatmap(df.corr(), annot=True, cmap="coolwarm", fmt=".2f")
    plt.title("Correlation Heatmap")
    plt.savefig("../reports/figures/correlation_heatmap.png")
    plt.close()

def boxplot_features(df, column_list, target_column):
    """Create boxplots for categorical features."""
    for column in column_list:
        plt.figure(figsize=(10, 5))
        sns.boxplot(x=column, y=target_column, data=df)
        plt.title(f"{target_column} vs {column}")
        plt.savefig(f"../reports/figures/{column}_boxplot.png")
        plt.close()

def scatter_plots(df, numeric_columns, target_column):
    """Create scatter plots for numeric features."""
    for column in numeric_columns:
        plt.figure(figsize=(10, 5))
        sns.scatterplot(x=column, y=target_column, data=df)
        plt.title(f"{target_column} vs {column}")
        plt.savefig(f"../reports/figures/{column}_scatter.png")
        plt.close()

def eda_pipeline(data_path, target_column):
    """Run the EDA pipeline."""
    df = load_data(data_path)

    numeric_columns = df.select_dtypes(include=["float64", "int64"]).columns.tolist()
    categorical_columns = df.select_dtypes(include=["object"]).columns.tolist()

    plot_distributions(df, numeric_columns)
    plot_correlations(df)
    boxplot_features(df, categorical_columns, target_column)
    scatter_plots(df, numeric_columns, target_column)
