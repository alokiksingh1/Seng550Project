import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import glob
import os

# Paths
PROCESSED_DATA_PATH = "../data/processed/calgary_housing_cleaned/"
FIGURES_PATH = "../reports/figures"

# Ensure the directory for saving plots exists
os.makedirs(FIGURES_PATH, exist_ok=True)

def load_spark_output(input_path):
    """Load Spark output files into a single pandas DataFrame."""
    all_files = glob.glob(f"{input_path}/part-*")
    df_list = [pd.read_csv(file) for file in all_files]
    df = pd.concat(df_list, ignore_index=True)
    print(f"Loaded data with {len(df)} rows and {len(df.columns)} columns.")
    return df

def save_plot(plt, filename):
    """Save the plot to the figures directory."""
    plot_path = os.path.join(FIGURES_PATH, filename)
    plt.savefig(plot_path)
    print(f"Plot saved: {plot_path}")
    plt.close()

def plot_distribution(df, column):
    """Plot the distribution of a numeric column."""
    plt.figure(figsize=(8, 6))
    sns.histplot(df[column], kde=True, bins=30)
    plt.title(f"Distribution of {column}")
    plt.xlabel(column)
    plt.ylabel("Frequency")
    save_plot(plt, f"{column}_distribution.png")

def plot_average_assessed_value_by_year(df):
    """Plot average assessed value by year."""
    avg_values = df.groupby("roll_year")["assessed_value"].mean().reset_index()
    plt.figure(figsize=(10, 6))
    sns.lineplot(data=avg_values, x="roll_year", y="assessed_value", marker="o")
    plt.title("Average Assessed Value by Year")
    plt.xlabel("Year")
    plt.ylabel("Average Assessed Value")
    save_plot(plt, "average_assessed_value_by_year.png")

def plot_average_assessed_value_by_property_type(df):
    """Plot average assessed value by property type."""
    avg_values = df.groupby("property_type")["assessed_value"].mean().reset_index()
    plt.figure(figsize=(12, 6))
    sns.barplot(data=avg_values, x="property_type", y="assessed_value")
    plt.title("Average Assessed Value by Property Type")
    plt.xlabel("Property Type")
    plt.ylabel("Average Assessed Value")
    plt.xticks(rotation=45)
    save_plot(plt, "average_assessed_value_by_property_type.png")

def plot_correlation_heatmap(df):
    """Plot a correlation heatmap for numeric columns."""
    numeric_df = df.select_dtypes(include=["number"])
    plt.figure(figsize=(10, 8))
    correlation_matrix = numeric_df.corr()
    sns.heatmap(correlation_matrix, annot=True, cmap="coolwarm", fmt=".2f")
    plt.title("Correlation Heatmap")
    save_plot(plt, "correlation_heatmap.png")

def scatter_plot(df, x_col, y_col):
    """Scatter plot for two variables."""
    plt.figure(figsize=(8, 6))
    sns.scatterplot(data=df, x=x_col, y=y_col)
    plt.title(f"{y_col} vs {x_col}")
    plt.xlabel(x_col)
    plt.ylabel(y_col)
    save_plot(plt, f"{y_col}_vs_{x_col}.png")

def boxplot_assessed_value_by_year(df):
    """Boxplot showing the distribution of assessed values by year."""
    plt.figure(figsize=(12, 6))
    sns.boxplot(data=df, x="roll_year", y="assessed_value")
    plt.title("Assessed Value Distribution by Year")
    plt.xlabel("Year")
    plt.ylabel("Assessed Value")
    save_plot(plt, "assessed_value_by_year_boxplot.png")

if __name__ == "__main__":
    # Load data from processed folder
    df = load_spark_output(PROCESSED_DATA_PATH)

    # Perform EDA and generate plots
    plot_distribution(df, "assessed_value")
    plot_average_assessed_value_by_year(df)
    plot_average_assessed_value_by_property_type(df)
    plot_correlation_heatmap(df)
    scatter_plot(df, x_col="land_size_sm", y_col="assessed_value")
    boxplot_assessed_value_by_year(df)
