import pandas as pd
import matplotlib.pyplot as plt
from prefect import task, flow, get_run_logger

DATA_PATH = "analytics_data.csv"
SUMMARY_PATH = "analytics_summary.csv"


@task
def read_csv(file_path: str) -> pd.DataFrame:
    """Reads CSV file into a Pandas DataFrame."""
    logger = get_run_logger()
    logger.info(f"Reading data from {file_path}...")
    df = pd.read_csv(file_path)
    logger.info(f"Data shape: {df.shape}")
    return df

@task
def validate_data(df: pd.DataFrame) -> pd.DataFrame:
    """Validates data by handling missing values."""
    logger = get_run_logger()
    missing_values = df.isnull().sum()
    logger.info(f"Missing values:\n{missing_values}")
    df_clean = df.dropna()
    logger.info(f"Data shape after cleaning: {df_clean.shape}")
    return df_clean

@task
def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    """Transforms data by normalizing the 'sales' column if it exists."""
    logger = get_run_logger()
    if "sales" in df.columns:
        df["sales_normalized"] = (df["sales"] - df["sales"].mean()) / df["sales"].std()
        logger.info("Sales column normalized.")
    return df

@task
def generate_summary(df: pd.DataFrame, output_path: str):
    """Generates and saves summary statistics."""
    logger = get_run_logger()
    summary = df.describe()
    summary.to_csv(output_path)
    logger.info(f"Summary statistics saved to {output_path}")

@task
def generate_histogram(df: pd.DataFrame):
    """Generates and saves a histogram of sales distribution."""
    logger = get_run_logger()
    if "sales" in df.columns:
        plt.hist(df["sales"], bins=20)
        plt.title("Sales Distribution")
        plt.xlabel("Sales")
        plt.ylabel("Frequency")
        plt.savefig("Histogram pic")
        plt.close()
        logger.info(f"Sales histogram saved to Histogram pic")

@flow
def analytics_pipeline():
    """Main Prefect pipeline to orchestrate tasks."""
    logger = get_run_logger()
    logger.info("Starting analytics pipeline...")

    df = read_csv(DATA_PATH)
    df_clean = validate_data(df)
    df_transformed = transform_data(df_clean)
    generate_summary(df_transformed, SUMMARY_PATH)
    generate_histogram(df_transformed)

    logger.info("Analytics pipeline completed successfully.")

# Run the pipeline
if __name__ == "__main__":
    analytics_pipeline()