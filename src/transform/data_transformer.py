import pandas as pd
from datetime import datetime
from src.utils.logger import pipeline_logger


def transform_weather_data(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and transform weather data."""
    if df.empty:
        pipeline_logger.warning("Empty DataFrame received for transformation")
        return df

    df = df.copy()
    df["temperature_f"] = (df["temperature"] * 9/5) + 32
    df["heat_index"] = df.apply(
        lambda r: "Hot" if r["temperature"] > 30
        else "Warm" if r["temperature"] > 20
        else "Cool" if r["temperature"] > 10
        else "Cold", axis=1
    )
    df["humidity_level"] = df["humidity"].apply(
        lambda h: "High" if h > 70 else "Medium" if h > 40 else "Low"
    )
    df["transformed_at"] = datetime.now()
    df = df.drop_duplicates(subset=["city"])
    df = df.dropna(subset=["temperature", "humidity"])

    pipeline_logger.info(f"Transformed {len(df)} weather records")
    return df


def transform_github_data(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and transform GitHub trending data."""
    if df.empty:
        pipeline_logger.warning("Empty DataFrame received for transformation")
        return df

    df = df.copy()
    df["popularity_tier"] = df["stars"].apply(
        lambda s: "Viral" if s > 10000
        else "Popular" if s > 1000
        else "Growing"
    )
    df["engagement_ratio"] = (df["forks"] / df["stars"].replace(0, 1)).round(3)
    df["transformed_at"] = datetime.now()
    df = df.drop_duplicates(subset=["repo_name"])
    df = df.dropna(subset=["repo_name", "stars"])

    pipeline_logger.info(f"Transformed {len(df)} GitHub records")
    return df


def validate_dataframe(df: pd.DataFrame, required_columns: list) -> bool:
    """Validate that DataFrame has required columns and is not empty."""
    if df.empty:
        pipeline_logger.error("Validation failed: DataFrame is empty")
        return False
    missing = [c for c in required_columns if c not in df.columns]
    if missing:
        pipeline_logger.error(f"Validation failed: missing columns {missing}")
        return False
    pipeline_logger.info(f"Validation passed: {len(df)} records, {len(df.columns)} columns")
    return True
