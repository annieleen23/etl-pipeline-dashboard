import os
from datetime import datetime
from dotenv import load_dotenv
from src.extract.api_extractor import extract_weather_data, extract_github_trending
from src.transform.data_transformer import transform_weather_data, transform_github_data, validate_dataframe
from src.load.db_loader import load_weather_data, load_github_data, log_pipeline_run
from src.utils.logger import pipeline_logger

load_dotenv()


def run_weather_pipeline():
    """Run the weather ETL pipeline."""
    started_at = datetime.now()
    pipeline_logger.info("Starting weather ETL pipeline")
    records_loaded = 0
    status = "failed"

    try:
        api_key = os.getenv("WEATHER_API_KEY")
        raw_df = extract_weather_data(api_key=api_key)
        pipeline_logger.info(f"Extracted {len(raw_df)} weather records")

        transformed_df = transform_weather_data(raw_df)
        required = ["city", "temperature", "humidity"]
        if not validate_dataframe(transformed_df, required):
            raise ValueError("Weather data validation failed")

        records_loaded = load_weather_data(transformed_df)
        status = "success"
        pipeline_logger.info(f"Weather pipeline completed: {records_loaded} records loaded")

    except Exception as e:
        pipeline_logger.error(f"Weather pipeline failed: {e}")

    finally:
        log_pipeline_run("weather_pipeline", status, records_loaded, started_at)

    return records_loaded


def run_github_pipeline():
    """Run the GitHub trending ETL pipeline."""
    started_at = datetime.now()
    pipeline_logger.info("Starting GitHub trending ETL pipeline")
    records_loaded = 0
    status = "failed"

    try:
        raw_df = extract_github_trending()
        pipeline_logger.info(f"Extracted {len(raw_df)} GitHub records")

        transformed_df = transform_github_data(raw_df)
        required = ["repo_name", "stars"]
        if not validate_dataframe(transformed_df, required):
            raise ValueError("GitHub data validation failed")

        records_loaded = load_github_data(transformed_df)
        status = "success"
        pipeline_logger.info(f"GitHub pipeline completed: {records_loaded} records loaded")

    except Exception as e:
        pipeline_logger.error(f"GitHub pipeline failed: {e}")

    finally:
        log_pipeline_run("github_pipeline", status, records_loaded, started_at)

    return records_loaded


def run_all_pipelines():
    """Run all ETL pipelines."""
    pipeline_logger.info("=" * 50)
    pipeline_logger.info("Starting all ETL pipelines")
    pipeline_logger.info("=" * 50)

    weather_records = run_weather_pipeline()
    github_records = run_github_pipeline()

    total = weather_records + github_records
    pipeline_logger.info(f"All pipelines completed. Total records loaded: {total}")
    return total


if __name__ == "__main__":
    run_all_pipelines()
