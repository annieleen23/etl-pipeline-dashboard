import pytest
import pandas as pd
from src.extract.api_extractor import _mock_weather_data, extract_github_trending
from src.transform.data_transformer import transform_weather_data, transform_github_data, validate_dataframe
from src.load.db_loader import create_tables, load_weather_data, query_weather_data


def test_mock_weather_extraction():
    df = _mock_weather_data("San Francisco")
    assert not df.empty
    assert "city" in df.columns
    assert "temperature" in df.columns
    assert len(df) > 0


def test_weather_transformation():
    df = _mock_weather_data("San Francisco")
    transformed = transform_weather_data(df)
    assert "temperature_f" in transformed.columns
    assert "heat_index" in transformed.columns
    assert "humidity_level" in transformed.columns


def test_validation_passes():
    df = pd.DataFrame([{"city": "SF", "temperature": 20, "humidity": 60}])
    assert validate_dataframe(df, ["city", "temperature", "humidity"]) is True


def test_validation_fails_empty():
    df = pd.DataFrame()
    assert validate_dataframe(df, ["city"]) is False


def test_validation_fails_missing_columns():
    df = pd.DataFrame([{"city": "SF"}])
    assert validate_dataframe(df, ["city", "temperature"]) is False


def test_load_and_query_weather():
    df = _mock_weather_data("Test City")
    transformed = transform_weather_data(df)
    records = load_weather_data(transformed)
    assert records > 0
    result = query_weather_data()
    assert not result.empty
