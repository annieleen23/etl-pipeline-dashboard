import requests
import pandas as pd
from datetime import datetime
from src.utils.logger import pipeline_logger


def extract_weather_data(city: str = "San Francisco", api_key: str = None) -> pd.DataFrame:
    """Extract weather data from OpenWeatherMap API."""
    if not api_key:
        pipeline_logger.warning("No API key provided, using mock data")
        return _mock_weather_data(city)
    
    url = f"http://api.openweathermap.org/data/2.5/weather"
    params = {"q": city, "appid": api_key, "units": "metric"}
    
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        return pd.DataFrame([{
            "city": city,
            "temperature": data["main"]["temp"],
            "humidity": data["main"]["humidity"],
            "pressure": data["main"]["pressure"],
            "weather": data["weather"][0]["description"],
            "wind_speed": data["wind"]["speed"],
            "extracted_at": datetime.now()
        }])
    except Exception as e:
        pipeline_logger.error(f"Failed to extract weather data: {e}")
        return _mock_weather_data(city)


def _mock_weather_data(city: str) -> pd.DataFrame:
    """Generate mock weather data for testing."""
    import random
    cities = [city, "New York", "Chicago", "Los Angeles", "Seattle"]
    records = []
    for c in cities:
        records.append({
            "city": c,
            "temperature": round(random.uniform(5, 35), 2),
            "humidity": random.randint(30, 90),
            "pressure": random.randint(1000, 1020),
            "weather": random.choice(["clear sky", "cloudy", "light rain", "sunny"]),
            "wind_speed": round(random.uniform(1, 20), 2),
            "extracted_at": datetime.now()
        })
    pipeline_logger.info(f"Generated mock data for {len(records)} cities")
    return pd.DataFrame(records)


def extract_github_trending() -> pd.DataFrame:
    """Extract trending repositories from GitHub API."""
    url = "https://api.github.com/search/repositories"
    params = {
        "q": "language:python created:>2024-01-01",
        "sort": "stars",
        "order": "desc",
        "per_page": 20
    }
    
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        items = response.json().get("items", [])
        
        records = [{
            "repo_name": item["full_name"],
            "stars": item["stargazers_count"],
            "forks": item["forks_count"],
            "language": item.get("language", "Unknown"),
            "description": (item.get("description") or "")[:200],
            "extracted_at": datetime.now()
        } for item in items]
        
        pipeline_logger.info(f"Extracted {len(records)} trending repositories")
        return pd.DataFrame(records)
    except Exception as e:
        pipeline_logger.error(f"Failed to extract GitHub data: {e}")
        return pd.DataFrame()
