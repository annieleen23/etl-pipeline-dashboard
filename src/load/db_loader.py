import pandas as pd
import sqlite3
import os
from datetime import datetime
from src.utils.logger import pipeline_logger


DB_PATH = "etl_pipeline.db"


def get_connection():
    """Get SQLite connection (fallback when PostgreSQL not available)."""
    return sqlite3.connect(DB_PATH)


def create_tables():
    """Create tables if they do not exist."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS weather_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            city TEXT NOT NULL,
            temperature REAL,
            temperature_f REAL,
            humidity INTEGER,
            pressure INTEGER,
            weather TEXT,
            wind_speed REAL,
            heat_index TEXT,
            humidity_level TEXT,
            extracted_at TIMESTAMP,
            transformed_at TIMESTAMP,
            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS github_trending (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            repo_name TEXT NOT NULL,
            stars INTEGER,
            forks INTEGER,
            language TEXT,
            description TEXT,
            popularity_tier TEXT,
            engagement_ratio REAL,
            extracted_at TIMESTAMP,
            transformed_at TIMESTAMP,
            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS pipeline_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pipeline_name TEXT NOT NULL,
            status TEXT NOT NULL,
            records_processed INTEGER,
            started_at TIMESTAMP,
            completed_at TIMESTAMP
        )
    """)
    conn.commit()
    conn.close()
    pipeline_logger.info("Database tables created/verified")


def load_weather_data(df: pd.DataFrame) -> int:
    """Load weather data into database."""
    if df.empty:
        return 0
    create_tables()
    conn = get_connection()
    df["loaded_at"] = datetime.now()
    df.to_sql("weather_data", conn, if_exists="append", index=False)
    conn.close()
    pipeline_logger.info(f"Loaded {len(df)} weather records to database")
    return len(df)


def load_github_data(df: pd.DataFrame) -> int:
    """Load GitHub trending data into database."""
    if df.empty:
        return 0
    create_tables()
    conn = get_connection()
    df["loaded_at"] = datetime.now()
    df.to_sql("github_trending", conn, if_exists="append", index=False)
    conn.close()
    pipeline_logger.info(f"Loaded {len(df)} GitHub records to database")
    return len(df)


def log_pipeline_run(name: str, status: str, records: int, started_at: datetime):
    """Log pipeline run metadata."""
    create_tables()
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO pipeline_runs (pipeline_name, status, records_processed, started_at, completed_at)
        VALUES (?, ?, ?, ?, ?)
    """, (name, status, records, started_at, datetime.now()))
    conn.commit()
    conn.close()


def query_weather_data() -> pd.DataFrame:
    """Query latest weather data."""
    create_tables()
    conn = get_connection()
    df = pd.read_sql("SELECT * FROM weather_data ORDER BY loaded_at DESC LIMIT 100", conn)
    conn.close()
    return df


def query_github_data() -> pd.DataFrame:
    """Query latest GitHub trending data."""
    create_tables()
    conn = get_connection()
    df = pd.read_sql("SELECT * FROM github_trending ORDER BY stars DESC LIMIT 50", conn)
    conn.close()
    return df


def query_pipeline_runs() -> pd.DataFrame:
    """Query pipeline run history."""
    create_tables()
    conn = get_connection()
    df = pd.read_sql("SELECT * FROM pipeline_runs ORDER BY started_at DESC LIMIT 20", conn)
    conn.close()
    return df
