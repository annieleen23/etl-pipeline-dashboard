import streamlit as st
import plotly.express as px
import pandas as pd
from src.pipeline import run_all_pipelines, run_weather_pipeline, run_github_pipeline
from src.load.db_loader import query_weather_data, query_github_data, query_pipeline_runs

st.set_page_config(page_title="ETL Pipeline Dashboard", page_icon="📊", layout="wide")

st.title("📊 ETL Pipeline Dashboard")
st.markdown("Real-time data pipeline monitoring — Weather & GitHub Trending")

col1, col2, col3 = st.columns(3)
with col1:
    if st.button("▶ Run All Pipelines", type="primary"):
        with st.spinner("Running pipelines..."):
            total = run_all_pipelines()
        st.success(f"✅ Loaded {total} total records")
with col2:
    if st.button("🌤 Run Weather Pipeline"):
        with st.spinner("Extracting weather data..."):
            n = run_weather_pipeline()
        st.success(f"✅ Loaded {n} weather records")
with col3:
    if st.button("🐙 Run GitHub Pipeline"):
        with st.spinner("Extracting GitHub data..."):
            n = run_github_pipeline()
        st.success(f"✅ Loaded {n} GitHub records")

st.markdown("---")

tab1, tab2, tab3 = st.tabs(["🌤 Weather Data", "🐙 GitHub Trending", "📋 Pipeline Runs"])

with tab1:
    st.subheader("Weather Data")
    df = query_weather_data()
    if df.empty:
        st.info("No data yet. Click 'Run Weather Pipeline' to fetch data.")
    else:
        col1, col2, col3 = st.columns(3)
        col1.metric("Cities Tracked", df["city"].nunique())
        avg_temp = df["temperature"].mean()
        col2.metric("Avg Temperature (°C)", f"{avg_temp:.1f}")
        avg_humidity = df["humidity"].mean()
        col3.metric("Avg Humidity (%)", f"{avg_humidity:.0f}")

        fig = px.bar(df.drop_duplicates("city"), x="city", y="temperature",
                     color="heat_index", title="Temperature by City")
        st.plotly_chart(fig, use_container_width=True)
        st.dataframe(df[["city","temperature","humidity","weather","heat_index","humidity_level"]].drop_duplicates("city"))

with tab2:
    st.subheader("GitHub Trending Repositories")
    df = query_github_data()
    if df.empty:
        st.info("No data yet. Click 'Run GitHub Pipeline' to fetch data.")
    else:
        col1, col2, col3 = st.columns(3)
        col1.metric("Repos Tracked", len(df))
        col2.metric("Total Stars", f"{df['stars'].sum():,}")
        col3.metric("Avg Stars", f"{df['stars'].mean():.0f}")

        fig = px.bar(df.head(10), x="stars", y="repo_name",
                     orientation="h", color="popularity_tier",
                     title="Top 10 Trending Repos by Stars")
        st.plotly_chart(fig, use_container_width=True)
        st.dataframe(df[["repo_name","stars","forks","language","popularity_tier"]].head(20))

with tab3:
    st.subheader("Pipeline Run History")
    df = query_pipeline_runs()
    if df.empty:
        st.info("No pipeline runs yet.")
    else:
        success = len(df[df["status"] == "success"])
        failed = len(df[df["status"] == "failed"])
        col1, col2, col3 = st.columns(3)
        col1.metric("Total Runs", len(df))
        col2.metric("Successful", success)
        col3.metric("Failed", failed)
        st.dataframe(df)
