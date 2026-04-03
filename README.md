# 📊 ETL Pipeline Dashboard

> Real-time data pipeline monitoring with automated ETL workflows, built with Python and Streamlit.

![Python](https://img.shields.io/badge/Python-3.10+-blue?style=flat-square&logo=python)
![Streamlit](https://img.shields.io/badge/Streamlit-1.28-red?style=flat-square&logo=streamlit)
![SQLite](https://img.shields.io/badge/SQLite-Database-green?style=flat-square&logo=sqlite)
![Docker](https://img.shields.io/badge/Docker-Ready-blue?style=flat-square&logo=docker)

---

## 🏗️ Architecture
```
┌─────────────────────────────────────────────────────────────┐
│                     ETL Pipeline                            │
│                                                             │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐  │
│  │   EXTRACT    │───▶│  TRANSFORM   │───▶│     LOAD     │  │
│  │              │    │              │    │              │  │
│  │ • Weather API│    │ • Clean data │    │ • SQLite DB  │  │
│  │ • GitHub API │    │ • Validate   │    │ • Log runs   │  │
│  │ • Mock data  │    │ • Enrich     │    │ • Query API  │  │
│  └──────────────┘    └──────────────┘    └──────────────┘  │
│                                                             │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                  Streamlit Dashboard                        │
│                                                             │
│   🌤 Weather Tab    │  🐙 GitHub Tab   │  📋 Pipeline Tab  │
│   • City metrics   │  • Trending repos│  • Run history    │
│   • Temp charts    │  • Stars ranking │  • Success rate   │
│   • Humidity data  │  • Language dist │  • Record counts  │
└─────────────────────────────────────────────────────────────┘
```

---

## ✨ Features

- **Automated ETL Pipelines** — Extract data from Weather API and GitHub API, transform and load into SQLite
- **Real-time Dashboard** — Interactive Streamlit UI with Plotly charts and live metrics
- **Data Validation** — Schema checks, null validation, and business rule enforcement at every pipeline stage
- **Pipeline Monitoring** — Full run history with status tracking, record counts, and timestamps
- **Fault Tolerant** — Graceful fallback to mock data when APIs are unavailable
- **Dockerized** — Ready for containerized deployment

---

## 🔧 Tech Stack

| Layer | Technology |
|-------|-----------|
| Language | Python 3.10+ |
| Dashboard | Streamlit + Plotly |
| Database | SQLite (PostgreSQL-ready) |
| Data Processing | Pandas |
| HTTP Client | Requests |
| Containerization | Docker |
| Testing | Pytest |

---

## 📁 Project Structure
```
etl-pipeline-dashboard/
├── src/
│   ├── extract/
│   │   └── api_extractor.py      # Weather & GitHub API extraction
│   ├── transform/
│   │   └── data_transformer.py   # Data cleaning & enrichment
│   ├── load/
│   │   └── db_loader.py          # SQLite persistence layer
│   ├── utils/
│   │   └── logger.py             # Structured logging
│   └── pipeline.py               # Orchestration entry point
├── tests/
│   └── test_pipeline.py          # Unit tests
├── dashboard.py                   # Streamlit dashboard
├── requirements.txt
└── docker-compose.yml
```

---

## 🚀 Getting Started

### Prerequisites
- Python 3.10+
- pip

### Installation
```bash
git clone https://github.com/annieleen23/etl-pipeline-dashboard
cd etl-pipeline-dashboard
pip install -r requirements.txt
```

### Run the Pipeline
```bash
PYTHONPATH=. python3 src/pipeline.py
```

### Launch the Dashboard
```bash
PYTHONPATH=. streamlit run dashboard.py
```

Open [http://localhost:8501](http://localhost:8501) in your browser.

---

## 📊 Pipeline Metrics

| Pipeline | Data Source | Records/Run | Avg Latency |
|----------|------------|-------------|-------------|
| Weather | OpenWeatherMap API | 5 cities | ~200ms |
| GitHub Trending | GitHub REST API | 20 repos | ~1s |

---

## 🧪 Running Tests
```bash
PYTHONPATH=. pytest tests/ -v
```

---

## 🔑 Key Engineering Decisions

- **Layered ETL Architecture** — Strict separation of Extract, Transform, Load concerns for maintainability and testability
- **Idempotent Operations** — Pipeline can safely re-run without creating duplicate records
- **Validation at Every Stage** — Schema and business rule checks prevent bad data from propagating downstream
- **Mock Data Fallback** — System remains functional for development and testing without live API keys
