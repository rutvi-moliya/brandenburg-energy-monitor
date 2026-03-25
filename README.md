# ⚡ Brandenburg Energy Monitor

A real-time electricity generation and consumption dashboard for Germany, built with Python and deployed on AWS EC2.

**Live dashboard:** http://13.62.98.131:8501

---

## What it does

- Fetches live hourly electricity data from the SMARD API (Bundesnetzagentur)
- Detects statistical anomalies using rolling mean ± 2 standard deviations
- Generates daily AI-powered summaries using GPT-4o-mini
- Runs automated hourly data collection via APScheduler
- Displays everything in a real-time Streamlit dashboard with Plotly charts
- Allows one-click CSV export of any dataset

---

## Dashboard panels

| Panel | Description |
|-------|-------------|
| Live metric cards | Current wind, solar, and consumption with week-over-week delta |
| Energy mix chart | Hourly generation chart with anomaly markers |
| Data summary | Statistics and CSV export for each metric |
| Anomaly log | Table of all detected statistical anomalies |
| AI daily summary | GPT-4o-mini generated plain-English energy analysis |

---

## Tech stack

| Tool | Purpose |
|------|---------|
| Python 3.12 | Core language |
| Streamlit | Dashboard framework |
| Plotly | Interactive charts |
| Pandas | Data processing |
| SQLite | Local database |
| APScheduler | Automated hourly fetching |
| OpenAI GPT-4o-mini | Daily AI summaries |
| PyYAML | Configuration management |
| AWS EC2 | Cloud deployment |

---

## Data sources

| Source | Data | Licence |
|--------|------|---------|
| [SMARD](https://www.smard.de) (Bundesnetzagentur) | Live hourly national Germany generation + consumption | CC BY 4.0 |
| [SMARD Download Center](https://www.smard.de/home/downloadcenter/download-marktdaten/) | 2025 historical baseline CSV | CC BY 4.0 |

---

## Architecture

```
SMARD API (hourly)
      ↓
  fetcher.py  →  database.py (SQLite)
                      ↓
               anomaly.py (rolling mean ± 2σ)
                      ↓
               ai_summary.py (GPT-4o-mini)
                      ↓
                  app.py (Streamlit dashboard)
```

The scheduler runs `fetcher.py` → `anomaly.py` automatically every 60 minutes via APScheduler, which runs as a background thread inside the Streamlit process.

---

## Anomaly detection

Anomalies are detected using a statistical baseline built from one full year of 2025 SMARD data:

1. For each hour of the day (0–23), calculate the mean and standard deviation of historical values
2. Compare each live reading against `mean ± 2σ` for its hour
3. Values outside this range are flagged and logged

**Why 2σ?** 2 standard deviations captures ~95% of normal variation. Values outside this range occur by chance only ~5% of the time, making them genuinely worth flagging.

**Why group by hour?** Solar at 2pm is completely different from solar at 2am. Hour grouping ensures like-for-like comparison.

---

## Project structure

```
energy_monitor/
├── .env                  ← secrets (never committed)
├── .gitignore
├── config.yaml           ← all configuration values
├── requirements.txt      ← pinned dependencies
├── README.md
│
├── app.py                ← Streamlit dashboard (entry point)
├── fetcher.py            ← SMARD API calls
├── database.py           ← SQLite read/write operations
├── anomaly.py            ← rolling mean ± 2σ detection
├── ai_summary.py         ← OpenAI GPT-4o-mini integration
├── scheduler.py          ← APScheduler hourly pipeline
├── historical_loader.py  ← SMARD CSV loader
├── config_loader.py      ← config.yaml loader with validation
│
└── data/
    └── historical/       ← SMARD CSV files (not committed)
```

---

## Local setup

```bash
# Clone the repository
git clone https://github.com/yourusername/brandenburg-energy-monitor
cd brandenburg-energy-monitor

# Create virtual environment
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Create .env file
echo "OPENAI_API_KEY=your_key_here" > .env

# Download SMARD historical CSV files
# See data/historical/README instructions

# Load historical baseline
python historical_loader.py

# Run the dashboard
streamlit run app.py
```

---

## Deployment

Deployed on AWS EC2 (t3.micro, Ubuntu 24.04 LTS) as a systemd service.

The dashboard runs continuously at the public URL above. The APScheduler fetches new data every hour and generates a daily AI summary at 08:00 CET.

---

## Security and privacy

- No user data collected — reads public government data only
- No login system, no cookies, no tracking
- SQLite database lives on the EC2 instance — never uploaded or shared
- OpenAI receives only aggregated statistics (min, max, avg per metric) — never raw data
- All API keys stored in `.env` — never committed to version control

---

## Licence

Data: CC BY 4.0 (Bundesnetzagentur SMARD)
Code: MIT