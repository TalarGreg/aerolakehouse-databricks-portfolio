# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze — OpenWeatherMap Ingest
# MAGIC Fetches current weather + 5-day/3h forecast for Polish airports
# MAGIC using OpenWeatherMap API 2.5 (free tier). API key stored in Databricks Secret Scope.
# MAGIC - **Job schedule:** every 20 min
# MAGIC - **Endpoints:** /data/2.5/weather (current) + /data/2.5/forecast (5-day/3h)
# MAGIC - **Output:** `/Volumes/{catalog}/bronze/raw_files/open_weather_map/airport=<IATA>/year=/month=/day=/hour=/`

# COMMAND ----------

import requests
import json
import time
import logging
from datetime import datetime, timezone

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("bronze.open_weather_map")

# COMMAND ----------

dbutils.widgets.text("catalog", "dev_alh")
catalog = dbutils.widgets.get("catalog")

# Secret scope is environment-specific — dev catalog uses aerolakehouse-dev,
# prod catalog uses aerolakehouse-prod. Derived from catalog prefix.
_env          = "dev" if catalog.startswith("dev") else "prod"
_secret_scope = f"aerolakehouse-{_env}"

# API key from Databricks Secret Scope — never hardcode
api_key = dbutils.secrets.get(scope=_secret_scope, key="openweathermap-api-key")

# COMMAND ----------

URL_CURRENT  = "https://api.openweathermap.org/data/2.5/weather"
URL_FORECAST = "https://api.openweathermap.org/data/2.5/forecast"

# Airports to ingest — add entries to expand coverage
AIRPORTS = {
    "WAW": {"latitude": 52.1657, "longitude": 20.9671},
    "GDN": {"latitude": 54.3776, "longitude": 18.4662},
}

MAX_RETRIES    = 3
RETRY_STATUSES = {502, 503}
BACKOFF_BASE   = 2  # seconds — exponential: 2, 4, 8

# COMMAND ----------

def fetch_owm(url: str, params: dict) -> dict:
    """Fetch OpenWeatherMap API with retry logic for 429 / 5xx errors."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(url, params=params, timeout=30)
        except requests.Timeout:
            log.warning(f"attempt {attempt}/{MAX_RETRIES} — request timed out")
            if attempt == MAX_RETRIES:
                raise RuntimeError("OpenWeatherMap request timed out") from None
            time.sleep(BACKOFF_BASE ** attempt)
            continue
        except requests.ConnectionError:
            log.warning(f"attempt {attempt}/{MAX_RETRIES} — connection error")
            if attempt == MAX_RETRIES:
                raise RuntimeError("OpenWeatherMap connection failed") from None
            time.sleep(BACKOFF_BASE ** attempt)
            continue

        if r.status_code == 429:
            wait = int(r.headers.get("Retry-After", BACKOFF_BASE ** attempt))
            log.warning(f"429 rate limited — waiting {wait}s (attempt {attempt}/{MAX_RETRIES})")
            if attempt == MAX_RETRIES:
                raise RuntimeError("OpenWeatherMap request failed with HTTP 429")
            time.sleep(wait)
            continue

        if r.status_code in RETRY_STATUSES:
            wait = BACKOFF_BASE ** attempt
            log.warning(f"{r.status_code} server error — waiting {wait}s (attempt {attempt}/{MAX_RETRIES})")
            if attempt == MAX_RETRIES:
                raise RuntimeError(
                    f"OpenWeatherMap request failed with HTTP {r.status_code}"
                )
            time.sleep(wait)
            continue

        if r.status_code == 401:
            raise RuntimeError("401 Unauthorized — check openweathermap-api-key in secret scope")

        if not r.ok:
            raise RuntimeError(
                f"OpenWeatherMap request failed with HTTP {r.status_code}"
            )
        return r.json()

    raise RuntimeError("fetch_owm: exceeded max retries")

def file_exists(path: str) -> bool:
    """Check if a file exists in Volume without raising on missing."""
    try:
        files = dbutils.fs.ls(path.rsplit("/", 1)[0])
        return any(f.path.endswith(path.rsplit("/", 1)[1]) for f in files)
    except Exception:
        return False  # parent dir doesn't exist yet

# COMMAND ----------

ingest_ts   = datetime.now(timezone.utc)
volume_base = f"/Volumes/{catalog}/bronze/raw_files/open_weather_map"

for iata, coords in AIRPORTS.items():
    t0 = time.time()

    base_params = {
        "lat":   coords["latitude"],
        "lon":   coords["longitude"],
        "appid": api_key,
        "units": "metric",  # Celsius, m/s — aviation standard
    }

    # Two separate API calls — current + 5-day forecast
    current  = fetch_owm(URL_CURRENT,  base_params)
    forecast = fetch_owm(URL_FORECAST, base_params)

    forecast_steps = forecast.get("cnt", 0)
    log.info(f"{iata} — current OK, forecast_steps={forecast_steps}")

    envelope = {
        "ingest_ts":  ingest_ts.isoformat(),
        "airport":    iata,
        "latitude":   coords["latitude"],
        "longitude":  coords["longitude"],
        "current":    current,                    # full current weather response
        "forecast":   forecast.get("list", []),   # list of 3h forecast steps
        "city":       forecast.get("city", {}),   # city metadata from forecast
    }

    envelope_json = json.dumps(envelope, ensure_ascii=False, allow_nan=False)

    partition_path = (
        f"airport={iata}/"
        f"year={ingest_ts.strftime('%Y')}/"
        f"month={ingest_ts.strftime('%m')}/"
        f"day={ingest_ts.strftime('%d')}/"
        f"hour={ingest_ts.strftime('%H')}"
    )

    file_name = f"owm_{iata}_{ingest_ts.strftime('%Y%m%d_%H%M%S')}.json"
    full_path = f"{volume_base}/{partition_path}/{file_name}"

    dbutils.fs.mkdirs(f"{volume_base}/{partition_path}")

    if file_exists(full_path):
        log.warning(f"{iata} — file already exists, skipping: {full_path}")
    else:
        dbutils.fs.put(full_path, envelope_json, overwrite=False)
        elapsed = time.time() - t0
        log.info(f"{iata} — written={full_path} size={len(envelope_json)}b elapsed={elapsed:.2f}s")
