# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze — OpenSky Ingest
# MAGIC Fetches aircraft position snapshot over Poland from OpenSky Network API and writes raw JSON to Volume with Hive-style partitioning.
# MAGIC - **Job schedule:** every 20 min
# MAGIC - **Output:** `/Volumes/{catalog}/bronze/raw_files/opensky/year=/month=/day=/hour=/`

# COMMAND ----------

import requests
import json
import sys
import time
import logging
from datetime import datetime, timezone

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("bronze.opensky")

# COMMAND ----------

dbutils.widgets.text("catalog", "dev_alh")
catalog = dbutils.widgets.get("catalog")

# COMMAND ----------

# Resolve src path so opensky_schema can be imported from the bundle library.
# DAB deploys to: /Users/<email>/.bundle/aerolakehouse/dev/files/
# Notebook is at: .../files/notebooks/bronze_opensky_ingest
# files/ root is 2 levels up from the notebook path.
_notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
_src_path = "/Workspace" + "/".join(_notebook_path.split("/")[:-2]) + "/src"
if _src_path not in sys.path:
    sys.path.insert(0, _src_path)

from bronze_ingestion.opensky_schema import (
    OPENSKY_API_FIELD_COUNT,
    OPENSKY_INGEST_FIELDS,
)

# COMMAND ----------

SOURCE_URL = "https://opensky-network.org/api/states/all"
# Bounding box for Poland; extended=1 returns category field (index 17)
PARAMS = {"lamin": 49.0, "lomin": 14.0, "lamax": 55.1, "lomax": 24.2, "extended": 1}

MAX_RETRIES    = 3
RETRY_STATUSES = {502, 503}
BACKOFF_BASE   = 2              # seconds — exponential: 2, 4, 8

# COMMAND ----------

def fetch_opensky(url: str, params: dict) -> dict:
    """Fetch OpenSky API with retry logic for 429 / 5xx errors."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(url, params=params, timeout=30)
        except requests.Timeout:
            log.warning(f"attempt {attempt}/{MAX_RETRIES} — request timed out")
            if attempt == MAX_RETRIES:
                raise
            time.sleep(BACKOFF_BASE ** attempt)
            continue
        except requests.ConnectionError as exc:
            log.warning(f"attempt {attempt}/{MAX_RETRIES} — connection error: {exc}")
            if attempt == MAX_RETRIES:
                raise
            time.sleep(BACKOFF_BASE ** attempt)
            continue

        if r.status_code == 429:
            wait = int(r.headers.get("Retry-After", BACKOFF_BASE ** attempt))
            log.warning(f"429 rate limited — waiting {wait}s (attempt {attempt}/{MAX_RETRIES})")
            if attempt == MAX_RETRIES:
                r.raise_for_status()
            time.sleep(wait)
            continue

        if r.status_code in RETRY_STATUSES:
            wait = BACKOFF_BASE ** attempt
            log.warning(f"{r.status_code} server error — waiting {wait}s (attempt {attempt}/{MAX_RETRIES})")
            if attempt == MAX_RETRIES:
                r.raise_for_status()
            time.sleep(wait)
            continue

        r.raise_for_status()
        return r.json()

    raise RuntimeError("fetch_opensky: exceeded max retries")

def map_states(raw_states: list) -> list:
    """Map raw state arrays to named dicts, sanitizing NaN/Infinity to None."""
    result = []
    for state in raw_states:
        trimmed = state[:12] + state[13:]  # drop sensors at index 12
        row = {
            field: (None if isinstance(v, float) and v != v else v)
            for field, v in zip(OPENSKY_INGEST_FIELDS, trimmed)
        }
        result.append(row)
    return result

def file_exists(path: str) -> bool:
    """Check if a file exists in DBFS/Volume without raising on missing."""
    try:
        files = dbutils.fs.ls(path.rsplit("/", 1)[0])
        return any(f.path.endswith(path.rsplit("/", 1)[1]) for f in files)
    except Exception:
        return False  # parent dir doesn't exist yet

# COMMAND ----------

t0 = time.time()
ingest_ts = datetime.now(timezone.utc)
payload   = fetch_opensky(SOURCE_URL, PARAMS)

api_time = payload.get("time")
states   = payload.get("states") or []

if states:
    field_count = len(states[0])
    if field_count != OPENSKY_API_FIELD_COUNT:
        log.warning(f"schema drift detected: expected {OPENSKY_API_FIELD_COUNT} fields, got {field_count}")
    log.info(f"states={len(states)} fields_per_state={field_count}")
else:
    log.warning("empty states — API returned no aircraft; recording raw response")

mapped_states = map_states(states)
log.info(f"api_time={api_time} ingest_ts={ingest_ts.isoformat()}")

# COMMAND ----------

envelope = {
    "ingest_ts":    ingest_ts.isoformat(),
    "source_url":   SOURCE_URL,
    "params":       PARAMS,
    "api_time":     api_time,
    "states_count": len(states),
    "states":       mapped_states,
}

# NaN/Infinity already sanitized in map_states; allow_nan=False as final guard
envelope_json = json.dumps(envelope, ensure_ascii=False, allow_nan=False)

# COMMAND ----------

volume_base = f"/Volumes/{catalog}/bronze/raw_files/opensky"

partition_path = (
    f"year={ingest_ts.strftime('%Y')}/"
    f"month={ingest_ts.strftime('%m')}/"
    f"day={ingest_ts.strftime('%d')}/"
    f"hour={ingest_ts.strftime('%H')}"
)

file_name = f"opensky_{ingest_ts.strftime('%Y%m%d_%H%M%S')}.json"
full_path = f"{volume_base}/{partition_path}/{file_name}"

dbutils.fs.mkdirs(f"{volume_base}/{partition_path}")

if file_exists(full_path):
    log.warning(f"file already exists, skipping write: {full_path}")
else:
    dbutils.fs.put(full_path, envelope_json, overwrite=False)
    elapsed = time.time() - t0
    log.info(f"written={full_path} size={len(envelope_json)}b states={len(states)} elapsed={elapsed:.2f}s")
