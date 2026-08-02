# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze — OurAirports Ingest
# MAGIC Downloads reference datasets (airports, runways, countries, regions) from OurAirports
# MAGIC and writes CSV + JSON metadata file per dataset to Volume.
# MAGIC - **Job schedule:** every 20 min; unchanged content is skipped by SHA-256
# MAGIC - **Output:** `/Volumes/{catalog}/bronze/raw_files/our_airports/dataset=<name>/year=/month=/day=/`

# COMMAND ----------

import requests
import json
import time
import hashlib
import logging
from datetime import datetime, timezone

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("bronze.our_airports")

# COMMAND ----------

dbutils.widgets.text("catalog", "dev_alh")
catalog = dbutils.widgets.get("catalog")

# COMMAND ----------

DATASETS = {
    "airports":  "https://ourairports.com/data/airports.csv",
    "runways":   "https://ourairports.com/data/runways.csv",
    "countries": "https://ourairports.com/data/countries.csv",
    "regions":   "https://ourairports.com/data/regions.csv",
}

MAX_RETRIES    = 3
RETRY_STATUSES = {502, 503}
BACKOFF_BASE   = 2  # seconds — exponential: 2, 4, 8

# COMMAND ----------

def fetch_csv(url: str) -> requests.Response:
    """Fetch CSV file with retry logic for 429 / 5xx errors."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(url, timeout=60)  # larger timeout — files can be big
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
        return r

    raise RuntimeError("fetch_csv: exceeded max retries")

def file_exists(path: str) -> bool:
    """Check if a file exists in Volume without raising on missing."""
    try:
        files = dbutils.fs.ls(path.rsplit("/", 1)[0])
        return any(f.path.endswith(path.rsplit("/", 1)[1]) for f in files)
    except Exception:
        return False  # parent dir doesn't exist yet

def parse_columns(csv_text: str) -> list:
    """Extract column names from CSV header row."""
    header = csv_text.splitlines()[0]
    return [col.strip('"') for col in header.split(",")]

def count_rows(csv_text: str) -> int:
    """Count data rows (excluding header)."""
    return len(csv_text.splitlines()) - 1

# COMMAND ----------

ingest_ts   = datetime.now(timezone.utc)
volume_base = f"/Volumes/{catalog}/bronze/raw_files/our_airports"

for dataset, url in DATASETS.items():
    t0 = time.time()
    log.info(f"{dataset} — fetching from {url}")

    response     = fetch_csv(url)
    csv_text     = response.text
    content_hash = hashlib.sha256(csv_text.encode("utf-8")).hexdigest()
    columns      = parse_columns(csv_text)
    row_count    = count_rows(csv_text)

    log.info(f"{dataset} — rows={row_count} cols={len(columns)} hash={content_hash[:12]}...")

    # Flat structure — no year/month/day partitioning for reference data.
    # Hash in filename acts as natural content-addressable dedup:
    # same content → same hash → same filename → file_exists() → skip.
    dataset_dir = f"{volume_base}/dataset={dataset}"
    csv_path    = f"{dataset_dir}/{dataset}_{content_hash[:16]}.csv"
    meta_path   = f"{dataset_dir}/{dataset}_{content_hash[:16]}_meta.json"

    dbutils.fs.mkdirs(dataset_dir)

    if file_exists(csv_path):
        log.info(f"{dataset} — content unchanged (hash match), skipping")
        continue

    meta = {
        "ingest_ts":        ingest_ts.isoformat(),
        "source_url":       url,
        "dataset":          dataset,
        "row_count":        row_count,
        "column_count":     len(columns),
        "columns":          columns,
        "file_size_bytes":  len(csv_text.encode("utf-8")),
        "content_hash":     f"sha256:{content_hash}",
        "encoding":         "utf-8",
    }

    dbutils.fs.put(csv_path,  csv_text,                                     overwrite=False)
    dbutils.fs.put(meta_path, json.dumps(meta, ensure_ascii=False, indent=2), overwrite=False)

    elapsed = time.time() - t0
    log.info(f"{dataset} — written csv={csv_path} meta={meta_path} elapsed={elapsed:.2f}s")
