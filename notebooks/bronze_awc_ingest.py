# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze — Aviation Weather Center (AWC) Ingest
# MAGIC Fetches aviation weather products for Polish airports from the free AWC API (no auth required).
# MAGIC - **Job schedule:** every 20 min
# MAGIC - **Products:** METAR, TAF, PIREP, SIGMET/AIRMET
# MAGIC - **Stations:** EPWA (WAW), EPGD (GDN)
# MAGIC - **Output:** `/Volumes/{catalog}/bronze/raw_files/awc/<product>/year=/month=/day=/hour=/`
# MAGIC - **Bronze contract:** METAR is promoted to Delta; other products remain in the raw Volume pending dedicated contracts

# COMMAND ----------

import requests
import json
import time
import logging
from datetime import datetime, timezone

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("bronze.awc")

# COMMAND ----------

dbutils.widgets.text("catalog", "dev_alh")
catalog = dbutils.widgets.get("catalog")

# COMMAND ----------

BASE_URL = "https://aviationweather.gov/api/data"

# ICAO station codes for target airports
STATIONS = ["EPWA", "EPGD"]
STATIONS_CSV = ",".join(STATIONS)

# METAR — last N hours of observations per station
METAR_HOURS = 2

# PIREP — search radius in NM around each station
PIREP_DISTANCE_NM = 200

# SIGMET/AIRMET — FIR region covering Poland (EPWW = Warsaw FIR)
SIGMET_FIR = "EPWW"

MAX_RETRIES    = 3
RETRY_STATUSES = {502, 503, 504}
BACKOFF_BASE   = 2  # seconds — exponential: 2, 4, 8

# COMMAND ----------

def fetch_awc(endpoint: str, params: dict) -> list:
    """Fetch AWC API (public, no auth). Returns list of records."""
    url = f"{BASE_URL}/{endpoint}"
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(url, params=params, timeout=30)
        except requests.Timeout:
            log.warning(f"{endpoint} attempt {attempt}/{MAX_RETRIES} — request timed out")
            if attempt == MAX_RETRIES:
                raise
            time.sleep(BACKOFF_BASE ** attempt)
            continue
        except requests.ConnectionError as exc:
            log.warning(f"{endpoint} attempt {attempt}/{MAX_RETRIES} — connection error: {exc}")
            if attempt == MAX_RETRIES:
                raise
            time.sleep(BACKOFF_BASE ** attempt)
            continue

        if r.status_code == 429:
            wait = int(r.headers.get("Retry-After", BACKOFF_BASE ** attempt))
            log.warning(f"{endpoint} 429 rate limited — waiting {wait}s (attempt {attempt}/{MAX_RETRIES})")
            if attempt == MAX_RETRIES:
                r.raise_for_status()
            time.sleep(wait)
            continue

        if r.status_code in RETRY_STATUSES:
            wait = BACKOFF_BASE ** attempt
            log.warning(f"{endpoint} {r.status_code} server error — waiting {wait}s (attempt {attempt}/{MAX_RETRIES})")
            if attempt == MAX_RETRIES:
                r.raise_for_status()
            time.sleep(wait)
            continue

        r.raise_for_status()

        # AWC returns empty body (not JSON) when there are no results
        if not r.text or not r.text.strip():
            return []

        data = r.json()
        if isinstance(data, list):
            return data
        return []

    raise RuntimeError(f"fetch_awc({endpoint}): exceeded max retries")

def file_exists(path: str) -> bool:
    """Check if a file exists in Volume without raising on missing."""
    try:
        files = dbutils.fs.ls(path.rsplit("/", 1)[0])
        return any(f.path.endswith(path.rsplit("/", 1)[1]) for f in files)
    except Exception:
        return False  # parent dir doesn't exist yet

def write_product(product: str, data: dict, ingest_ts: datetime, volume_base: str) -> None:
    """Serialize envelope to JSON and write to Volume partition."""
    envelope_json = json.dumps(data, ensure_ascii=False, allow_nan=False)

    partition_path = (
        f"product={product}/"
        f"year={ingest_ts.strftime('%Y')}/"
        f"month={ingest_ts.strftime('%m')}/"
        f"day={ingest_ts.strftime('%d')}/"
        f"hour={ingest_ts.strftime('%H')}"
    )

    file_name = f"awc_{product}_{ingest_ts.strftime('%Y%m%d_%H%M%S')}.json"
    full_path = f"{volume_base}/{partition_path}/{file_name}"

    dbutils.fs.mkdirs(f"{volume_base}/{partition_path}")

    if file_exists(full_path):
        log.warning(f"{product} — file already exists, skipping: {full_path}")
    else:
        dbutils.fs.put(full_path, envelope_json, overwrite=False)
        log.info(f"{product} — written={full_path} size={len(envelope_json)}b")

# COMMAND ----------

ingest_ts   = datetime.now(timezone.utc)
volume_base = f"/Volumes/{catalog}/bronze/raw_files/awc"

# --------------------------------------------------------------------------
# 1. METAR — current observations for EPWA + EPGD (last METAR_HOURS hours)
# --------------------------------------------------------------------------
t0 = time.time()
metar_data = fetch_awc("metar", {
    "ids":    STATIONS_CSV,
    "format": "json",
    "hours":  METAR_HOURS,
})
log.info(f"METAR — records={len(metar_data)} elapsed={time.time()-t0:.2f}s")

write_product("metar", {
    "ingest_ts":  ingest_ts.isoformat(),
    "stations":   STATIONS,
    "hours":      METAR_HOURS,
    "count":      len(metar_data),
    "data":       metar_data,
}, ingest_ts, volume_base)

# --------------------------------------------------------------------------
# 2. TAF — terminal aerodrome forecasts for EPWA + EPGD
# --------------------------------------------------------------------------
t0 = time.time()
taf_data = fetch_awc("taf", {
    "ids":    STATIONS_CSV,
    "format": "json",
})
log.info(f"TAF — records={len(taf_data)} elapsed={time.time()-t0:.2f}s")

write_product("taf", {
    "ingest_ts": ingest_ts.isoformat(),
    "stations":  STATIONS,
    "count":     len(taf_data),
    "data":      taf_data,
}, ingest_ts, volume_base)

# --------------------------------------------------------------------------
# 3. PIREP — pilot reports within PIREP_DISTANCE_NM NM of each station
# --------------------------------------------------------------------------
t0 = time.time()
pirep_all = []
for station in STATIONS:
    records = fetch_awc("pirep", {
        "id":       station,
        "format":   "json",
        "distance": PIREP_DISTANCE_NM,
    })
    pirep_all.extend(records)
    log.info(f"PIREP {station} — records={len(records)}")

log.info(f"PIREP — total={len(pirep_all)} elapsed={time.time()-t0:.2f}s")

write_product("pirep", {
    "ingest_ts":    ingest_ts.isoformat(),
    "stations":     STATIONS,
    "distance_nm":  PIREP_DISTANCE_NM,
    "count":        len(pirep_all),
    "data":         pirep_all,
}, ingest_ts, volume_base)

# --------------------------------------------------------------------------
# 4. SIGMET/AIRMET — active advisories for Warsaw FIR (EPWW)
# --------------------------------------------------------------------------
t0 = time.time()
sigmet_data = fetch_awc("sigmet", {
    "format": "json",
    "fir":    SIGMET_FIR,
})
log.info(f"SIGMET/AIRMET — records={len(sigmet_data)} elapsed={time.time()-t0:.2f}s")

write_product("sigmet", {
    "ingest_ts": ingest_ts.isoformat(),
    "fir":       SIGMET_FIR,
    "count":     len(sigmet_data),
    "data":      sigmet_data,
}, ingest_ts, volume_base)
