# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze — Aviationstack Ingest
# MAGIC Fetches departures + arrivals for Polish airports using Aviationstack API (Free Plan).
# MAGIC - **Job schedule:** every 20 min
# MAGIC - **API refresh gate:** at most once every 48 hours after a complete successful pull
# MAGIC - **Endpoint:** `https://api.aviationstack.com/v1/flights`
# MAGIC - **Security:** HTTPS is mandatory; the notebook never downgrades to HTTP
# MAGIC - **Filter:** dep_iata / arr_iata per airport; no date or status filter
# MAGIC - **Coverage:** first page only, up to 100 records per airport and direction
# MAGIC - **Limit:** 100 requests/month on free tier
# MAGIC - **Output:** `/Volumes/{catalog}/bronze/raw_files/aviationstack/airport=<IATA>/year=/month=/day=/hour=/`

# COMMAND ----------

import requests
import json
import time
import logging
from datetime import datetime, timedelta, timezone

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("bronze.aviationstack")

# COMMAND ----------

dbutils.widgets.text("catalog", "dev_alh")
catalog = dbutils.widgets.get("catalog")

# Secret scope is environment-specific — dev catalog uses aerolakehouse-dev,
# prod catalog uses aerolakehouse-prod. Derived from catalog prefix.
_env         = "dev" if catalog.startswith("dev") else "prod"
_secret_scope = f"aerolakehouse-{_env}"

# COMMAND ----------

# Credentials are sent as query parameters required by the provider, so transport
# encryption is mandatory. Use a plan that supports HTTPS; never downgrade to HTTP.
BASE_URL = "https://api.aviationstack.com/v1/flights"

AIRPORTS = ["WAW", "GDN"]
MIN_REFRESH_INTERVAL = timedelta(hours=48)

MAX_RETRIES    = 3
RETRY_STATUSES = {502, 503, 504}
BACKOFF_BASE   = 2  # seconds — exponential: 2, 4, 8

# COMMAND ----------

def fetch_aviationstack(params: dict) -> dict:
    """Fetch Aviationstack API with retry logic for 429 / 5xx errors."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(BASE_URL, params=params, timeout=30)
        except requests.Timeout:
            log.warning(f"attempt {attempt}/{MAX_RETRIES} — request timed out")
            if attempt == MAX_RETRIES:
                raise RuntimeError("Aviationstack request timed out") from None
            time.sleep(BACKOFF_BASE ** attempt)
            continue
        except requests.ConnectionError:
            log.warning(f"attempt {attempt}/{MAX_RETRIES} — connection error")
            if attempt == MAX_RETRIES:
                raise RuntimeError("Aviationstack connection failed") from None
            time.sleep(BACKOFF_BASE ** attempt)
            continue

        if r.status_code == 429:
            wait = int(r.headers.get("Retry-After", BACKOFF_BASE ** attempt))
            log.warning(f"429 rate limited — waiting {wait}s (attempt {attempt}/{MAX_RETRIES})")
            if attempt == MAX_RETRIES:
                raise RuntimeError("Aviationstack request failed with HTTP 429")
            time.sleep(wait)
            continue

        if r.status_code in RETRY_STATUSES:
            wait = BACKOFF_BASE ** attempt
            log.warning(f"{r.status_code} server error — waiting {wait}s (attempt {attempt}/{MAX_RETRIES})")
            if attempt == MAX_RETRIES:
                raise RuntimeError(
                    f"Aviationstack request failed with HTTP {r.status_code}"
                )
            time.sleep(wait)
            continue

        if r.status_code == 401:
            raise RuntimeError("401 Unauthorized — check aviationstack-api-key in secret scope")

        if not r.ok:
            raise RuntimeError(
                f"Aviationstack request failed with HTTP {r.status_code}"
            )

        body = r.json()
        # Aviationstack returns HTTP 200 but with error object on bad key / quota exceeded
        if "error" in body:
            err = body["error"]
            raise RuntimeError(f"Aviationstack API error {err.get('code')}: {err.get('message')}")

        # A successful response returns immediately. Later attempts are used only
        # after retryable failures from this same request.
        return body

    raise RuntimeError("fetch_aviationstack: exceeded max retries")

def file_exists(path: str) -> bool:
    """Check if a file exists in Volume without raising on missing."""
    try:
        files = dbutils.fs.ls(path.rsplit("/", 1)[0])
        return any(f.path.endswith(path.rsplit("/", 1)[1]) for f in files)
    except Exception:
        return False  # parent dir doesn't exist yet

def read_json_file(path: str) -> dict | None:
    """Read a small JSON control file, returning None when it does not exist."""
    if not file_exists(path):
        return None
    try:
        return json.loads(dbutils.fs.head(path, 1024 * 1024))
    except Exception as exc:
        raise RuntimeError(f"Invalid Aviationstack control file {path}: {exc}") from exc

def write_json_file(path: str, payload: dict) -> None:
    """Replace a small JSON control file."""
    dbutils.fs.mkdirs(path.rsplit("/", 1)[0])
    dbutils.fs.put(
        path,
        json.dumps(payload, ensure_ascii=False, allow_nan=False),
        overwrite=True,
    )

def parse_utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)

# COMMAND ----------

volume_base = f"/Volumes/{catalog}/bronze/raw_files/aviationstack"
control_base = f"/Volumes/{catalog}/bronze/raw_files/_control/aviationstack"
last_success_path = f"{control_base}/last_success.json"
pending_path = f"{control_base}/pending.json"

now = datetime.now(timezone.utc)
last_success = read_json_file(last_success_path)

if last_success:
    completed_at = parse_utc(last_success["completed_at_utc"])
    next_refresh_at = completed_at + MIN_REFRESH_INTERVAL
    if now < next_refresh_at:
        remaining = next_refresh_at - now
        log.info(
            "Aviationstack refresh skipped: last complete success=%s, "
            "next refresh=%s, remaining=%s",
            completed_at.isoformat(),
            next_refresh_at.isoformat(),
            remaining,
        )
        dbutils.notebook.exit(
            json.dumps(
                {
                    "status": "SKIPPED_48H_GATE",
                    "last_success_at_utc": completed_at.isoformat(),
                    "next_refresh_at_utc": next_refresh_at.isoformat(),
                }
            )
        )

# A pending cycle survives task retries and scheduled reruns. Successful API
# responses are cached outside the Auto Loader input path, so only missing
# requests are repeated after a partial failure.
pending = read_json_file(pending_path)

# If cleanup failed after a previously completed cycle, discard that stale
# pending state before starting the next 48-hour refresh.
if (
    pending is not None
    and last_success is not None
    and pending.get("cycle_id") == last_success.get("cycle_id")
):
    stale_cycle_path = f"{control_base}/pending/{pending['cycle_id']}"
    log.warning("Removing stale completed cycle state: %s", pending["cycle_id"])
    dbutils.fs.rm(stale_cycle_path, recurse=True)
    dbutils.fs.rm(pending_path)
    pending = None

if pending is None:
    cycle_started_at = now
    cycle_id = cycle_started_at.strftime("%Y%m%dT%H%M%SZ")
    pending = {
        "cycle_id": cycle_id,
        "started_at_utc": cycle_started_at.isoformat(),
    }
    write_json_file(pending_path, pending)
else:
    cycle_id = pending["cycle_id"]
    cycle_started_at = parse_utc(pending["started_at_utc"])

cycle_cache_path = f"{control_base}/pending/{cycle_id}"
api_key = None

def load_or_fetch(cache_path: str, params: dict) -> dict:
    """Use a successful cached response before making another API request."""
    cached = read_json_file(cache_path)
    if cached is not None:
        log.info("Using cached successful API response: %s", cache_path)
        return cached

    global api_key
    if api_key is None:
        api_key = dbutils.secrets.get(
            scope=_secret_scope,
            key="aviationstack-api-key",
        )

    payload = fetch_aviationstack({"access_key": api_key, **params})
    write_json_file(cache_path, payload)
    return payload

for iata in AIRPORTS:
    t0 = time.time()

    partition_path = (
        f"airport={iata}/"
        f"year={cycle_started_at.strftime('%Y')}/"
        f"month={cycle_started_at.strftime('%m')}/"
        f"day={cycle_started_at.strftime('%d')}/"
        f"hour={cycle_started_at.strftime('%H')}"
    )
    file_name = f"aviationstack_{iata}_{cycle_id}.json"
    full_path = f"{volume_base}/{partition_path}/{file_name}"

    # This covers a task restart after the final data file was written but
    # before the global success marker was updated.
    if file_exists(full_path):
        log.info("%s already completed in cycle %s; skipping API", iata, cycle_id)
        continue

    # Departures from this airport
    dep_payload = load_or_fetch(
        f"{cycle_cache_path}/{iata}_departures.json",
        {
            "dep_iata": iata,
            "limit": 100,
        },
    )
    departures = dep_payload.get("data", [])

    # Arrivals to this airport
    arr_payload = load_or_fetch(
        f"{cycle_cache_path}/{iata}_arrivals.json",
        {
            "arr_iata": iata,
            "limit": 100,
        },
    )
    arrivals = arr_payload.get("data", [])

    log.info(f"{iata} — departures={len(departures)} arrivals={len(arrivals)}")

    envelope = {
        "ingest_ts":        datetime.now(timezone.utc).isoformat(),
        "airport_iata":     iata,
        "departures":       departures,
        "arrivals":         arrivals,
        "departures_count": len(departures),
        "arrivals_count":   len(arrivals),
    }

    envelope_json = json.dumps(envelope, ensure_ascii=False, allow_nan=False)

    dbutils.fs.mkdirs(f"{volume_base}/{partition_path}")
    dbutils.fs.put(full_path, envelope_json, overwrite=False)
    elapsed = time.time() - t0
    log.info(f"{iata} — written={full_path} size={len(envelope_json)}b elapsed={elapsed:.2f}s")

completed_at = datetime.now(timezone.utc)
write_json_file(
    last_success_path,
    {
        "cycle_id": cycle_id,
        "started_at_utc": cycle_started_at.isoformat(),
        "completed_at_utc": completed_at.isoformat(),
        "airports": AIRPORTS,
        "successful_requests_per_cycle": len(AIRPORTS) * 2,
        "minimum_refresh_interval_hours": int(
            MIN_REFRESH_INTERVAL.total_seconds() / 3600
        ),
    },
)

# Cleanup is best-effort. The success marker is written first so a cleanup
# failure cannot cause the API to be called again within the 48-hour window.
try:
    dbutils.fs.rm(cycle_cache_path, recurse=True)
    dbutils.fs.rm(pending_path)
except Exception as exc:
    log.warning("Could not clean Aviationstack pending state: %s", exc)

log.info(
    "Aviationstack cycle %s completed at %s; next refresh after %s",
    cycle_id,
    completed_at.isoformat(),
    (completed_at + MIN_REFRESH_INTERVAL).isoformat(),
)
