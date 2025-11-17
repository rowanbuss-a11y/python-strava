#!/usr/bin/env python3
"""
strava_sync.py
Volledige detail-sync: haalt activiteiten op van Strava (laatste N dagen),
haalt per-activity details (calories, gear_name, heart rate, kudos, comments, etc.),
maakt ontbrekende Supabase-kolommen aan (indien mogelijk) en upsert naar Supabase.
Maakt CSV + JSON backups.

Kopieer/plak rechtstreeks in GitHub. Zorg dat secrets zijn ingesteld.
"""

import os
import time
import json
import csv
import math
import requests
from datetime import datetime, timedelta, timezone
from supabase import create_client, Client

# -----------------------
# Config / env
# -----------------------
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
STRAVA_CLIENT_ID = os.getenv("STRAVA_CLIENT_ID")
STRAVA_CLIENT_SECRET = os.getenv("STRAVA_CLIENT_SECRET")
STRAVA_REFRESH_TOKEN = os.getenv("STRAVA_REFRESH_TOKEN")

CSV_FILE = os.getenv("CSV_FILE", "activiteiten.csv")
JSON_FILE = os.getenv("JSON_FILE", "activiteiten_raw.json")
DAYS_BACK = int(os.getenv("DAYS_BACK", "90"))

SUPABASE_TABLE = os.getenv("SUPABASE_TABLE", "strava_activities")

# sanity checks
required_env = {
    "SUPABASE_URL": SUPABASE_URL,
    "SUPABASE_KEY": SUPABASE_KEY,
    "STRAVA_CLIENT_ID": STRAVA_CLIENT_ID,
    "STRAVA_CLIENT_SECRET": STRAVA_CLIENT_SECRET,
    "STRAVA_REFRESH_TOKEN": STRAVA_REFRESH_TOKEN,
}
missing = [k for k, v in required_env.items() if not v]
if missing:
    raise RuntimeError(f"Missing required env vars: {missing}")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# -----------------------
# Helpers: rate limit + requests
# -----------------------
def safe_get(url, headers=None, params=None, max_retries=5, backoff=2):
    """GET request with retry on 429 and basic exponential backoff."""
    headers = headers or {}
    attempt = 0
    while attempt < max_retries:
        try:
            r = requests.get(url, headers=headers, params=params, timeout=30)
        except requests.RequestException as e:
            attempt += 1
            wait = backoff ** attempt
            print(f"⚠️ RequestException: {e} — retry in {wait}s (attempt {attempt}/{max_retries})")
            time.sleep(wait)
            continue

        if r.status_code == 429:
            # read Retry-After header if present
            retry_after = r.headers.get("Retry-After")
            wait = int(retry_after) if retry_after and retry_after.isdigit() else max(5, backoff ** (attempt + 1))
            print(f"⏳ Rate limit (429) — wacht {wait}s (attempt {attempt + 1}/{max_retries})")
            time.sleep(wait)
            attempt += 1
            continue

        # other HTTP errors will be handled by caller via r.raise_for_status() if needed
        return r
    raise RuntimeError(f"Max retries reached for GET {url}")

def safe_post(url, headers=None, data=None, json_payload=None, max_retries=3):
    headers = headers or {}
    attempt = 0
    while attempt < max_retries:
        try:
            r = requests.post(url, headers=headers, data=data, json=json_payload, timeout=30)
        except requests.RequestException as e:
            attempt += 1
            time.sleep(2 ** attempt)
            continue

        if r.status_code == 429:
            retry_after = r.headers.get("Retry-After")
            wait = int(retry_after) if retry_after and retry_after.isdigit() else 5
            time.sleep(wait)
            attempt += 1
            continue

        return r
    raise RuntimeError(f"Max retries reached for POST {url}")

# -----------------------
# Strava auth & fetch
# -----------------------
def refresh_access_token():
    url = "https://www.strava.com/api/v3/oauth/token"
    payload = {
        "client_id": STRAVA_CLIENT_ID,
        "client_secret": STRAVA_CLIENT_SECRET,
        "grant_type": "refresh_token",
        "refresh_token": STRAVA_REFRESH_TOKEN,
    }
    r = safe_post(url, data=payload)
    r.raise_for_status()
    token_data = r.json()
    access_token = token_data.get("access_token")
    if not access_token:
        raise RuntimeError("Failed to obtain Strava access token")
    print("✅ Nieuw access token opgehaald")
    return access_token

def fetch_activities_summary(access_token, after_ts):
    """Fetch activities list (summary). Returns list of activity summary dicts."""
    print(f"⏱ Ophalen activiteiten vanaf timestamp {after_ts} ({datetime.utcfromtimestamp(after_ts).date()})")
    all_acts = []
    page = 1
    per_page = 200
    headers = {"Authorization": f"Bearer {access_token}"}
    while True:
        params = {"page": page, "per_page": per_page, "after": after_ts}
        r = safe_get("https://www.strava.com/api/v3/athlete/activities", headers=headers, params=params)
        r.raise_for_status()
        data = r.json()
        if not data:
            break
        all_acts.extend(data)
        print(f"  → pagina {page}: {len(data)} activiteiten")
        if len(data) < per_page:
            break
        page += 1
    print(f"📦 Totaal {len(all_acts)} activiteiten samenvatting opgehaald")
    return all_acts

def fetch_activity_details(access_token, activity_id):
    """Fetch full activity details for a single activity id."""
    headers = {"Authorization": f"Bearer {access_token}"}
    url = f"https://www.strava.com/api/v3/activities/{activity_id}"
    r = safe_get(url, headers=headers)
    if r.status_code == 404:
        return {}
    r.raise_for_status()
    return r.json()

def fetch_gear_name(access_token, gear_id):
    """Fetch gear name given gear id; returns None if not found."""
    if not gear_id:
        return None
    headers = {"Authorization": f"Bearer {access_token}"}
    url = f"https://www.strava.com/api/v3/gear/{gear_id}"
    r = safe_get(url, headers=headers)
    if r.status_code == 404:
        return None
    r.raise_for_status()
    return r.json().get("name")

# -----------------------
# Supabase schema helpers
# -----------------------
def ensure_supabase_columns():
    """
    Ensure required columns exist. Will attempt to run ALTER TABLE per column via Supabase RPC.
    If RPC fails it will continue (user can run SQL manually).
    """
    print("🔍 Controleren / aanmaken Supabase-kolommen (indien nodig)...")
    required_columns = {
        "id": "bigint",
        "name": "text",
        "type": "text",
        "start_date": "timestamptz",
        "start_date_local": "text",
        "distance": "numeric",
        "distance_km": "numeric",
        "moving_time": "integer",
        "elapsed_time": "integer",
        "total_elevation_gain": "numeric",
        "elev_high": "numeric",
        "elev_low": "numeric",
        "average_speed": "numeric",
        "average_speed_kmh": "numeric",
        "max_speed": "numeric",
        "max_speed_kmh": "numeric",
        "average_watts": "numeric",
        "weighted_average_watts": "numeric",
        "kilojoules": "numeric",
        "calories": "numeric",
        "suffer_score": "numeric",
        "average_heartrate": "numeric",
        "max_heartrate": "numeric",
        "has_heartrate": "boolean",
        "pr_count": "integer",
        "kudos_count": "integer",
        "comment_count": "integer",
        "athlete_count": "integer",
        "photo_count": "integer",
        "gear_id": "text",
        "gear_name": "text",
        "device_name": "text",
        "perceived_exertion": "integer",
        "workout_type": "integer",
        "achievement_count": "integer",
        "trainer": "boolean",
        "commute": "boolean",
        "private": "boolean",
        "flagged": "boolean",
        "best_efforts_count": "integer",
        "splits_metric_count": "integer",
        "laps_count": "integer",
        "segment_efforts_count": "integer",
        "map_id": "text",
        "map_summary_polyline": "text",
        "map_polyline": "text",
        "map_resource_state": "integer",
        "external_id": "text",
        "upload_id": "bigint",
        "description": "text"
    }

    # Use ALTER TABLE per column (works in Postgres)
    for col, col_type in required_columns.items():
        sql = f"ALTER TABLE {SUPABASE_TABLE} ADD COLUMN IF NOT EXISTS {col} {col_type};"
        try:
            # supabase-python has rpc; name of RPC function may differ by setup.
            # Try to call a SQL executor RPC if available (many projects expose a 'pg_execute' or 'sql' rpc)
            # We'll attempt supabase.rpc("sql", {"query": sql}) as we've used before — ignore failures.
            try:
                supabase.rpc("sql", {"query": sql})
            except Exception:
                # Fallback: try direct POST to /rest/v1/rpc/sql? (not always available) — ignore if fails
                pass
        except Exception:
            pass

    # Small pause to give Supabase time (schema cache)
    time.sleep(1.5)
    print("✅ Supabase-kolommen gecontroleerd (indien mogelijk aangemaakt).")

# -----------------------
# Process & map fields
# -----------------------
def safe_num(x):
    if x is None:
        return None
    try:
        if isinstance(x, bool):
            return None
        return float(x)
    except Exception:
        return None

def prepare_row(summary, details, access_token, gear_cache):
    """
    Build a flat dict with all desired fields, combining summary (list endpoint) and details.
    Prefer detail values when present.
    """
    # use details if present else summary
    d = details or {}
    s = summary or {}

    # gear name: prefer details gear->name; fallback call to gear endpoint if only id present
    gear_id = d.get("gear_id") or s.get("gear_id")
    gear_name = None
    # Strava may include gear as an object in details: d.get('gear', {}).get('name') possible
    if isinstance(d.get("gear"), dict):
        gear_name = d.get("gear", {}).get("name")
        if not gear_id:
            gear_id = d.get("gear", {}).get("id")
    if not gear_name and gear_id:
        # cache gear lookup
        if gear_id in gear_cache:
            gear_name = gear_cache[gear_id]
        else:
            try:
                gear_name = fetch_gear_name(access_token, gear_id)
            except Exception:
                gear_name = None
            gear_cache[gear_id] = gear_name

    # helper conversions
    dist = d.get("distance", s.get("distance"))
    avg_speed = d.get("average_speed", s.get("average_speed"))
    max_speed = d.get("max_speed", s.get("max_speed"))
    moving_time = d.get("moving_time", s.get("moving_time"))
    elapsed_time = d.get("elapsed_time", s.get("elapsed_time"))

    # times
    start_date = d.get("start_date") or s.get("start_date")
    start_date_local = d.get("start_date_local") or s.get("start_date_local")

    row = {
        "id": int(d.get("id") or s.get("id")),
        "name": d.get("name") or s.get("name"),
        "type": d.get("type") or s.get("type"),
        "start_date": start_date,
        "start_date_local": start_date_local,
        "distance": safe_num(dist),
        "distance_km": round(safe_num(dist) / 1000, 3) if safe_num(dist) is not None else None,
        "moving_time": int(moving_time) if moving_time is not None else None,
        "elapsed_time": int(elapsed_time) if elapsed_time is not None else None,
        "total_elevation_gain": safe_num(d.get("total_elevation_gain") or s.get("total_elevation_gain")),
        "elev_high": safe_num(d.get("elev_high")),
        "elev_low": safe_num(d.get("elev_low")),
        "average_speed": safe_num(avg_speed),
        "average_speed_kmh": round(safe_num(avg_speed) * 3.6, 3) if safe_num(avg_speed) is not None else None,
        "max_speed": safe_num(max_speed),
        "max_speed_kmh": round(safe_num(max_speed) * 3.6, 3) if safe_num(max_speed) is not None else None,
        "average_watts": safe_num(d.get("average_watts")),
        "weighted_average_watts": safe_num(d.get("weighted_average_watts")),
        "kilojoules": safe_num(d.get("kilojoules")),
        "calories": safe_num(d.get("calories")),
        "suffer_score": safe_num(d.get("suffer_score")),
        "average_heartrate": safe_num(d.get("average_heartrate")),
        "max_heartrate": safe_num(d.get("max_heartrate")),
        "has_heartrate": bool(d.get("has_heartrate") or False),
        "pr_count": int(d.get("pr_count") or 0),
        "kudos_count": int(d.get("kudos_count") or s.get("kudos_count") or 0),
        "comment_count": int(d.get("comment_count") or s.get("comment_count") or 0),
        "athlete_count": int(d.get("athlete_count") or s.get("athlete_count") or 1),
        "photo_count": int(d.get("photo_count") or s.get("photo_count") or 0),
        "gear_id": gear_id,
        "gear_name": gear_name,
        "device_name": d.get("device_name") or s.get("device_name"),
        "perceived_exertion": d.get("perceived_exertion"),
        "workout_type": d.get("workout_type"),
        "achievement_count": int(d.get("achievement_count") or 0),
        "trainer": bool(d.get("trainer") or False),
        "commute": bool(d.get("commute") or False),
        "private": bool(d.get("private") or False),
        "flagged": bool(d.get("flagged") or False),
        "best_efforts_count": int(len(d.get("best_efforts", [])) if d.get("best_efforts") is not None else 0),
        "splits_metric_count": int(len(d.get("splits_metric", [])) if d.get("splits_metric") is not None else 0),
        "laps_count": int(len(d.get("laps", [])) if d.get("laps") is not None else 0),
        "segment_efforts_count": int(len(d.get("segment_efforts", [])) if d.get("segment_efforts") is not None else 0),
        "map_id": (d.get("map") or s.get("map") or {}).get("id"),
        "map_summary_polyline": (d.get("map") or s.get("map") or {}).get("summary_polyline"),
        "map_polyline": (d.get("map") or s.get("map") or {}).get("polyline"),
        "map_resource_state": (d.get("map") or s.get("map") or {}).get("resource_state"),
        "external_id": d.get("external_id") or s.get("external_id"),
        "upload_id": d.get("upload_id") or s.get("upload_id"),
        "description": d.get("description") or s.get("description")
    }

    return row

# -----------------------
# Upload + save
# -----------------------
def upload_rows(rows):
    if not rows:
        print("ℹ️ Geen rijen om te uploaden.")
        return
    try:
        # upsert
        supabase.table(SUPABASE_TABLE).upsert(rows, on_conflict="id").execute()
        print(f"✅ {len(rows)} rijen geüpload naar Supabase.")
    except Exception as e:
        print(f"❌ Upload error: {e}")

def save_json_csv(rows):
    if not rows:
        return
    # JSON dump
    with open(JSON_FILE, "w", encoding="utf-8") as f:
        json.dump(rows, f, indent=2, ensure_ascii=False)
    # CSV dump (dynamic columns)
    keys = list(rows[0].keys())
    with open(CSV_FILE, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(rows)
    print(f"💾 Backup: {JSON_FILE} + {CSV_FILE}")

# -----------------------
# Main flow
# -----------------------
def main():
    print("▶️ Starting Strava → Supabase sync")
    # ensure schema
    ensure_supabase_columns()

    # get access token
    access_token = refresh_access_token()

    # compute after timestamp
    after_dt = datetime.utcnow() - timedelta(days=DAYS_BACK)
    after_ts = int(after_dt.replace(tzinfo=timezone.utc).timestamp())

    # fetch summary
    summaries = fetch_activities_summary(access_token, after_ts)

    # fetch details and prepare rows
    rows = []
    gear_cache = {}
    for idx, s in enumerate(summaries, start=1):
        aid = s.get("id")
        print(f"[{idx}/{len(summaries)}] ophalen details voor activity {aid} ...")
        try:
            details = fetch_activity_details(access_token, aid)
        except Exception as e:
            print(f"  ⚠️ Kon details niet ophalen voor {aid}: {e}")
            details = {}
        try:
            row = prepare_row(s, details, access_token, gear_cache)
            rows.append(row)
        except Exception as e:
            print(f"  ⚠️ Fout bij voorbereiden rij voor {aid}: {e}")
        # small delay to be gentle on API
        time.sleep(0.3)

    # upload + backups
    upload_rows(rows)
    save_json_csv(rows)
    print("✅ Sync klaar.")

if __name__ == "__main__":
    main()
