import os
import json
import time
from datetime import datetime, timedelta
import requests

# -------------------------------
# Helper functies
# -------------------------------

def env(name: str, required: bool = True, default: str | None = None) -> str | None:
    value = os.environ.get(name, default)
    if required and (value is None or value == ""):
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value

def get_access_token() -> str:
    """Haalt een geldig access token op via refresh token"""
    client_id = env("STRAVA_CLIENT_ID")
    client_secret = env("STRAVA_CLIENT_SECRET")
    refresh_token = env("STRAVA_REFRESH_TOKEN")

    url = "https://www.strava.com/oauth/token"
    payload = {
        "client_id": client_id,
        "client_secret": client_secret,
        "refresh_token": refresh_token,
        "grant_type": "refresh_token"
    }

    response = requests.post(url, data=payload, timeout=30)
    response.raise_for_status()
    data = response.json()
    return data["access_token"]

def fetch_recent_activities(access_token: str, days: int = 30) -> list[dict]:
    """Haal activiteiten van de laatste N dagen op"""
    activities = []
    page = 1
    per_page = 200
    after_date = datetime.now() - timedelta(days=days)
    after_timestamp = int(after_date.timestamp())

    print(f"DEBUG: Fetching activities from last {days} days")

    while True:
        url = "https://www.strava.com/api/v3/athlete/activities"
        headers = {"Authorization": f"Bearer {access_token}"}
        params = {"page": page, "per_page": per_page, "after": after_timestamp}

        response = requests.get(url, headers=headers, params=params, timeout=30)
        if response.status_code == 429:
            print("DEBUG: Rate limit reached, waiting 60s...")
            time.sleep(60)
            continue
        response.raise_for_status()

        page_activities = response.json()
        if not page_activities:
            break

        print(f"DEBUG: Page {page} → {len(page_activities)} activities")
        activities.extend(page_activities)
        if len(page_activities) < per_page:
            break
        page += 1

    print(f"DEBUG: Total {len(activities)} activities fetched")
    return activities

def prepare_for_supabase(activities: list[dict]) -> list[dict]:
    """Haal alleen de kolommen die in Supabase-tabel bestaan"""
    prepared = []
    for act in activities:
        start_latlng = act.get("start_latlng") or []
        end_latlng = act.get("end_latlng") or []

        prepared.append({
            "id": act.get("id"),
            "name": act.get("name"),
            "type": act.get("type"),
            "start_date": act.get("start_date"),
            "distance": float(act.get("distance", 0)),
            "moving_time": int(act.get("moving_time", 0)),
            "elapsed_time": int(act.get("elapsed_time", 0)),
            "total_elevation_gain": float(act.get("total_elevation_gain", 0)),
            "average_speed": float(act.get("average_speed", 0)),
            "max_speed": float(act.get("max_speed", 0)),
            "average_heartrate": act.get("average_heartrate"),
            "max_heartrate": act.get("max_heartrate"),
            "start_latitude": start_latlng[0] if len(start_latlng) > 0 else None,
            "start_longitude": start_latlng[1] if len(start_latlng) > 1 else None,
            "end_latitude": end_latlng[0] if len(end_latlng) > 0 else None,
            "end_longitude": end_latlng[1] if len(end_latlng) > 1 else None,
        })
    return prepared

def upload_to_supabase(activities: list[dict]):
    """Upload activiteiten naar Supabase via REST API"""
    supabase_url = env("SUPABASE_URL")
    supabase_key = env("SUPABASE_KEY")
    table = "strava_activities"

    headers = {
        "apikey": supabase_key,
        "Authorization": f"Bearer {supabase_key}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates"  # upsert
    }

    data = json.dumps(activities)
    response = requests.post(f"{supabase_url}/rest/v1/{table}", headers=headers, data=data, timeout=30)
    if not response.ok:
        print(f"DEBUG: Upload error: {response.status_code} {response.text}")
    else:
        print(f"DEBUG: Uploaded {len(activities)} activities to Supabase")

# -------------------------------
# Main
# -------------------------------

def main():
    print("DEBUG: Start sync - laatste 30 dagen")
    token = get_access_token()
    activities = fetch_recent_activities(token, days=30)
    if not activities:
        print("DEBUG: Geen nieuwe activiteiten gevonden")
        return

    prepared = prepare_for_supabase(activities)
    upload_to_supabase(prepared)
    print("DEBUG: Sync afgerond ✅")

if __name__ == "__main__":
    main()
