import os
import csv
import json
import time
from datetime import datetime, timedelta

import requests
from supabase import create_client, Client

def env(name: str, required: bool = True, default: str | None = None) -> str | None:
    value = os.environ.get(name, default)
    if required and (value is None or value == ""):
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value

def get_access_token() -> str:
    """Haalt een geldig access token op via refresh token."""
    client_id = env("STRAVA_CLIENT_ID")
    client_secret = env("STRAVA_CLIENT_SECRET")
    refresh_token = env("STRAVA_REFRESH_TOKEN")

    url = "https://www.strava.com/oauth/token"
    payload = {
        "client_id": client_id,
        "client_secret": client_secret,
        "refresh_token": refresh_token,
        "grant_type": "refresh_token",
    }

    response = requests.post(url, data=payload, timeout=30)
    response.raise_for_status()
    data = response.json()

    # Optioneel: nieuwe refresh token terugschrijven
    new_refresh = data.get("refresh_token")
    if new_refresh:
        with open("new_refresh_token.txt", "w") as f:
            f.write(new_refresh)
        print("DEBUG: New refresh token available, update your secret")

    return data["access_token"]

def fetch_recent_activities(access_token: str, days: int = 30) -> list[dict]:
    activities = []
    page = 1
    per_page = 200
    after_timestamp = int((datetime.now() - timedelta(days=days)).timestamp())

    print(f"DEBUG: Fetching activities from last {days} days")

    while True:
        url = "https://www.strava.com/api/v3/athlete/activities"
        headers = {"Authorization": f"Bearer {access_token}"}
        params = {"page": page, "per_page": per_page, "after": after_timestamp}

        response = requests.get(url, headers=headers, params=params, timeout=60)

        if response.status_code == 429:
            print("DEBUG: Rate limit hit, waiting 60s...")
            time.sleep(60)
            continue

        if response.status_code == 401:
            raise RuntimeError("401 Unauthorized - token may be invalid")

        response.raise_for_status()
        page_activities = response.json()

        if not page_activities:
            break

        activities.extend(page_activities)
        print(f"DEBUG: Page {page} → {len(page_activities)} activities")
        if len(page_activities) < per_page:
            break
        page += 1

    print(f"DEBUG: Total {len(activities)} activities fetched")
    return activities

def save_to_csv(activities: list[dict], filename: str) -> None:
    if not activities:
        return

    fieldnames = [
        "ID", "Naam", "Datum", "Type", "Afstand (m)", "Moving Time (s)",
        "Hoogtemeters", "Gemiddelde snelheid (m/s)", "Max snelheid (m/s)",
        "Gemiddelde hartslag", "Max hartslag"
    ]

    file_exists = os.path.exists(filename)
    existing_ids = set()
    if file_exists:
        with open(filename, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                existing_ids.add(row["ID"])

    new_rows = []
    for act in activities:
        act_id = str(act.get("id"))
        if act_id in existing_ids:
            continue

        new_rows.append({
            "ID": act_id,
            "Naam": act.get("name"),
            "Datum": act.get("start_date"),
            "Type": act.get("type"),
            "Afstand (m)": act.get("distance"),
            "Moving Time (s)": act.get("moving_time"),
            "Hoogtemeters": act.get("total_elevation_gain"),
            "Gemiddelde snelheid (m/s)": act.get("average_speed"),
            "Max snelheid (m/s)": act.get("max_speed"),
            "Gemiddelde hartslag": act.get("average_heartrate"),
            "Max hartslag": act.get("max_heartrate"),
        })

    mode = "a" if file_exists else "w"
    with open(filename, mode, newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        writer.writerows(new_rows)

    print(f"DEBUG: Saved {len(new_rows)} new activities to {filename}")

def save_to_json(activities: list[dict], filename: str) -> None:
    if not activities:
        return

    existing = []
    if os.path.exists(filename):
        try:
            with open(filename, "r", encoding="utf-8") as f:
                existing = json.load(f)
        except Exception:
            existing = []

    existing.extend(activities)
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(existing, f)

    print(f"DEBUG: Saved {len(activities)} activities to {filename}")

def upload_to_supabase(activities: list[dict]) -> None:
    url = env("SUPABASE_URL")
    key = env("SUPABASE_KEY")
    supabase: Client = create_client(url, key)

    if not activities:
        print("DEBUG: Geen activiteiten om te uploaden")
        return

    # Prepare data for Supabase table
    prepared = []
    for act in activities:
        start_latlng = act.get("start_latlng", [])
        end_latlng = act.get("end_latlng", [])
        prepared.append({
            "id": act.get("id"),
            "name": act.get("name"),
            "type": act.get("type"),
            "start_date": act.get("start_date"),
            "distance": act.get("distance"),
            "moving_time": act.get("moving_time"),
            "total_elevation_gain": act.get("total_elevation_gain"),
            "average_speed": act.get("average_speed"),
            "max_speed": act.get("max_speed"),
            "average_heartrate": act.get("average_heartrate"),
            "max_heartrate": act.get("max_heartrate"),
            "start_latitude": start_latlng[0] if start_latlng else None,
            "start_longitude": start_latlng[1] if start_latlng and len(start_latlng) > 1 else None,
            "end_latitude": end_latlng[0] if end_latlng else None,
            "end_longitude": end_latlng[1] if end_latlng and len(end_latlng) > 1 else None,
        })

    try:
        resp = supabase.table("strava_activities").upsert(prepared, on_conflict="id").execute()
        print(f"DEBUG: {len(prepared)} activiteiten geüpload naar Supabase")
    except Exception as e:
        print("DEBUG: Upload failed:", e)

def main():
    csv_file = os.environ.get("CSV_FILE", "activiteiten.csv")
    json_file = os.environ.get("JSON_FILE", "activiteiten_raw.json")
    days = int(os.environ.get("DAYS_BACK", "30"))

    print(f"DEBUG: Start sync - laatste {days} dagen")
    token = get_access_token()
    activities = fetch_recent_activities(token, days=days)

    save_to_
