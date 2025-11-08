"""
Strava sync script - laatste 30 dagen
Slaat activiteiten op in CSV, JSON en Supabase
"""
import os
import csv
import json
import time
from datetime import datetime, timedelta

import requests

def env(name: str, required: bool = True, default: str | None = None) -> str | None:
    value = os.environ.get(name, default)
    if required and (value is None or value == ""):
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value

def get_access_token() -> str:
    """Haal een geldig Strava access token op via refresh token"""
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
    r = requests.post(url, data=payload, timeout=30)
    r.raise_for_status()
    data = r.json()

    new_refresh = data.get("refresh_token")
    if new_refresh:
        # Schrijf nieuwe refresh token naar file als check
        with open("new_refresh_token.txt", "w") as f:
            f.write(new_refresh)
        print("DEBUG: New refresh token available, update your secret")

    return data["access_token"]

def fetch_recent_activities(access_token: str, days: int = 30) -> list[dict]:
    """Haal activiteiten van de laatste N dagen op"""
    activities = []
    page = 1
    per_page = 200
    after_timestamp = int((datetime.now() - timedelta(days=days)).timestamp())

    while True:
        url = "https://www.strava.com/api/v3/athlete/activities"
        headers = {"Authorization": f"Bearer {access_token}"}
        params = {"page": page, "per_page": per_page, "after": after_timestamp}
        r = requests.get(url, headers=headers, params=params, timeout=60)

        if r.status_code == 429:
            print("DEBUG: Rate limit hit, waiting 60s...")
            time.sleep(60)
            continue
        r.raise_for_status()
        page_activities = r.json()
        if not page_activities:
            break
        activities.extend(page_activities)
        page += 1

    print(f"DEBUG: Total {len(activities)} activities fetched")
    return activities

def save_to_csv(activities: list[dict], filename: str) -> None:
    """Sla activiteiten op in CSV"""
    if not activities:
        return

    fieldnames = [
        "ID", "Naam", "Datum", "Type", "Afstand (km)", "Tijd (min)",
        "Totale tijd (min)", "Hoogtemeters", "Gemiddelde snelheid (km/u)",
        "Max snelheid (km/u)", "Gemiddelde hartslag", "Max hartslag"
    ]

    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for act in activities:
            writer.writerow({
                "ID": act.get("id"),
                "Naam": act.get("name"),
                "Datum": act.get("start_date"),
                "Type": act.get("type"),
                "Afstand (km)": round(float(act.get("distance", 0)) / 1000, 2),
                "Tijd (min)": int(act.get("moving_time", 0) / 60),
                "Totale tijd (min)": int(act.get("elapsed_time", 0) / 60),
                "Hoogtemeters": round(float(act.get("total_elevation_gain", 0)), 2),
                "Gemiddelde snelheid (km/u)": round(float(act.get("average_speed", 0)) * 3.6, 2),
                "Max snelheid (km/u)": round(float(act.get("max_speed", 0)) * 3.6, 2),
                "Gemiddelde hartslag": float(act.get("average_heartrate")) if act.get("average_heartrate") else None,
                "Max hartslag": float(act.get("max_heartrate")) if act.get("max_heartrate") else None
            })
    print(f"DEBUG: Saved {len(activities)} activities to {filename}")

def save_to_json(activities: list[dict], filename: str) -> None:
    """Sla activiteiten op in JSON"""
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(activities, f)
    print(f"DEBUG: Saved {len(activities)} activities to {filename}")

def upload_to_supabase(activities: list[dict]) -> None:
    """Upload activiteiten naar Supabase"""
    supabase_url = env("SUPABASE_URL")
    supabase_key = env("SUPABASE_KEY")
    headers = {
        "apikey": supabase_key,
        "Authorization": f"Bearer {supabase_key}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates"
    }

    prepared = []
    for act in activities:
        try:
            prepared.append({
                "id": int(act.get("id")),
                "name": act.get("name") or "",
                "type": act.get("type") or "",
                "start_date": act.get("start_date"),
                "distance": round(float(act.get("distance", 0)) / 1000, 2),
                "moving_time": int(act.get("moving_time", 0)),
                "elapsed_time": int(act.get("elapsed_time", 0)),
                "total_elevation_gain": round(float(act.get("total_elevation_gain", 0)), 2),
                "average_speed": round(float(act.get("average_speed", 0)) * 3.6, 2),
                "max_speed": round(float(act.get("max_speed", 0)) * 3.6, 2),
                "average_heartrate": float(act.get("average_heartrate")) if act.get("average_heartrate") else None,
                "max_heartrate": float(act.get("max_heartrate")) if act.get("max_heartrate") else None,
                "start_latitude": float(act.get("start_latlng")[0]) if act.get("start_latlng") else None,
                "start_longitude": float(act.get("start_latlng")[1]) if act.get("start_latlng") else None,
                "end_latitude": float(act.get("end_latlng")[0]) if act.get("end_latlng") else None,
                "end_longitude": float(act.get("end_latlng")[1]) if act.get("end_latlng") else None,
                "timezone": act.get("timezone") or "",
                "utc_offset": int(act.get("utc_offset", 0)),
                "kudos_count": int(act.get("kudos_count", 0)),
                "comment_count": int(act.get("comment_count", 0)),
                "gear_id": act.get("gear_id"),
                "trainer": bool(act.get("trainer", False)),
                "commute": bool(act.get("commute", False)),
                "private": bool(act.get("private", False)),
                "description": act.get("description") or ""
            })
        except Exception as e:
            print(f"DEBUG: Error preparing activity {act.get('id')}: {e}")

    if not prepared:
        print("DEBUG: No activities to upload")
        return

    r = requests.post(f"{supabase_url}/rest/v1/strava_activities", headers=headers, data=json.dumps(prepared))
    if r.status_code >= 400:
        print(f"DEBUG: Upload error: {r.status_code} {r.text}")
    else:
        print(f"DEBUG: {len(prepared)} activities successfully uploaded to Supabase")

def main() -> None:
    days = int(os.environ.get("DAYS_BACK", "30"))
    csv_file = os.environ.get("CSV_FILE", "activiteiten.csv")
    json_file = os.environ.get("JSON_FILE", "activiteiten_raw.json")

    print(f"DEBUG: Start sync - laatste {days} dagen")

    token = get_access_token()
    activities = fetch_recent_activities(token, days=days)

    if not activities:
        print("DEBUG: Geen nieuwe activiteiten")
        return

    save_to_csv(activities, csv_file)
    save_to_json(activities, json_file)
    upload_to_supabase(activities)
    print("DEBUG: Sync afgerond ✅")

if __name__ == "__main__":
    main()
