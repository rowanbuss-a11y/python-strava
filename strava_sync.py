"""
Strava sync script - laatste 30 dagen
Uploadt direct naar Supabase
"""
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
    """Haal een geldig access token op via refresh token of auth code"""
    client_id = env("STRAVA_CLIENT_ID")
    client_secret = env("STRAVA_CLIENT_SECRET")
    refresh_token = os.environ.get("STRAVA_REFRESH_TOKEN")
    auth_code = os.environ.get("STRAVA_AUTH_CODE")
    redirect_uri = os.environ.get("STRAVA_REDIRECT_URI")

    # Refresh token flow
    if refresh_token:
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
        if new_refresh and new_refresh != refresh_token:
            print("DEBUG: New refresh token available, update your secret")
            with open("new_refresh_token.txt", "w") as f:
                f.write(new_refresh)
        return data["access_token"]

    # Fallback: auth code
    if auth_code:
        url = "https://www.strava.com/oauth/token"
        payload = {
            "client_id": client_id,
            "client_secret": client_secret,
            "code": auth_code,
            "grant_type": "authorization_code",
            "redirect_uri": redirect_uri,
        }
        r = requests.post(url, data=payload, timeout=30)
        r.raise_for_status()
        data = r.json()
        new_refresh = data.get("refresh_token")
        if new_refresh:
            print("DEBUG: New refresh token obtained from auth code")
            with open("new_refresh_token.txt", "w") as f:
                f.write(new_refresh)
        return data["access_token"]

    raise RuntimeError("No valid refresh token or auth code available")

def fetch_recent_activities(access_token: str, days: int = 30) -> list[dict]:
    """Haal activiteiten op van de laatste N dagen"""
    activities = []
    page = 1
    per_page = 200
    after_timestamp = int((datetime.now() - timedelta(days=days)).timestamp())

    print(f"DEBUG: Fetching activities from last {days} days")

    while True:
        url = "https://www.strava.com/api/v3/athlete/activities"
        headers = {"Authorization": f"Bearer {access_token}"}
        params = {"page": page, "per_page": per_page, "after": after_timestamp}
        r = requests.get(url, headers=headers, params=params, timeout=60)

        if r.status_code == 429:
            print("DEBUG: Rate limit reached, waiting 60s...")
            time.sleep(60)
            continue
        r.raise_for_status()
        page_activities = r.json()
        if not page_activities:
            break
        activities.extend(page_activities)
        print(f"DEBUG: Page {page} → {len(page_activities)} activiteiten")
        if len(page_activities) < per_page:
            break
        page += 1

    print(f"DEBUG: Total {len(activities)} activiteiten fetched")
    return activities

def prepare_activities(activities: list[dict]) -> list[dict]:
    """Haal relevante velden eruit en pas types aan voor Supabase"""
    prepared = []
    for act in activities:
        start_latlng = act.get("start_latlng") or []
        end_latlng = act.get("end_latlng") or []
        prepared.append({
            "id": act.get("id"),
            "name": act.get("name"),
            "type": act.get("type"),
            "start_date": act.get("start_date"),
            "distance": act.get("distance"),
            "moving_time": act.get("moving_time"),  # seconds, keep as is
            "total_elevation_gain": act.get("total_elevation_gain"),
            "average_speed": act.get("average_speed"),
            "max_speed": act.get("max_speed"),
            "start_latitude": start_latlng[0] if len(start_latlng) > 0 else None,
            "start_longitude": start_latlng[1] if len(start_latlng) > 1 else None,
            "end_latitude": end_latlng[0] if len(end_latlng) > 0 else None,
            "end_longitude": end_latlng[1] if len(end_latlng) > 1 else None,
            "timezone": act.get("timezone"),
            "utc_offset": act.get("utc_offset"),
            "kudos_count": act.get("kudos_count"),
            "comment_count": act.get("comment_count"),
            "gear_id": act.get("gear_id"),
            "trainer": act.get("trainer"),
            "commute": act.get("commute"),
            "private": act.get("private"),
            "description": act.get("description"),
        })
    return prepared

def upload_to_supabase(data: list[dict]) -> None:
    url = env("SUPABASE_URL")
    key = env("SUPABASE_KEY")
    supabase: Client = create_client(url, key)

    if not data:
        print("DEBUG: Geen activiteiten om te uploaden")
        return

    resp = supabase.table("strava_activities").upsert(data, on_conflict="id").execute()
    if resp.error:
        print("DEBUG: Upload failed:", resp.error)
    else:
        print(f"DEBUG: {len(data)} activiteiten geüpload naar Supabase")

def save_to_csv(activities: list[dict], filename: str) -> None:
    if not activities:
        return
    fieldnames = ["ID", "Naam", "Datum", "Type", "Afstand (km)", "Tijd (s)"]
    file_exists = os.path.exists(filename)
    with open(filename, "a" if file_exists else "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        for act in activities:
            writer.writerow({
                "ID": act.get("id"),
                "Naam": act.get("name"),
                "Datum": act.get("start_date"),
                "Type": act.get("type"),
                "Afstand (km)": round(act.get("distance", 0) / 1000, 2),
                "Tijd (s)": act.get("moving_time"),
            })

def save_to_json(activities: list[dict], filename: str) -> None:
    if not activities:
        return
    existing = []
    if os.path.exists(filename):
        with open(filename, "r", encoding="utf-8") as f:
            try:
                existing = json.load(f)
            except Exception:
                existing = []
    existing.extend(activities)
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(existing, f)
    print(f"DEBUG: Saved {len(activities)} activities to {filename}")

def main() -> None:
    days = int(os.environ.get("DAYS_BACK", "30"))
    csv_file = os.environ.get("CSV_FILE", "activiteiten.csv")
    json_file = os.environ.get("JSON_FILE", "activiteiten_raw.json")

    print(f"DEBUG: Start sync - laatste {days} dagen")
    token = get_access_token()
    activities = fetch_recent_activities(token, days=days)
    prepared = prepare_activities(activities)

    save_to_csv(prepared, csv_file)
    save_to_json(prepared, json_file)
    upload_to_supabase(prepared)

    print("DEBUG: Sync afgerond ✅")

if __name__ == "__main__":
    main()
