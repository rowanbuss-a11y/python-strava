"""
Strava sync script - laatste 30 dagen
Haal recente activiteiten op en upload rechtstreeks naar Supabase
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

def get_supabase_client() -> Client:
    url = env("SUPABASE_URL")
    key = env("SUPABASE_KEY")
    return create_client(url, key)

def get_access_token() -> str:
    """Haalt een geldig access token op via refresh token"""
    client_id = env("STRAVA_CLIENT_ID")
    client_secret = env("STRAVA_CLIENT_SECRET")
    refresh_token = env("STRAVA_REFRESH_TOKEN")

    url = "https://www.strava.com/oauth/token"
    payload = {
        "client_id": client_id,
        "client_secret": client_secret,
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
    }

    r = requests.post(url, data=payload, timeout=30)
    r.raise_for_status()
    data = r.json()

    new_refresh = data.get("refresh_token")
    if new_refresh and new_refresh != refresh_token:
        print("DEBUG: New refresh token available, update your secret")

    return data["access_token"]

def fetch_recent_activities(access_token: str, days: int = 30, max_retries: int = 5) -> list[dict]:
    """Haal activiteiten van de laatste N dagen op, met retries"""
    activities = []
    page = 1
    per_page = 200
    after_timestamp = int((datetime.now() - timedelta(days=days)).timestamp())

    print(f"DEBUG: Fetching activities from last {days} days")

    while True:
        url = "https://www.strava.com/api/v3/athlete/activities"
        headers = {"Authorization": f"Bearer {access_token}"}
        params = {"after": after_timestamp, "page": page, "per_page": per_page}

        retries = 0
        while retries < max_retries:
            try:
                r = requests.get(url, headers=headers, params=params, timeout=60)
                if r.status_code == 429:
                    print("DEBUG: Rate limit, wacht 60s...")
                    time.sleep(60)
                    continue
                r.raise_for_status()
                break
            except requests.exceptions.RequestException as e:
                retries += 1
                wait = 2 ** retries
                print(f"DEBUG: Fout bij ophalen (retry {retries}/{max_retries} in {wait}s): {e}")
                time.sleep(wait)
        else:
            print(f"DEBUG: Max retries bereikt voor pagina {page}, stop fetching")
            break

        page_activities = r.json()
        if not page_activities:
            break

        print(f"DEBUG: Pagina {page} → {len(page_activities)} activiteiten")
        activities.extend(page_activities)
        page += 1

    print(f"DEBUG: Totaal {len(activities)} activiteiten opgehaald")
    return activities

def prepare_for_supabase(activity: dict) -> dict:
    """Zet activiteiten klaar voor Supabase upload, cast types correct"""
    start_latlng = activity.get("start_latlng") or [None, None]
    end_latlng = activity.get("end_latlng") or [None, None]

    def safe_int(value):
        if value is None:
            return None
        return int(round(value))

    def safe_float(value):
        if value is None:
            return None
        return float(value)

    return {
        "id": activity.get("id"),
        "name": activity.get("name"),
        "type": activity.get("type"),
        "start_date": activity.get("start_date"),
        "distance": safe_float(activity.get("distance")),
        "moving_time": safe_int(activity.get("moving_time")),
        "elapsed_time": safe_int(activity.get("elapsed_time")),
        "total_elevation_gain": safe_float(activity.get("total_elevation_gain")),
        "average_speed": safe_float(activity.get("average_speed")),
        "max_speed": safe_float(activity.get("max_speed")),
        "average_heartrate": safe_float(activity.get("average_heartrate")),
        "max_heartrate": safe_float(activity.get("max_heartrate")),
        "start_latitude": safe_float(start_latlng[0]),
        "start_longitude": safe_float(start_latlng[1]),
        "end_latitude": safe_float(end_latlng[0]),
        "end_longitude": safe_float(end_latlng[1]),
        "timezone": activity.get("timezone"),
        "utc_offset": safe_float(activity.get("utc_offset")),
        "kudos_count": activity.get("kudos_count", 0),
        "comment_count": activity.get("comment_count", 0),
        "gear_id": activity.get("gear_id"),
        "trainer": activity.get("trainer", False),
        "commute": activity.get("commute", False),
        "private": activity.get("private", False),
        "description": activity.get("description"),
    }

def upload_to_supabase(prepared_activities: list[dict]) -> None:
    if not prepared_activities:
        print("DEBUG: Geen nieuwe activiteiten om te uploaden")
        return

    client = get_supabase_client()
    table_name = "strava_activities"

    for act in prepared_activities:
        try:
            resp = client.table(table_name).upsert(act, on_conflict="id").execute()
            if resp.status_code >= 400:
                print(f"DEBUG: Upload error: {resp.status_code} {resp.json()}")
        except Exception as e:
            print(f"DEBUG: Upload failed: {e}")

    print(f"DEBUG: {len(prepared_activities)} activiteiten geüpload naar Supabase")

def save_to_csv(activities: list[dict], filename: str) -> None:
    if not activities:
        return

    fieldnames = ["ID", "Naam", "Datum", "Type", "Afstand (km)", "Tijd (min)", "Totale tijd (min)",
                  "Hoogtemeters", "Gemiddelde snelheid (km/u)", "Max snelheid (km/u)",
                  "Gemiddelde hartslag", "Max hartslag"]
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for act in activities:
            writer.writerow({
                "ID": act.get("id"),
                "Naam": act.get("name"),
                "Datum": act.get("start_date"),
                "Type": act.get("type"),
                "Afstand (km)": round(act.get("distance", 0) / 1000, 2),
                "Tijd (min)": round(act.get("moving_time", 0) / 60, 2),
                "Totale tijd (min)": round(act.get("elapsed_time", 0) / 60, 2),
                "Hoogtemeters": act.get("total_elevation_gain"),
                "Gemiddelde snelheid (km/u)": round(act.get("average_speed", 0) * 3.6, 2),
                "Max snelheid (km/u)": round(act.get("max_speed", 0) * 3.6, 2),
                "Gemiddelde hartslag": act.get("average_heartrate"),
                "Max hartslag": act.get("max_heartrate"),
            })

def save_to_json(activities: list[dict], filename: str) -> None:
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(activities, f)

def main() -> None:
    csv_file = os.environ.get("CSV_FILE", "activiteiten.csv")
    json_file = os.environ.get("JSON_FILE", "activiteiten_raw.json")
    days = int(os.environ.get("DAYS_BACK", "30"))

    print(f"DEBUG: Start sync - laatste {days} dagen")

    token = get_access_token()
    activities = fetch_recent_activities(token, days=days)

    if not activities:
        print("DEBUG: Geen nieuwe activiteiten gevonden")
        return

    save_to_csv(activities, csv_file)
    save_to_json(activities, json_file)

    prepared = [prepare_for_supabase(act) for act in activities]
    upload_to_supabase(prepared)

    print("DEBUG: Sync afgerond ✅")

if __name__ == "__main__":
    main()
