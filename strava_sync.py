import os
import json
import time
from datetime import datetime, timedelta
import requests
from supabase import create_client, Client

# ---- Helpers ----
def env(name: str, required: bool = True, default: str | None = None) -> str | None:
    value = os.environ.get(name, default)
    if required and (value is None or value == ""):
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value

# ---- Strava OAuth ----
def get_access_token() -> str:
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
    resp = requests.post(url, data=payload, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    # Update refresh token if veranderd
    new_refresh = data.get("refresh_token")
    if new_refresh and new_refresh != refresh_token:
        print("DEBUG: New refresh token available, update your secret")
        with open("new_refresh_token.txt", "w") as f:
            f.write(new_refresh)
    return data["access_token"]

# ---- Fetch activiteiten ----
def fetch_recent_activities(access_token: str, days: int = 30) -> list[dict]:
    activities = []
    page = 1
    per_page = 200
    after_timestamp = int((datetime.now() - timedelta(days=days)).timestamp())
    headers = {"Authorization": f"Bearer {access_token}"}

    print(f"DEBUG: Fetching activities from last {days} days")
    while True:
        params = {"page": page, "per_page": per_page, "after": after_timestamp}
        r = requests.get("https://www.strava.com/api/v3/athlete/activities", headers=headers, params=params, timeout=60)

        if r.status_code == 429:
            print("DEBUG: Rate limit, waiting 60s...")
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

    print(f"DEBUG: Totaal {len(activities)} activiteiten opgehaald")
    return activities

# ---- Upload naar Supabase ----
def upload_to_supabase(activities: list[dict]) -> None:
    url = env("SUPABASE_URL")
    key = env("SUPABASE_KEY")
    supabase: Client = create_client(url, key)
    inserted = 0

    for act in activities:
        start_latlng = act.get("start_latlng") or [None, None]
        end_latlng = act.get("end_latlng") or [None, None]

        data = {
            "id": act.get("id"),
            "name": act.get("name"),
            "type": act.get("type"),
            "start_date": act.get("start_date"),
            "distance": act.get("distance"),
            "moving_time": int(act.get("moving_time", 0)),  # in seconden
            "elapsed_time": int(act.get("elapsed_time", 0)),  # in seconden
            "total_elevation_gain": act.get("total_elevation_gain"),
            "average_speed": act.get("average_speed"),
            "max_speed": act.get("max_speed"),
            "average_heartrate": act.get("average_heartrate"),
            "max_heartrate": act.get("max_heartrate"),
            "start_latitude": start_latlng[0],
            "start_longitude": start_latlng[1],
            "end_latitude": end_latlng[0],
            "end_longitude": end_latlng[1],
            "timezone": act.get("timezone"),
            "utc_offset": act.get("utc_offset"),
            "kudos_count": act.get("kudos_count", 0),
            "comment_count": act.get("comment_count", 0),
            "gear_id": act.get("gear_id"),
            "trainer": act.get("trainer", False),
            "commute": act.get("commute", False),
            "private": act.get("private", False),
            "description": act.get("description"),
        }

        try:
            res = supabase.table("strava_activities").upsert(data).execute()
            if res.status_code >= 400:
                print(f"DEBUG: Upload failed: {res.data}")
            else:
                inserted += 1
        except Exception as e:
            print(f"DEBUG: Upload error: {e}")

    print(f"DEBUG: {inserted} activiteiten geüpload naar Supabase")

# ---- Main ----
def main():
    days = int(os.environ.get("DAYS_BACK", "30"))
    token = get_access_token()
    activities = fetch_recent_activities(token, days=days)

    if not activities:
        print("DEBUG: Geen nieuwe activiteiten gevonden")
        return

    upload_to_supabase(activities)

    # Optioneel: lokaal opslaan
    csv_file = os.environ.get("CSV_FILE", "activiteiten.csv")
    json_file = os.environ.get("JSON_FILE", "activiteiten_raw.json")
    with open(json_file, "w", encoding="utf-8") as f:
        json.dump(activities, f)
    print(f"DEBUG: {len(activities)} activiteiten opgeslagen in {json_file}")

if __name__ == "__main__":
    print(f"DEBUG: Start sync - laatste {os.environ.get('DAYS_BACK', 30)} dagen")
    main()
    print("DEBUG: Sync afgerond ✅")
