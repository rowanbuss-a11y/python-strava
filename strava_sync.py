import os
import json
import requests
from supabase import create_client, Client

def env(name: str, required=True):
    value = os.environ.get(name)
    if required and not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value

# --- Supabase client ---
SUPABASE_URL = env("SUPABASE_URL")
SUPABASE_KEY = env("SUPABASE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# --- Strava config ---
STRAVA_CLIENT_ID = env("STRAVA_CLIENT_ID")
STRAVA_CLIENT_SECRET = env("STRAVA_CLIENT_SECRET")
STRAVA_REFRESH_TOKEN = env("STRAVA_REFRESH_TOKEN")

def get_access_token():
    url = "https://www.strava.com/oauth/token"
    payload = {
        "client_id": STRAVA_CLIENT_ID,
        "client_secret": STRAVA_CLIENT_SECRET,
        "grant_type": "refresh_token",
        "refresh_token": STRAVA_REFRESH_TOKEN
    }
    resp = requests.post(url, data=payload)
    resp.raise_for_status()
    data = resp.json()
    new_refresh = data.get("refresh_token")
    if new_refresh and new_refresh != STRAVA_REFRESH_TOKEN:
        print("DEBUG: New refresh token available, update your secret")
        with open("new_refresh_token.txt", "w") as f:
            f.write(new_refresh)
    return data["access_token"]

def fetch_recent_activities(access_token, days=30):
    from datetime import datetime, timedelta
    after = int((datetime.now() - timedelta(days=days)).timestamp())
    activities = []
    page = 1
    while True:
        url = "https://www.strava.com/api/v3/athlete/activities"
        params = {"after": after, "page": page, "per_page": 200}
        headers = {"Authorization": f"Bearer {access_token}"}
        resp = requests.get(url, headers=headers, params=params)
        if resp.status_code == 429:
            import time; time.sleep(60)
            continue
        resp.raise_for_status()
        page_activities = resp.json()
        if not page_activities:
            break
        activities.extend(page_activities)
        page += 1
    return activities

def upload_to_supabase(activities):
    for act in activities:
        start_latlng = act.get("start_latlng") or [None, None]
        end_latlng = act.get("end_latlng") or [None, None]
        data = {
            "id": act.get("id"),
            "name": act.get("name"),
            "type": act.get("type"),
            "start_date": act.get("start_date"),
            "distance": act.get("distance"),
            "moving_time": act.get("moving_time"),  # in seconden
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
            "kudos_count": act.get("kudos_count"),
            "comment_count": act.get("comment_count"),
            "gear_id": act.get("gear_id"),
            "trainer": act.get("trainer"),
            "commute": act.get("commute"),
            "private": act.get("private"),
            "description": act.get("description"),
        }
        resp = supabase.table("strava_activities").upsert(data, on_conflict="id").execute()
        if resp.status_code != 200:
            print("DEBUG: Upload failed:", resp.data)

def save_backup(activities):
    with open("activiteiten_raw.json", "w") as f:
        json.dump(activities, f)
    import csv
    keys = ["id","name","type","start_date","distance","moving_time","total_elevation_gain",
            "average_speed","max_speed","average_heartrate","max_heartrate"]
    with open("activiteiten.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        for act in activities:
            writer.writerow({k: act.get(k) for k in keys})

def main():
    print("DEBUG: Start sync - laatste 30 dagen")
    token = get_access_token()
    activities = fetch_recent_activities(token)
    print(f"DEBUG: Total {len(activities)} activities fetched")
    save_backup(activities)
    upload_to_supabase(activities)
    print("DEBUG: Sync afgerond ✅")

if __name__ == "__main__":
    main()
