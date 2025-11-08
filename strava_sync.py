import os
import json
import requests
from supabase import create_client, Client

def env(name: str, required: bool = True, default: str | None = None):
    value = os.environ.get(name, default)
    if required and not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value

SUPABASE_URL = env("SUPABASE_URL")
SUPABASE_KEY = env("SUPABASE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

STRAVA_ACCESS_TOKEN = env("STRAVA_ACCESS_TOKEN", required=False)
STRAVA_CLIENT_ID = env("STRAVA_CLIENT_ID")
STRAVA_CLIENT_SECRET = env("STRAVA_CLIENT_SECRET")
STRAVA_REFRESH_TOKEN = env("STRAVA_REFRESH_TOKEN")
DAYS_BACK = int(os.environ.get("DAYS_BACK", "30"))

def get_access_token():
    if STRAVA_ACCESS_TOKEN:
        return STRAVA_ACCESS_TOKEN

    payload = {
        "client_id": STRAVA_CLIENT_ID,
        "client_secret": STRAVA_CLIENT_SECRET,
        "refresh_token": STRAVA_REFRESH_TOKEN,
        "grant_type": "refresh_token",
    }
    response = requests.post("https://www.strava.com/oauth/token", data=payload, timeout=30)
    response.raise_for_status()
    data = response.json()
    return data["access_token"]

def fetch_recent_activities(token, days=DAYS_BACK):
    from datetime import datetime, timedelta
    activities = []
    page = 1
    per_page = 200
    after_timestamp = int((datetime.now() - timedelta(days=days)).timestamp())

    headers = {"Authorization": f"Bearer {token}"}

    while True:
        url = "https://www.strava.com/api/v3/athlete/activities"
        params = {"after": after_timestamp, "page": page, "per_page": per_page}
        r = requests.get(url, headers=headers, params=params, timeout=60)
        if r.status_code != 200:
            raise RuntimeError(f"Error fetching activities: {r.status_code} {r.text}")
        page_activities = r.json()
        if not page_activities:
            break
        activities.extend(page_activities)
        if len(page_activities) < per_page:
            break
        page += 1
    return activities

def prepare_for_supabase(act):
    start_latlng = act.get("start_latlng") or [None, None]
    end_latlng = act.get("end_latlng") or [None, None]

    return {
        "id": int(act.get("id")),
        "name": act.get("name"),
        "type": act.get("type"),
        "start_date": act.get("start_date"),
        "distance": float(act.get("distance", 0)),
        "moving_time": float(act.get("moving_time", 0)),
        "total_elevation_gain": float(act.get("total_elevation_gain", 0)),
        "average_speed": float(act.get("average_speed", 0)),
        "max_speed": float(act.get("max_speed", 0)),
        "average_heartrate": float(act.get("average_heartrate") or 0),
        "max_heartrate": float(act.get("max_heartrate") or 0),
        "start_latitude": float(start_latlng[0]) if start_latlng[0] is not None else None,
        "start_longitude": float(start_latlng[1]) if start_latlng[1] is not None else None,
        "end_latitude": float(end_latlng[0]) if end_latlng[0] is not None else None,
        "end_longitude": float(end_latlng[1]) if end_latlng[1] is not None else None,
        "timezone": act.get("timezone"),
        "utc_offset": float(act.get("utc_offset") or 0),
        "kudos_count": int(act.get("kudos_count") or 0),
        "comment_count": int(act.get("comment_count") or 0),
        "gear_id": act.get("gear_id"),
        "trainer": act.get("trainer"),
        "commute": act.get("commute"),
        "private": act.get("private"),
        "description": act.get("description"),
    }

def upload_to_supabase(activities):
    prepared = [prepare_for_supabase(act) for act in activities]
    for data in prepared:
        resp = supabase.table("strava_activities").upsert(data, on_conflict="id").execute()
        if resp.status_code != 200:
            print("DEBUG: Upload failed:", resp.data)

def main():
    print(f"DEBUG: Start sync - laatste {DAYS_BACK} dagen")
    token = get_access_token()
    activities = fetch_recent_activities(token)
    print(f"DEBUG: Totaal {len(activities)} activiteiten opgehaald")
    if activities:
        upload_to_supabase(activities)
    print("DEBUG: Sync afgerond ✅")

if __name__ == "__main__":
    main()
