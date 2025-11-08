import os
import requests
import json
from datetime import datetime, timedelta
from supabase import create_client, Client
import time

# --------------------------------------------------
# Configuratie
# --------------------------------------------------
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
STRAVA_CLIENT_ID = os.environ.get("STRAVA_CLIENT_ID")
STRAVA_CLIENT_SECRET = os.environ.get("STRAVA_CLIENT_SECRET")
STRAVA_REFRESH_TOKEN = os.environ.get("STRAVA_REFRESH_TOKEN")

SUPABASE_TABLE = "strava_activities"
SUPABASE: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

DAYS_BACK = int(os.environ.get("DAYS_BACK", 60))

# --------------------------------------------------
# Strava OAuth refresh
# --------------------------------------------------
def refresh_strava_token():
    print("DEBUG: Refreshing Strava access token...")
    response = requests.post(
        "https://www.strava.com/api/v3/oauth/token",
        data={
            "client_id": STRAVA_CLIENT_ID,
            "client_secret": STRAVA_CLIENT_SECRET,
            "grant_type": "refresh_token",
            "refresh_token": STRAVA_REFRESH_TOKEN,
        },
    )
    response.raise_for_status()
    token_data = response.json()
    print("DEBUG: Nieuw access token verkregen")
    return token_data["access_token"]

# --------------------------------------------------
# Haal gear name op indien gear_id aanwezig
# --------------------------------------------------
def fetch_gear_name(access_token, gear_id):
    if not gear_id:
        return None
    url = f"https://www.strava.com/api/v3/gears/{gear_id}"
    headers = {"Authorization": f"Bearer {access_token}"}
    try:
        r = requests.get(url, headers=headers)
        r.raise_for_status()
        gear_data = r.json()
        return gear_data.get("name")
    except Exception as e:
        print(f"⚠️ Failed to fetch gear name for {gear_id}: {e}")
        return None

# --------------------------------------------------
# Activiteiten ophalen
# --------------------------------------------------
def fetch_recent_activities(access_token, days=DAYS_BACK):
    print(f"DEBUG: Fetching activities from last {days} days")
    cutoff_ts = int((datetime.utcnow() - timedelta(days=days)).timestamp())
    url = "https://www.strava.com/api/v3/athlete/activities"
    headers = {"Authorization": f"Bearer {access_token}"}
    params = {"per_page": 200, "page": 1}
    all_acts = []
    while True:
        r = requests.get(url, headers=headers, params=params)
        r.raise_for_status()
        page_data = r.json()
        if not page_data:
            break
        # filter op cutoff_ts
        filtered = [
            a for a in page_data
            if int(datetime.strptime(a["start_date_local"], "%Y-%m-%dT%H:%M:%SZ").timestamp()) >= cutoff_ts
        ]
        all_acts.extend(filtered)
        if len(page_data) < 200:
            break
        params["page"] += 1
    print(f"DEBUG: Total {len(all_acts)} activities fetched")
    return all_acts

# --------------------------------------------------
# Upload naar Supabase
# --------------------------------------------------
def upload_to_supabase(access_token, activities):
    print("DEBUG: Uploading activities to Supabase...")

    payload = []
    for act in activities:
        gear_id = act.get("gear_id")
        gear_name = fetch_gear_name(access_token, gear_id)
        time.sleep(0.1)  # voorkom te veel requests tegelijk

        payload.append({
            "id": act.get("id"),
            "name": act.get("name"),
            "type": act.get("type"),
            "start_date": act.get("start_date_local"),
            "distance": act.get("distance"),
            "moving_time": act.get("moving_time"),
            "total_elevation_gain": act.get("total_elevation_gain"),
            "average_speed": act.get("average_speed"),
            "max_speed": act.get("max_speed"),
            "gear_id": gear_id,
            "gear_name": gear_name,
            "calories": act.get("calories"),
            "average_heartrate": act.get("average_heartrate"),
            "max_heartrate": act.get("max_heartrate"),
            "trainer": act.get("trainer"),
            "commute": act.get("commute"),
            "private": act.get("private"),
            "description": act.get("description"),
            "achievement_count": act.get("achievement_count"),
        })

    if not payload:
        print("⚠️ Geen activiteiten om te uploaden")
        return

    try:
        SUPABASE.table(SUPABASE_TABLE).upsert(payload, on_conflict="id").execute()
        print(f"✅ Uploaded {len(payload)} activities to Supabase")
    except Exception as e:
        print(f"❌ ERROR: Upload failed → {e}")

# --------------------------------------------------
# Main
# --------------------------------------------------
def main():
    token = refresh_strava_token()
    activities = fetch_recent_activities(token, DAYS_BACK)

    # Raw JSON opslaan voor debugging
    with open("activiteiten_raw.json", "w") as f:
        json.dump(activities, f, indent=2)

    upload_to_supabase(token, activities)
    print("DEBUG: Sync afgerond ✅")

if __name__ == "__main__":
    main()
