import os
import requests
import json
from datetime import datetime, timedelta
from supabase import create_client, Client

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
# Activiteiten ophalen (laatste 'days' dagen)
# --------------------------------------------------
def fetch_recent_activities(access_token, days=60):
    print(f"DEBUG: Fetching activities from last {days} days")
    url = "https://www.strava.com/api/v3/athlete/activities"
    headers = {"Authorization": f"Bearer {access_token}"}
    params = {"per_page": 200, "page": 1}
    all_acts = []
    cutoff_ts = int((datetime.utcnow() - timedelta(days=days)).timestamp())
    
    while True:
        r = requests.get(url, headers=headers, params=params)
        r.raise_for_status()
        page_data = r.json()
        if not page_data:
            break

        # Filter alleen activiteiten binnen de laatste 'days' dagen
        filtered = [
            a for a in page_data
            if int(datetime.fromisoformat(a["start_date_local"].replace("Z", "+00:00")).timestamp()) >= cutoff_ts
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
def upload_to_supabase(activities):
    print("DEBUG: Uploading activities to Supabase...")

    payload = []
    skipped = 0

    for act in activities:
        calories = act.get("calories")
        if calories is None:
            skipped += 1

        payload.append({
            "id": act.get("id"),
            "name": act.get("name"),
            "type": act.get("type"),
            "start_date": act.get("start_date_local"),
            "calories": calories,
            "gear_id": act.get("gear_id"),
            "gear_name": act.get("gear_name"),
            "distance": act.get("distance"),
            "moving_time": act.get("moving_time"),
            "elapsed_time": act.get("elapsed_time"),
            "total_elevation_gain": act.get("total_elevation_gain"),
            "average_speed": act.get("average_speed"),
            "max_speed": act.get("max_speed"),
            "average_heartrate": act.get("average_heartrate"),
            "max_heartrate": act.get("max_heartrate"),
            "has_heartrate": act.get("has_heartrate"),
            "trainer": act.get("trainer"),
            "commute": act.get("commute"),
            "private": act.get("private"),
            "description": act.get("description"),
        })

    print(f"DEBUG: {skipped} activiteiten zonder calorieën")

    if not payload:
        print("⚠️ Geen activiteiten om te uploaden")
        return

    try:
        SUPABASE.table(SUPABASE_TABLE).upsert(payload, on_conflict="id").execute()
        print(f"✅ Uploaded {len(payload)} records to Supabase")
    except Exception as e:
        print(f"❌ ERROR: Upload to Supabase failed → {e}")

# --------------------------------------------------
# Main
# --------------------------------------------------
def main():
    token = refresh_strava_token()
    activities = fetch_recent_activities(token, days=60)

    # Opslaan van raw JSON voor debugging
    with open("activiteiten_raw.json", "w") as f:
        json.dump(activities, f, indent=2)

    upload_to_supabase(activities)
    print("DEBUG: Sync afgerond ✅")

if __name__ == "__main__":
    main()
