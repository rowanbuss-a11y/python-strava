import os
import requests
import json
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

# Cache voor gear names
GEAR_CACHE = {}

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
# Gear naam ophalen met caching
# --------------------------------------------------
def fetch_gear_name(access_token, gear_id):
    if not gear_id:
        return None
    if gear_id in GEAR_CACHE:
        return GEAR_CACHE[gear_id]
    url = f"https://www.strava.com/api/v3/gear/{gear_id}"
    headers = {"Authorization": f"Bearer {access_token}"}
    r = requests.get(url, headers=headers)
    if r.status_code == 200:
        name = r.json().get("name")
        GEAR_CACHE[gear_id] = name
        return name
    return None

# --------------------------------------------------
# Activiteiten ophalen
# --------------------------------------------------
def fetch_recent_activities(access_token, days=60):
    print(f"DEBUG: Fetching activities from last {days} days")
    url = "https://www.strava.com/api/v3/athlete/activities"
    headers = {"Authorization": f"Bearer {access_token}"}
    params = {"per_page": 200, "page": 1}
    all_acts = []
    cutoff_ts = int((requests.utils.datetime.datetime.utcnow() - requests.utils.datetime.timedelta(days=days)).timestamp())
    
    while True:
        r = requests.get(url, headers=headers, params=params)
        r.raise_for_status()
        page_data = r.json()
        if not page_data:
            break
        # filter op cutoff date
        filtered = [a for a in page_data if int(requests.utils.datetime.datetime.strptime(a["start_date_local"], "%Y-%m-%dT%H:%M:%S").timestamp()) >= cutoff_ts]
        all_acts.extend(filtered)
        if len(page_data) < 200:
            break
        params["page"] += 1
    print(f"DEBUG: Total {len(all_acts)} activities fetched")
    return all_acts

# --------------------------------------------------
# Upload naar Supabase
# --------------------------------------------------
def upload_to_supabase(activities, access_token):
    print("DEBUG: Preparing activities for Supabase upload...")
    payload = []

    for act in activities:
        payload.append({
            "id": act.get("id"),
            "name": act.get("name"),
            "type": act.get("type"),
            "start_date": act.get("start_date_local"),
            "distance": act.get("distance"),
            "moving_time": act.get("moving_time"),
            "total_elevation_gain": act.get("total_elevation_gain"),
            "average_speed": act.get("average_speed"),
            "start_latitude": act.get("start_latitude"),
            "start_longitude": act.get("start_longitude"),
            "end_latitude": act.get("end_latitude"),
            "end_longitude": act.get("end_longitude"),
            "average_heartrate": act.get("average_heartrate"),
            "max_heartrate": act.get("max_heartrate"),
            "calories": act.get("calories"),
            "gear_id": act.get("gear_id"),
            "gear_name": fetch_gear_name(access_token, act.get("gear_id")),
            "trainer": act.get("trainer"),
            "commute": act.get("commute"),
            "private": act.get("private"),
            "description": act.get("description"),
        })

    if not payload:
        print("⚠️ Geen activiteiten om te uploaden")
        return

    try:
        SUPABASE.table(SUPABASE_TABLE).upsert(payload, on_conflict="id").execute()
        print(f"✅ Uploaded {len(payload)} activities to Supabase")
    except Exception as e:
        print(f"❌ ERROR: Upload to Supabase failed → {e}")

# --------------------------------------------------
# Main
# --------------------------------------------------
def main():
    token = refresh_strava_token()
    activities = fetch_recent_activities(token, days=60)
    
    # Raw JSON voor debugging
    with open("activiteiten_raw.json", "w") as f:
        json.dump(activities, f, indent=2)
    
    upload_to_supabase(activities, token)
    print("DEBUG: Sync afgerond ✅")

if __name__ == "__main__":
    main()
