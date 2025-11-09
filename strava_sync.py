import os
import time
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
# Strava OAuth Token vernieuwen
# --------------------------------------------------
def refresh_strava_token():
    print("🔑 Nieuw Strava access token ophalen...")
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
    print("✅ Nieuw access token verkregen")
    return token_data["access_token"]

# --------------------------------------------------
# Activiteiten ophalen (samenvatting)
# --------------------------------------------------
def fetch_activities_summary(access_token, days=60):
    cutoff_date = datetime.utcnow() - timedelta(days=days)
    cutoff_ts = int(cutoff_date.timestamp())
    print(f"⏱ Ophalen activiteiten vanaf: {cutoff_date.date()}")

    url = "https://www.strava.com/api/v3/athlete/activities"
    headers = {"Authorization": f"Bearer {access_token}"}
    params = {"per_page": 200, "page": 1, "after": cutoff_ts}

    all_activities = []
    while True:
        r = requests.get(url, headers=headers, params=params)
        if r.status_code == 429:
            print("⚠️ Rate limit bereikt, 15 sec wachten...")
            time.sleep(15)
            continue
        r.raise_for_status()
        data = r.json()
        if not data:
            break
        all_activities.extend(data)
        if len(data) < 200:
            break
        params["page"] += 1

    print(f"📦 {len(all_activities)} activiteiten opgehaald")
    return all_activities

# --------------------------------------------------
# Extra details ophalen (calories + gear name)
# --------------------------------------------------
def fetch_activity_details(access_token, activity_id):
    url = f"https://www.strava.com/api/v3/activities/{activity_id}"
    headers = {"Authorization": f"Bearer {access_token}"}
    r = requests.get(url, headers=headers)
    if r.status_code == 429:
        print("⚠️ Rate limit bereikt bij details, 15 sec wachten...")
        time.sleep(15)
        return fetch_activity_details(access_token, activity_id)
    if r.status_code != 200:
        print(f"❌ Fout bij ophalen details {activity_id}: {r.text}")
        return {}
    return r.json()

# --------------------------------------------------
# Uploaden naar Supabase
# --------------------------------------------------
def upload_to_supabase(activities):
    print("⬆️ Uploaden naar Supabase...")
    try:
        SUPABASE.table(SUPABASE_TABLE).upsert(activities, on_conflict="id").execute()
        print(f"✅ {len(activities)} records geüpload naar Supabase")
    except Exception as e:
        print(f"❌ Upload naar Supabase mislukt: {e}")

# --------------------------------------------------
# Main flow
# --------------------------------------------------
def main():
    access_token = refresh_strava_token()
    activities = fetch_activities_summary(access_token, days=60)

    detailed_activities = []
    for i, act in enumerate(activities, start=1):
        act_id = act.get("id")
        print(f"🔍 Details ophalen voor activiteit {i}/{len(activities)} ({act_id})...")
        details = fetch_activity_details(access_token, act_id)

        gear_name = None
        if details.get("gear_id"):
            gear_url = f"https://www.strava.com/api/v3/gears/{details['gear_id']}"
            gear_resp = requests.get(gear_url, headers={"Authorization": f"Bearer {access_token}"})
            if gear_resp.status_code == 200:
                gear_name = gear_resp.json().get("name")

        detailed_activities.append({
            "id": act_id,
            "name": act.get("name"),
            "type": act.get("type"),
            "distance": act.get("distance"),
            "moving_time": act.get("moving_time"),
            "elapsed_time": act.get("elapsed_time"),
            "start_date": act.get("start_date_local"),
            "average_speed": act.get("average_speed"),
            "average_speed_kmh": round(act.get("average_speed", 0) * 3.6, 2),
            "calories": details.get("calories"),
            "gear_id": details.get("gear_id"),
            "gear_name": gear_name,
        })

        time.sleep(1)  # API limieten respecteren

    upload_to_supabase(detailed_activities)

    with open("activiteiten_raw.json", "w") as f:
        json.dump(detailed_activities, f, indent=2)
    print("💾 Data opgeslagen in activiteiten_raw.json")
    print("✅ Sync volledig afgerond.")

if __name__ == "__main__":
    main()
