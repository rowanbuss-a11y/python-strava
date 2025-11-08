import os
import requests
import time
import json
import csv
from datetime import datetime, timedelta
from supabase import create_client, Client

# --------------------------------------------------
# CONFIG
# --------------------------------------------------
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
SUPABASE_TABLE = "strava_activities"

STRAVA_CLIENT_ID = os.environ["STRAVA_CLIENT_ID"]
STRAVA_CLIENT_SECRET = os.environ["STRAVA_CLIENT_SECRET"]
STRAVA_REFRESH_TOKEN = os.environ["STRAVA_REFRESH_TOKEN"]

CSV_FILE = os.environ.get("CSV_FILE", "activiteiten.csv")
JSON_FILE = os.environ.get("JSON_FILE", "activiteiten_raw.json")
DAYS_BACK = 60

SUPABASE: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

gear_cache = {}


# --------------------------------------------------
# TOKEN MANAGEMENT
# --------------------------------------------------
def refresh_access_token():
    """Vernieuwt Strava access token via refresh token."""
    r = requests.post(
        "https://www.strava.com/api/v3/oauth/token",
        data={
            "client_id": STRAVA_CLIENT_ID,
            "client_secret": STRAVA_CLIENT_SECRET,
            "grant_type": "refresh_token",
            "refresh_token": STRAVA_REFRESH_TOKEN,
        },
    )
    r.raise_for_status()
    token_data = r.json()
    print("✅ Nieuw access token opgehaald")
    return token_data["access_token"]


# --------------------------------------------------
# GEAR DETAILS
# --------------------------------------------------
def get_gear_name(access_token, gear_id):
    """Haalt de gear name op, met caching."""
    if not gear_id:
        return None
    if gear_id in gear_cache:
        return gear_cache[gear_id]

    url = f"https://www.strava.com/api/v3/gear/{gear_id}"
    headers = {"Authorization": f"Bearer {access_token}"}
    r = requests.get(url, headers=headers)

    if r.status_code == 404:
        print(f"⚠️ Gear niet gevonden: {gear_id}")
        gear_cache[gear_id] = None
        return None
    elif r.status_code == 429:
        print("⏳ Rate limit bereikt bij gear-oproep — 30 sec wachten...")
        time.sleep(30)
        return get_gear_name(access_token, gear_id)

    r.raise_for_status()
    gear_name = r.json().get("name")
    gear_cache[gear_id] = gear_name
    return gear_name


# --------------------------------------------------
# ACTIVITEITEN OPHALEN
# --------------------------------------------------
def get_activities(access_token, after_timestamp):
    """Haalt activiteiten op vanaf een bepaalde datum."""
    activities = []
    page = 1
    while True:
        url = f"https://www.strava.com/api/v3/athlete/activities?page={page}&per_page=200&after={after_timestamp}"
        headers = {"Authorization": f"Bearer {access_token}"}
        r = requests.get(url, headers=headers)

        if r.status_code == 429:
            print("⏳ Rate limit bereikt, 30 sec wachten...")
            time.sleep(30)
            continue

        r.raise_for_status()
        data = r.json()
        if not data:
            break
        activities.extend(data)
        page += 1
    print(f"📦 {len(activities)} activiteiten opgehaald")
    return activities


# --------------------------------------------------
# CONVERSIE NAAR STRUCTUUR
# --------------------------------------------------
def prepare_activity(activity, access_token):
    """Converteert Strava-activiteit naar opslagformaat."""
    gear_id = activity.get("gear_id")
    gear_name = get_gear_name(access_token, gear_id)

    return {
        "id": activity.get("id"),
        "name": activity.get("name"),
        "type": activity.get("type"),
        "start_date": activity.get("start_date"),
        "distance_km": round(activity.get("distance", 0) / 1000, 2),
        "moving_time_min": round(activity.get("moving_time", 0) / 60, 2),
        "elevation_gain": activity.get("total_elevation_gain"),
        "average_watts": activity.get("average_watts"),
        "calories": activity.get("calories"),
        "gear_id": gear_id,
        "gear_name": gear_name,
        "average_heartrate": activity.get("average_heartrate"),
        "max_heartrate": activity.get("max_heartrate"),
        "average_speed_kmh": round(activity.get("average_speed", 0) * 3.6, 2),
        "max_speed_kmh": round(activity.get("max_speed", 0) * 3.6, 2),
        "map_polyline": activity.get("map", {}).get("polyline"),
    }


# --------------------------------------------------
# SUPABASE UPLOAD
# --------------------------------------------------
def upload_to_supabase(rows):
    """Uploadt nieuwe activiteiten naar Supabase."""
    if not rows:
        print("⚠️ Geen nieuwe activiteiten om te uploaden.")
        return

    try:
        SUPABASE.table(SUPABASE_TABLE).upsert(rows, on_conflict="id").execute()
        print(f"✅ {len(rows)} activiteiten geüpload naar Supabase")
    except Exception as e:
        print(f"❌ Upload naar Supabase mislukt: {e}")


# --------------------------------------------------
# CSV OPSLAG (backup)
# --------------------------------------------------
def save_to_csv_and_json(rows):
    if not rows:
        return

    # JSON opslaan
    with open(JSON_FILE, "w", encoding="utf-8") as f:
        json.dump(rows, f, indent=2)

    # CSV opslaan
    keys = rows[0].keys()
    with open(CSV_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(rows)
    print(f"💾 Data opgeslagen in {CSV_FILE} en {JSON_FILE}")


# --------------------------------------------------
# MAIN
# --------------------------------------------------
def main():
    access_token = refresh_access_token()

    after_date = datetime.utcnow() - timedelta(days=DAYS_BACK)
    after_timestamp = int(after_date.timestamp())

    print(f"⏱ Ophalen activiteiten vanaf: {after_date.strftime('%Y-%m-%d')}")
    activities = get_activities(access_token, after_timestamp)

    # Data voorbereiden
    rows = [prepare_activity(a, access_token) for a in activities]

    # Upload naar Supabase
    upload_to_supabase(rows)

    # Backup lokaal opslaan
    save_to_csv_and_json(rows)

    print("✅ Sync volledig afgerond.")


if __name__ == "__main__":
    main()
