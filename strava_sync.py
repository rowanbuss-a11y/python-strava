import os
import requests
import csv
import json
import time
from datetime import datetime, timedelta
from supabase import create_client, Client

# === Configuratie ===
STRAVA_CLIENT_ID = os.getenv("STRAVA_CLIENT_ID")
STRAVA_CLIENT_SECRET = os.getenv("STRAVA_CLIENT_SECRET")
STRAVA_REFRESH_TOKEN = os.getenv("STRAVA_REFRESH_TOKEN")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
CSV_FILE = os.getenv("CSV_FILE", "activiteiten.csv")
JSON_FILE = os.getenv("JSON_FILE", "activiteiten_raw.json")
DAYS_BACK = int(os.getenv("DAYS_BACK", "60"))

SUPABASE: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# === Access token ophalen ===
def get_access_token():
    url = "https://www.strava.com/api/v3/oauth/token"
    data = {
        "client_id": STRAVA_CLIENT_ID,
        "client_secret": STRAVA_CLIENT_SECRET,
        "grant_type": "refresh_token",
        "refresh_token": STRAVA_REFRESH_TOKEN
    }
    r = requests.post(url, data=data)
    r.raise_for_status()
    token = r.json()
    print("✅ Nieuw access token opgehaald")
    return token["access_token"]

# === Supabase-schema controleren en uitbreiden ===
def ensure_supabase_schema():
    print("🔍 Controleren Supabase-schema...")
    required_columns = {
        "id": "bigint",
        "name": "text",
        "type": "text",
        "start_date": "timestamp",
        "distance_km": "float8",
        "moving_time_min": "float8",
        "elevation_gain": "float8",
        "average_watts": "float8",
        "calories": "float8",
        "gear_id": "text",
        "gear_name": "text",
        "average_heartrate": "float8",
        "max_heartrate": "float8",
        "average_speed_kmh": "float8",
        "max_speed_kmh": "float8",
        "map_polyline": "text"
    }

    try:
        r = requests.get(f"{SUPABASE_URL}/rest/v1/strava_activities?limit=1",
                         headers={"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"})
        if r.status_code not in [200, 404]:
            print("⚠️ Kon Supabase-tabel niet lezen:", r.text)
            return

        # Proberen te creëren van ontbrekende kolommen
        for col, col_type in required_columns.items():
            alter = f"ALTER TABLE strava_activities ADD COLUMN IF NOT EXISTS {col} {col_type};"
            requests.post(
                f"{SUPABASE_URL}/rest/v1/rpc/exec_sql",
                headers={
                    "apikey": SUPABASE_KEY,
                    "Authorization": f"Bearer {SUPABASE_KEY}",
                    "Content-Type": "application/json"
                },
                json={"query": alter}
            )
        print("✅ Supabase-schema gecontroleerd en up-to-date")
    except Exception as e:
        print("⚠️ Schema-check overgeslagen:", e)

# === Activiteiten ophalen ===
def get_activities(access_token, after_timestamp):
    activities = []
    page = 1
    per_page = 200
    while True:
        url = f"https://www.strava.com/api/v3/athlete/activities?page={page}&per_page={per_page}&after={after_timestamp}"
        r = requests.get(url, headers={"Authorization": f"Bearer {access_token}"})
        if r.status_code == 429:
            print("⚠️ Rate limit bereikt, wachten 30 sec...")
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

# === Details ophalen ===
def get_activity_details(access_token, activity_id):
    url = f"https://www.strava.com/api/v3/activities/{activity_id}"
    r = requests.get(url, headers={"Authorization": f"Bearer {access_token}"})
    if r.status_code == 429:
        print(f"⚠️ Rate limit bij details {activity_id}, wachten 30 sec...")
        time.sleep(30)
        return get_activity_details(access_token, activity_id)
    if r.status_code != 200:
        print(f"❌ Fout bij ophalen details {activity_id}: {r.text}")
        return {}
    return r.json()

# === Gearnaam ophalen ===
def get_gear_name(access_token, gear_id, cache):
    if not gear_id:
        return None
    if gear_id in cache:
        return cache[gear_id]
    url = f"https://www.strava.com/api/v3/gear/{gear_id}"
    r = requests.get(url, headers={"Authorization": f"Bearer {access_token}"})
    if r.status_code == 200:
        data = r.json()
        gear_name = data.get("name")
        cache[gear_id] = gear_name
        return gear_name
    else:
        print(f"⚠️ Gear lookup mislukt voor {gear_id}: {r.text}")
        return None

# === Supabase upload ===
def upload_to_supabase(data):
    try:
        response = SUPABASE.table("strava_activities").upsert(data).execute()
        if hasattr(response, "error") and response.error:
            print("❌ Upload naar Supabase mislukt:", response.error)
        else:
            print(f"✅ {len(data)} records geüpload naar Supabase")
    except Exception as e:
        print("❌ Fout bij upload:", e)

# === Lokale opslag ===
def save_locally(csv_data, json_data):
    keys = csv_data[0].keys() if csv_data else []
    with open(CSV_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(csv_data)
    with open(JSON_FILE, "w", encoding="utf-8") as f:
        json.dump(json_data, f, indent=2, ensure_ascii=False)
    print(f"💾 Data opgeslagen in {CSV_FILE} en {JSON_FILE}")

# === Main ===
def main():
    ensure_supabase_schema()
    access_token = get_access_token()

    after_date = datetime.now() - timedelta(days=DAYS_BACK)
    after_timestamp = int(after_date.timestamp())
    print(f"⏱ Ophalen activiteiten vanaf: {after_date.strftime('%Y-%m-%d')}")

    activities = get_activities(access_token, after_timestamp)
    gear_cache = {}
    results = []

    for act in activities:
        details = get_activity_details(access_token, act["id"])
        if not details:
            continue

        gear_id = details.get("gear_id")
        gear_name = get_gear_name(access_token, gear_id, gear_cache)
        calories = details.get("calories", 0)

        results.append({
            "id": act.get("id"),
            "name": act.get("name"),
            "type": act.get("type"),
            "start_date": act.get("start_date"),
            "distance_km": round(act.get("distance", 0) / 1000, 2),
            "moving_time_min": round(act.get("moving_time", 0) / 60, 2),
            "elevation_gain": act.get("total_elevation_gain", 0),
            "average_watts": details.get("average_watts"),
            "calories": calories,
            "gear_id": gear_id,
            "gear_name": gear_name,
            "average_heartrate": details.get("average_heartrate"),
            "max_heartrate": details.get("max_heartrate"),
            "average_speed_kmh": round(details.get("average_speed", 0) * 3.6, 2),
            "max_speed_kmh": round(details.get("max_speed", 0) * 3.6, 2),
            "map_polyline": details.get("map", {}).get("summary_polyline")
        })

    if results:
        upload_to_supabase(results)
        save_locally(results, activities)
        print("✅ Sync volledig afgerond.")
    else:
        print("ℹ️ Geen nieuwe activiteiten gevonden.")

if __name__ == "__main__":
    main()
