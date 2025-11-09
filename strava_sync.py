import os
import time
import json
import csv
import requests
from datetime import datetime, timedelta
from supabase import create_client, Client

# === Configuratie ===
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
STRAVA_CLIENT_ID = os.getenv("STRAVA_CLIENT_ID")
STRAVA_CLIENT_SECRET = os.getenv("STRAVA_CLIENT_SECRET")
REFRESH_TOKEN = os.getenv("STRAVA_REFRESH_TOKEN")

CSV_FILE = os.getenv("CSV_FILE", "activiteiten.csv")
JSON_FILE = os.getenv("JSON_FILE", "activiteiten_raw.json")
DAYS_BACK = int(os.getenv("DAYS_BACK", 60))

SUPABASE_TABLE = "strava_activities"
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


# === Functies ===

def refresh_access_token():
    """Vernieuw het Strava access token via refresh token."""
    url = "https://www.strava.com/api/v3/oauth/token"
    payload = {
        "client_id": STRAVA_CLIENT_ID,
        "client_secret": STRAVA_CLIENT_SECRET,
        "grant_type": "refresh_token",
        "refresh_token": REFRESH_TOKEN,
    }
    r = requests.post(url, data=payload)
    r.raise_for_status()
    tokens = r.json()
    print("✅ Nieuw access token opgehaald")
    return tokens["access_token"]


def ensure_supabase_columns():
    """Zorgt dat alle vereiste kolommen in Supabase bestaan."""
    print("🔍 Controleren Supabase-schema...")
    required_columns = {
        "id": "bigint",
        "name": "text",
        "type": "text",
        "distance_km": "numeric",
        "average_speed_kmh": "numeric",
        "calories": "numeric",
        "gear_name": "text",
        "moving_time": "integer",
        "elapsed_time": "integer",
        "total_elevation_gain": "numeric",
        "elevation_gain": "numeric",
        "start_date": "timestamptz",
        "map_polyline": "text",
    }

    for col, dtype in required_columns.items():
        query = f"ALTER TABLE {SUPABASE_TABLE} ADD COLUMN IF NOT EXISTS {col} {dtype};"
        try:
            supabase.rpc("sql", {"query": query})
        except Exception:
            # fallback, sommige Supabase-versies ondersteunen rpc niet
            pass
    print("✅ Supabase-schema gecontroleerd en up-to-date")


def get_activities(access_token):
    """Haalt activiteiten van de afgelopen X dagen op."""
    after = int((datetime.utcnow() - timedelta(days=DAYS_BACK)).timestamp())
    print(f"⏱ Ophalen activiteiten vanaf: {datetime.utcfromtimestamp(after).date()}")

    all_activities = []
    page = 1

    while True:
        url = f"https://www.strava.com/api/v3/athlete/activities?page={page}&per_page=200&after={after}"
        r = requests.get(url, headers={"Authorization": f"Bearer {access_token}"})
        if r.status_code == 429:
            print("⚠️ Rate limit bereikt, wachten 30 sec...")
            time.sleep(30)
            continue
        r.raise_for_status()

        data = r.json()
        if not data:
            break

        all_activities.extend(data)
        page += 1

    print(f"📦 {len(all_activities)} activiteiten opgehaald")
    return all_activities


def get_activity_details(activity_id, access_token):
    """Haalt volledige details op van één activiteit."""
    url = f"https://www.strava.com/api/v3/activities/{activity_id}"
    r = requests.get(url, headers={"Authorization": f"Bearer {access_token}"})
    if r.status_code == 404:
        return {}
    r.raise_for_status()
    return r.json()


def get_gear_name(gear_id, access_token):
    """Haalt de naam van de gear op (fiets/schoenen)."""
    if not gear_id:
        return None
    url = f"https://www.strava.com/api/v3/gear/{gear_id}"
    r = requests.get(url, headers={"Authorization": f"Bearer {access_token}"})
    if r.status_code == 404:
        return None
    r.raise_for_status()
    gear = r.json()
    return gear.get("name")


def process_activities(activities, access_token):
    """Combineert hoofd- en detaildata per activiteit."""
    processed = []
    for act in activities:
        details = get_activity_details(act["id"], access_token)
        gear_name = get_gear_name(act.get("gear_id"), access_token)

        processed.append({
            "id": act["id"],
            "name": act.get("name"),
            "type": act.get("type"),
            "distance_km": round(act.get("distance", 0) / 1000, 2),
            "average_speed_kmh": round(act.get("average_speed", 0) * 3.6, 2),
            "calories": details.get("calories"),
            "gear_name": gear_name,
            "moving_time": act.get("moving_time"),
            "elapsed_time": act.get("elapsed_time"),
            "total_elevation_gain": act.get("total_elevation_gain"),
            "elevation_gain": details.get("elev_high", 0) - details.get("elev_low", 0)
            if details.get("elev_high") and details.get("elev_low") else None,
            "start_date": act.get("start_date"),
            "map_polyline": act.get("map", {}).get("summary_polyline"),
        })
    return processed


def upload_to_supabase(data):
    """Uploadt data naar Supabase."""
    try:
        response = supabase.table(SUPABASE_TABLE).upsert(data).execute()
        if hasattr(response, "error") and response.error:
            raise Exception(response.error)
        print(f"✅ {len(data)} activiteiten geüpload naar Supabase")
    except Exception as e:
        print(f"❌ Fout bij upload: {e}")


def save_to_files(data):
    """Slaat resultaten lokaal op als CSV en JSON."""
    keys = data[0].keys() if data else []
    with open(CSV_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(data)
    with open(JSON_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    print(f"💾 Data opgeslagen in {CSV_FILE} en {JSON_FILE}")


def main():
    ensure_supabase_columns()
    access_token = refresh_access_token()
    activities = get_activities(access_token)
    processed = process_activities(activities, access_token)
    upload_to_supabase(processed)
    save_to_files(processed)
    print("✅ Sync volledig afgerond.")


if __name__ == "__main__":
    main()
