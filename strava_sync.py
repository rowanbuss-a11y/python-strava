import os
import requests
import time
import json
import csv
from datetime import datetime, timedelta

# Environment variables
CLIENT_ID = os.environ["STRAVA_CLIENT_ID"]
CLIENT_SECRET = os.environ["STRAVA_CLIENT_SECRET"]
REFRESH_TOKEN = os.environ["STRAVA_REFRESH_TOKEN"]
ACCESS_TOKEN_FILE = "access_token.json"
CSV_FILE = os.environ.get("CSV_FILE", "activiteiten.csv")
JSON_FILE = os.environ.get("JSON_FILE", "activiteiten_raw.json")
DAYS_BACK = 60  # Historie van 60 dagen

# Cache voor gear requests
gear_cache = {}

def save_access_token(token_data):
    with open(ACCESS_TOKEN_FILE, "w") as f:
        json.dump(token_data, f)

def load_access_token():
    if os.path.exists(ACCESS_TOKEN_FILE):
        with open(ACCESS_TOKEN_FILE) as f:
            return json.load(f)
    return None

def refresh_access_token():
    data = {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "grant_type": "refresh_token",
        "refresh_token": REFRESH_TOKEN
    }
    r = requests.post("https://www.strava.com/oauth/token", data=data)
    r.raise_for_status()
    token_data = r.json()
    save_access_token(token_data)
    return token_data["access_token"]

def get_valid_access_token():
    token_data = load_access_token()
    if token_data:
        expires_at = token_data.get("expires_at", 0)
        if expires_at > time.time():
            return token_data["access_token"]
    return refresh_access_token()

def get_gear_name(access_token, gear_id):
    """Haalt gear name op via Strava API en cache het."""
    if not gear_id:
        return None
    if gear_id in gear_cache:
        return gear_cache[gear_id]
    
    url = f"https://www.strava.com/api/v3/gear/{gear_id}"
    headers = {"Authorization": f"Bearer {access_token}"}
    
    for _ in range(3):  # retry bij tijdelijke fouten
        r = requests.get(url, headers=headers)
        if r.status_code == 200:
            data = r.json()
            gear_name = data.get("name")
            gear_cache[gear_id] = gear_name
            return gear_name
        elif r.status_code == 429:  # rate limit
            print("Rate limit gear bereikt, wacht 30 sec...")
            time.sleep(30)
        else:
            print(f"Fout bij ophalen gear {gear_id}: {r.status_code}")
            return None
    return None

def prepare_activity_row(activity, access_token):
    gear_id = activity.get("gear_id")
    gear_name = activity.get("gear", {}).get("name") or get_gear_name(access_token, gear_id)

    row = {
        "ID": activity.get("id"),
        "Naam": activity.get("name"),
        "Datum": activity.get("start_date"),
        "Type": activity.get("type"),
        "Afstand (km)": round(activity.get("distance", 0) / 1000, 2),
        "Tijd (min)": round(activity.get("moving_time", 0) / 60, 2),
        "Totale tijd (min)": round(activity.get("elapsed_time", 0) / 60, 2),
        "Hoogtemeters": activity.get("total_elevation_gain", 0),
        "Gemiddeld vermogen (W)": activity.get("average_watts"),
        "Calorieën": activity.get("calories"),
        "Gear ID": gear_id,
        "Gear naam": gear_name,
        "Heart Rate Gemiddeld": activity.get("average_heartrate"),
        "Heart Rate Max": activity.get("max_heartrate"),
        "Snelheid Gemiddeld (km/u)": round((activity.get("average_speed", 0) * 3.6), 2),
        "Snelheid Max (km/u)": round((activity.get("max_speed", 0) * 3.6), 2),
        "Polyline": activity.get("map", {}).get("polyline")
    }
    return row

def get_activities(access_token, after_timestamp):
    activities = []
    page = 1
    while True:
        url = f"https://www.strava.com/api/v3/athlete/activities?page={page}&per_page=200&after={after_timestamp}"
        headers = {"Authorization": f"Bearer {access_token}"}
        r = requests.get(url, headers=headers)
        if r.status_code == 429:
            print("Rate limit activiteiten bereikt, wachten 30 sec...")
            time.sleep(30)
            continue
        r.raise_for_status()
        batch = r.json()
        if not batch:
            break
        activities.extend(batch)
        page += 1
    return activities

def load_existing_ids():
    existing_ids = set()
    if os.path.exists(CSV_FILE):
        with open(CSV_FILE, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                existing_ids.add(int(row["ID"]))
    return existing_ids

def main():
    access_token = get_valid_access_token()
    after_date = datetime.now() - timedelta(days=DAYS_BACK)
    after_timestamp = int(after_date.timestamp())

    existing_ids = load_existing_ids()
    print(f"Bestaande activiteiten: {len(existing_ids)}")

    activities = get_activities(access_token, after_timestamp)
    print(f"Opgehaalde activiteiten: {len(activities)}")

    new_rows = []
    for act in activities:
        if act["id"] in existing_ids:
            continue
        row = prepare_activity_row(act, access_token)
        new_rows.append(row)

    if not new_rows:
        print("Geen nieuwe activiteiten gevonden.")
        return

    # JSON opslaan
    all_json = []
    if os.path.exists(JSON_FILE):
        with open(JSON_FILE) as f:
            all_json = json.load(f)
    all_json.extend(new_rows)
    with open(JSON_FILE, "w") as f:
        json.dump(all_json, f, indent=2)

    # CSV opslaan
    if os.path.exists(CSV_FILE):
        with open(CSV_FILE, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            all_rows = list(reader)
    else:
        all_rows = []

    all_rows.extend(new_rows)
    keys = all_rows[0].keys()
    with open(CSV_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(all_rows)

    print(f"Succesvol {len(new_rows)} nieuwe activiteiten toegevoegd aan CSV en JSON.")

if __name__ == "__main__":
    main()
