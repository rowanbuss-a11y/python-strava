import os
import requests
import time
import csv
import json
from datetime import datetime

# ======================
# Config vanuit GitHub secrets
# ======================
CLIENT_ID = int(os.getenv("STRAVA_CLIENT_ID"))
CLIENT_SECRET = os.getenv("STRAVA_CLIENT_SECRET")
REFRESH_TOKEN = os.getenv("STRAVA_REFRESH_TOKEN")
CSV_FILE = os.getenv("CSV_FILE", "activiteiten.csv")
DETAILS_FILE = os.getenv("DETAILS_FILE", "existing_details.json")
PAUSE = 2  # pauze tussen requests om rate limits te vermijden

# ======================
# Token management
# ======================
def get_access_token():
    url = "https://www.strava.com/oauth/token"
    payload = {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "grant_type": "refresh_token",
        "refresh_token": REFRESH_TOKEN
    }
    response = requests.post(url, data=payload)
    response.raise_for_status()
    token_data = response.json()
    # update REFRESH_TOKEN als deze verandert
    global REFRESH_TOKEN
    REFRESH_TOKEN = token_data["refresh_token"]
    return token_data["access_token"]

# ======================
# Load/save existing details
# ======================
def load_existing_details():
    if os.path.exists(DETAILS_FILE):
        with open(DETAILS_FILE, "r") as f:
            return json.load(f)
    return {}

def save_existing_details(details):
    with open(DETAILS_FILE, "w") as f:
        json.dump(details, f)

# ======================
# CSV helpers
# ======================
def load_existing_ids():
    if not os.path.exists(CSV_FILE):
        return set()
    with open(CSV_FILE, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return set(row["ID"] for row in reader if "ID" in row)

def save_activities_to_csv(activities):
    if not activities:
        return
    fieldnames = list(activities[0].keys())
    existing_ids = load_existing_ids()
    new_rows = [a for a in activities if str(a["ID"]) not in existing_ids]
    mode = "a" if os.path.exists(CSV_FILE) else "w"
    with open(CSV_FILE, mode, newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if mode == "w":
            writer.writeheader()
        writer.writerows(new_rows)
    print(f"{len(new_rows)} activiteiten opgeslagen in {CSV_FILE}")

# ======================
# API helpers
# ======================
def get_activities(access_token, after=None):
    all_activities = []
    page = 1
    per_page = 200
    headers = {"Authorization": f"Bearer {access_token}"}
    while True:
        params = {"page": page, "per_page": per_page}
        if after:
            params["after"] = int(after.timestamp())
        response = requests.get("https://www.strava.com/api/v3/athlete/activities",
                                headers=headers, params=params)
        if response.status_code == 429:
            print("Rate limit bereikt, wachten 120 seconden...")
            time.sleep(120)
            continue
        response.raise_for_status()
        data = response.json()
        if not data:
            break
        all_activities.extend(data)
        page += 1
    print(f"Totaal {len(all_activities)} nieuwe activiteiten opgehaald")
    return all_activities

def get_activity_details(activity_id, access_token, existing_details):
    if str(activity_id) in existing_details:
        return existing_details[str(activity_id)]
    url = f"https://www.strava.com/api/v3/activities/{activity_id}"
    headers = {"Authorization": f"Bearer {access_token}"}
    for _ in range(3):
        response = requests.get(url, headers=headers)
        if response.status_code == 429:
            print("Rate limit, even wachten...")
            time.sleep(120)
            continue
        response.raise_for_status()
        data = response.json()
        existing_details[str(activity_id)] = data
        save_existing_details(existing_details)
        time.sleep(PAUSE)
        return data
    return None

def prepare_activity_row(activity):
    gear = activity.get("gear", {}) if isinstance(activity.get("gear"), dict) else {}
    row = {
        "ID": activity.get("id"),
        "Naam": activity.get("name"),
        "Datum": activity.get("start_date"),
        "Type": activity.get("type"),
        "Afstand_km": round(activity.get("distance", 0)/1000, 2),
        "Tijd_min": round(activity.get("moving_time", 0)/60, 2),
        "Hoogtemeters": activity.get("total_elevation_gain"),
        "Gear_ID": activity.get("gear_id"),
        "Gear_naam": gear.get("name"),
        "Calorieën": activity.get("calories"),
        "Gemiddeld_vermogen_W": activity.get("average_watts"),
        "Max_snelheid_km_u": round(activity.get("max_speed",0)*3.6,2)
    }
    return row

# ======================
# Main sync
# ======================
def main():
    access_token = get_access_token()
    existing_details = load_existing_details()
    
    # Optioneel: alleen activiteiten na laatste in CSV
    last_date = None
    if os.path.exists(CSV_FILE):
        with open(CSV_FILE, "r") as f:
            reader = csv.DictReader(f)
            dates = [datetime.strptime(r["Datum"], "%Y-%m-%dT%H:%M:%SZ") for r in reader if r.get("Datum")]
            if dates:
                last_date = max(dates)
    
    summary_activities = get_activities(access_token, after=last_date)
    
    all_activities = []
    for act in summary_activities:
        details = get_activity_details(act["id"], access_token, existing_details)
        merged = {**act, **details} if details else act
        all_activities.append(prepare_activity_row(merged))
    
    save_activities_to_csv(all_activities)
    print("Sync klaar ✅")

if __name__ == "__main__":
    main()
