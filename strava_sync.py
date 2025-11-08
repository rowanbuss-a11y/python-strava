#!/usr/bin/env python3
# strava_sync.py
import os
import time
import requests
import csv
import json
from datetime import datetime, timedelta
import polyline

# ------------------------------
# Config via GitHub Secrets / Environment Variables
# ------------------------------
CLIENT_ID = os.getenv("STRAVA_CLIENT_ID")
CLIENT_SECRET = os.getenv("STRAVA_CLIENT_SECRET")
REFRESH_TOKEN = os.getenv("STRAVA_REFRESH_TOKEN")
CSV_FILE = os.getenv("CSV_FILE", "activiteiten.csv")
JSON_FILE = os.getenv("JSON_FILE", "activiteiten_raw.json")
DAYS_BACK = int(os.getenv("DAYS_BACK", "30"))

# ------------------------------
# Access token management
# ------------------------------
def refresh_access_token():
    global REFRESH_TOKEN
    url = "https://www.strava.com/oauth/token"
    payload = {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "grant_type": "refresh_token",
        "refresh_token": REFRESH_TOKEN
    }
    response = requests.post(url, data=payload)
    response.raise_for_status()
    tokens = response.json()
    REFRESH_TOKEN = tokens["refresh_token"]
    return tokens["access_token"]

# ------------------------------
# Load existing activity IDs
# ------------------------------
def load_existing_ids():
    if not os.path.exists(CSV_FILE):
        return set()
    existing_ids = set()
    with open(CSV_FILE, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            existing_ids.add(str(row.get("ID")))
    return existing_ids

# ------------------------------
# Prepare activity row for CSV
# ------------------------------
def prepare_activity_row(activity):
    gear = activity.get("gear") if isinstance(activity.get("gear"), dict) else {}
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
        "Gear ID": activity.get("gear_id"),
        "Gear naam": gear.get("name") if gear else None,
    }
    return row

# ------------------------------
# Save to CSV
# ------------------------------
def save_activities_to_csv(activities):
    if not activities:
        print("Geen nieuwe activiteiten om op te slaan.")
        return
    file_exists = os.path.isfile(CSV_FILE)
    fieldnames = list(prepare_activity_row(activities[0]).keys())
    with open(CSV_FILE, mode="a" if file_exists else "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        for activity in activities:
            writer.writerow(prepare_activity_row(activity))
    print(f"{len(activities)} nieuwe activiteiten opgeslagen in CSV.")

# ------------------------------
# Save to JSON
# ------------------------------
def save_activities_to_json(activities):
    existing_data = []
    if os.path.exists(JSON_FILE):
        with open(JSON_FILE, "r", encoding="utf-8") as f:
            existing_data = json.load(f)
    existing_data.extend(activities)
    with open(JSON_FILE, "w", encoding="utf-8") as f:
        json.dump(existing_data, f, indent=2, ensure_ascii=False)
    print(f"{len(activities)} nieuwe activiteiten opgeslagen in JSON.")

# ------------------------------
# Get activities from Strava
# ------------------------------
def get_activities(access_token, after_timestamp):
    activities = []
    page = 1
    per_page = 200
    while True:
        url = "https://www.strava.com/api/v3/athlete/activities"
        headers = {"Authorization": f"Bearer {access_token}"}
        params = {"page": page, "per_page": per_page, "after": after_timestamp}
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 401:
            # Token verlopen
            access_token = refresh_access_token()
            headers = {"Authorization": f"Bearer {access_token}"}
            response = requests.get(url, headers=headers, params=params)
        elif response.status_code == 429:
            print("Rate limit bereikt, wachten 30 sec...")
            time.sleep(30)
            continue
        response.raise_for_status()
        page_activities = response.json()
        if not page_activities:
            break
        activities.extend(page_activities)
        page += 1
    return activities

# ------------------------------
# Main
# ------------------------------
def main():
    access_token = refresh_access_token()
    existing_ids = load_existing_ids()
    after_date = datetime.utcnow() - timedelta(days=DAYS_BACK)
    after_timestamp = int(after_date.timestamp())
    print(f"Ophalen activiteiten na {after_date.isoformat()}...")

    new_activities = []
    all_activities = get_activities(access_token, after_timestamp)
    for act in all_activities:
        if str(act["id"]) not in existing_ids:
            new_activities.append(act)

    print(f"Totaal {len(new_activities)} nieuwe activiteiten gevonden.")
    save_activities_to_csv(new_activities)
    save_activities_to_json(new_activities)

if __name__ == "__main__":
    main()
